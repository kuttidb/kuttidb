package main

import (
	"fmt"
	"log"
	"os"
	"sync"
	"time"

	"kuttidb"
)

func main() {
	var c *kuttidb.Client
	var err error
	if token := os.Getenv("KUTTIDB_AUTH_TOKEN"); token != "" {
		c, err = kuttidb.NewAuthenticated("127.0.0.1:7394", 4, []byte(token))
	} else {
		c, err = kuttidb.New("127.0.0.1:7394", 4)
	}
	if err != nil {
		log.Fatal(err)
	}
	defer c.Close()

	// single ops
	if err := c.Put("hello", []byte("world")); err != nil {
		log.Fatal(err)
	}
	v, err := c.Get("hello")
	if err != nil || string(v) != "world" {
		log.Fatalf("get: %v %q", err, v)
	}
	if missing, _ := c.Get("nope"); missing != nil {
		log.Fatal("expected miss")
	}
	ok, _ := c.Delete("hello")
	if !ok {
		log.Fatal("delete should report hit")
	}

	// ttl
	if err := c.PutWithTTL("ttl-key", []byte("brief"), 30*time.Second); err != nil {
		log.Fatal(err)
	}
	if v, _ := c.Get("ttl-key"); v == nil {
		log.Fatal("ttl key should be alive")
	}

	// batched + concurrency
	pairs := map[string][]byte{}
	for i := 0; i < 5000; i++ {
		pairs[fmt.Sprintf("g%d", i)] = []byte(fmt.Sprintf("v%d", i))
	}
	if err := c.PutMany(pairs); err != nil {
		log.Fatal(err)
	}
	// batched with per-item TTL
	items := make([]kuttidb.Item, 0, 100)
	for i := 0; i < 100; i++ {
		ttl := time.Duration(30) * time.Second
		if i%2 == 0 {
			ttl = 0
		}
		items = append(items, kuttidb.Item{
			Key: fmt.Sprintf("bt%d", i), Value: []byte("y10"), TTL: ttl,
		})
	}
	if err := c.PutManyTTL(items); err != nil {
		log.Fatal(err)
	}
	if v, _ := c.Get("bt0"); v == nil {
		log.Fatal("bt0 should exist")
	}
	if v, _ := c.Get("bt1"); v == nil {
		log.Fatal("bt1 should exist")
	}

	var wg sync.WaitGroup
	errCh := make(chan error, 8)
	for w := 0; w < 8; w++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			keys := make([]string, 0, 625)
			for i := id * 625; i < (id+1)*625; i++ {
				keys = append(keys, fmt.Sprintf("g%d", i))
			}
			got, err := c.GetMany(keys)
			if err != nil {
				errCh <- err
				return
			}
			for i, k := range keys {
				want := fmt.Sprintf("v%s", k[1:])
				if string(got[i]) != want {
					errCh <- fmt.Errorf("mismatch %s: %q != %q", k, got[i], want)
					return
				}
			}
		}(w)
	}
	wg.Wait()
	close(errCh)
	for e := range errCh {
		log.Fatal(e)
	}

	stats, _ := c.Stats()
	fmt.Printf("GO CLIENT OK — stats: %s\n", stats)
}
