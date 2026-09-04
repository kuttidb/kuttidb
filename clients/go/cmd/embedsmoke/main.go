package main

import (
	"fmt"
	"log"
	"sync"
	"time"

	"github.com/kuttidb/kuttidb/clients/go"
)

func main() {
	region := "/tmp/ctest-embed/db.embed"
	port := "7398"

	// attach via shared memory
	db, err := kuttidb.OpenEmbed(region)
	if err != nil {
		log.Fatal(err)
	}
	defer db.Close()

	// wire client on the same table (server serves both frontends)
	wc, err := kuttidb.New("127.0.0.1:"+port, 2)
	if err != nil {
		log.Fatal(err)
	}
	defer wc.Close()

	// embed write -> network read
	if err := db.Put("go-embed", []byte("via shared memory"), 0); err != nil {
		log.Fatal(err)
	}
	v, _ := wc.Get("go-embed")
	if string(v) != "via shared memory" {
		log.Fatalf("network read of embed write failed: %q", v)
	}

	// network write -> embed read
	if err := wc.Put("go-net", []byte("via tcp")); err != nil {
		log.Fatal(err)
	}
	v, ok, err := db.Get("go-net")
	if err != nil || !ok || string(v) != "via tcp" {
		log.Fatalf("embed read of network write failed: %v %v", ok, err)
	}

	// ttl via embed
	if err := db.Put("go-ttl", []byte("brief"), 30*time.Second); err != nil {
		log.Fatal(err)
	}
	if _, ok, _ := db.Get("go-ttl"); !ok {
		log.Fatal("ttl key should be alive")
	}

	// concurrent embed writers + readers
	var wg sync.WaitGroup
	for g := 0; g < 8; g++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			for i := 0; i < 5000; i++ {
				k := fmt.Sprintf("gem%d-%d", id, i)
				if err := db.Put(k, []byte("x100bytes-padding-padding-padding-padding-padding"), 0); err != nil {
					log.Fatal(err)
				}
				if _, ok, _ := db.Get(k); !ok {
					log.Fatalf("missing %s", k)
				}
			}
		}(g)
	}
	wg.Wait()

	fmt.Printf("GO EMBED OK — count=%d mem=%d\n", db.Count(), db.MemUsage())
}
