package main

import (
	"fmt"
	"os"
	"strconv"
	"sync"
	"time"

	"kuttidb"
)

func main() {
	port := "7394"
	workers := 8
	per := 50000
	if len(os.Args) > 1 {
		port = os.Args[1]
	}
	if len(os.Args) > 2 {
		workers, _ = strconv.Atoi(os.Args[2])
	}
	if len(os.Args) > 3 {
		per, _ = strconv.Atoi(os.Args[3])
	}

	total := workers * per * 2
	c, err := kuttidb.New("127.0.0.1:"+port, workers)
	if err != nil {
		fmt.Println(err)
		os.Exit(1)
	}
	defer c.Close()

	start := time.Now()
	var wg sync.WaitGroup
	for w := 0; w < workers; w++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			pairs := map[string][]byte{}
			keys := make([]string, 0, per)
			for i := 0; i < per; i++ {
				k := fmt.Sprintf("b%d-%d", id, i)
				pairs[k] = []byte("x100bytes-padding-padding-padding-padding-padding-pa")
				keys = append(keys, k)
			}
			if err := c.PutMany(pairs); err != nil {
				panic(err)
			}
			got, err := c.GetMany(keys)
			if err != nil {
				panic(err)
			}
			for i := range got {
				if got[i] == nil {
					panic(fmt.Sprintf("miss %s", keys[i]))
				}
			}
		}(w)
	}
	wg.Wait()
	dt := time.Since(start)
	fmt.Printf("%d Go-client ops in %.2fs = %.0f ops/s\n", total, dt.Seconds(), float64(total)/dt.Seconds())
}
