package main

import (
	"context"
	"fmt"
	"log"
	"os"
	"time"

	mocache "github.com/med/mocache/sdk/go"
)

func main() {
	nodes := []string{
		"http://cache-0.cache-headless.svc.cluster.local:8090",
		"http://cache-1.cache-headless.svc.cluster.local:8090",
		"http://cache-2.cache-headless.svc.cluster.local:8090",
	}
	if v := os.Getenv("MOCACHE_NODES"); v != "" {
		nodes = splitComma(v)
	}

	client := mocache.New(nodes, mocache.WithTimeout(time.Second))
	defer client.Close()

	// Sync: blocks until timeout.
	if err := client.Set("user:123", []byte("some_value"), 300*time.Second); err != nil {
		log.Fatal(err)
	}
	val, ok, err := client.Get("user:123")
	if err != nil {
		log.Fatal(err)
	}
	fmt.Printf("sync hit=%v val=%s\n", ok, val)

	// Context: cancellable (idiomatic Go "async").
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	if err := client.SetContext(ctx, "order:1", []byte("x"), time.Minute); err != nil {
		log.Fatal(err)
	}

	// Channel async: does not block the caller on the round-trip.
	res := <-client.GetAsync(ctx, "order:1")
	if res.Err != nil {
		log.Fatal(res.Err)
	}
	fmt.Printf("async found=%v val=%s\n", res.Found, res.Value)

	n, err := client.InvalidatePrefix("user:")
	if err != nil {
		log.Fatal(err)
	}
	fmt.Printf("invalidated prefix=%d\n", n)
	n, err = client.InvalidateRegex(`^order:`)
	if err != nil {
		log.Fatal(err)
	}
	fmt.Printf("invalidated regex=%d\n", n)
}

func splitComma(s string) []string {
	var out []string
	start := 0
	for i := 0; i <= len(s); i++ {
		if i == len(s) || s[i] == ',' {
			if i > start {
				out = append(out, s[start:i])
			}
			start = i + 1
		}
	}
	return out
}
