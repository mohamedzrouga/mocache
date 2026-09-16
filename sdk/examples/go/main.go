package main

import (
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

	// HTTP (default). For the fast unary RPC path:
	//   mocache.New(nodes, mocache.WithProtocol(mocache.ProtocolGRPC))
	client := mocache.New(nodes, mocache.WithTimeout(time.Second))
	defer client.Close()

	if err := client.Set("user:123", []byte("some_value"), 300*time.Second); err != nil {
		log.Fatal(err)
	}
	val, ok, err := client.Get("user:123")
	if err != nil {
		log.Fatal(err) // node down / timeout — treat as miss in production
	}
	if ok {
		fmt.Printf("hit: %s\n", val)
	} else {
		fmt.Println("miss")
	}
	_ = client.Delete("user:123")
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
