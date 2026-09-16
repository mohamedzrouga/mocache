package main

import (
	"flag"
	"log"
	"net"
	"net/http"
	"os"
	"time"

	"github.com/med/mocache/internal/cache"
	"github.com/med/mocache/internal/server"
)

func main() {
	httpAddr := flag.String("http", ":8090", "HTTP listen address")
	rpcAddr := flag.String("rpc", ":8091", "binary RPC (gRPC-style) listen address")
	capacity := flag.Int("capacity", 100_000, "maximum number of cached items")
	flag.Parse()

	if *capacity < 1 {
		log.Fatal("capacity must be >= 1")
	}

	c := cache.New(*capacity)
	errc := make(chan error, 2)

	httpSrv := &http.Server{
		Addr:              *httpAddr,
		Handler:           server.NewMux(c),
		ReadHeaderTimeout: 5 * time.Second,
	}
	go func() {
		log.Printf("http listening on %s (capacity=%d)", *httpAddr, *capacity)
		errc <- httpSrv.ListenAndServe()
	}()

	if *rpcAddr != "" {
		ln, err := net.Listen("tcp", *rpcAddr)
		if err != nil {
			log.Fatal(err)
		}
		go func() {
			log.Printf("rpc listening on %s", *rpcAddr)
			errc <- server.ServeRPC(ln, c)
		}()
	}

	err := <-errc
	log.Println(err)
	os.Exit(1)
}
