package main

import (
	"context"
	"flag"
	"log"
	"net"
	"raftkv/internal/server"
	"raftkv/internal/statemachine"
	"sync"
)

type local struct {
	mu sync.Mutex
	m  *statemachine.Machine
}

func (l *local) Execute(_ context.Context, c statemachine.Command) (statemachine.Result, error) {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.m.Apply(c), nil
}
func main() {
	addr := flag.String("addr", "127.0.0.1:6379", "RESP address")
	flag.Parse()
	l, e := net.Listen("tcp", *addr)
	if e != nil {
		log.Fatal(e)
	}
	log.Printf("standalone bootstrap listening on %s", *addr)
	log.Fatal(server.New(&local{m: statemachine.New()}).Serve(l))
}
