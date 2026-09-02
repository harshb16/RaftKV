package main

import (
	"flag"
	"fmt"
	"log/slog"
	"os"
	"os/signal"
	"raftkv/internal/server"
	"syscall"
)

func main() {
	if e := run(); e != nil {
		slog.Error("raftkv failed", "error", e)
		os.Exit(1)
	}
}
func run() error {
	f := flag.NewFlagSet("raftkv", flag.ContinueOnError)
	config := f.String("config", "configs/node1.json", "JSON node configuration")
	if e := f.Parse(os.Args[1:]); e != nil {
		return e
	}
	c, e := server.ReadConfig(*config)
	if e != nil {
		return e
	}
	rt, e := server.Launch(c)
	if e != nil {
		return e
	}
	defer rt.Close()
	slog.SetDefault(slog.New(slog.NewJSONHandler(os.Stderr, nil)))
	slog.Info("node started", "node", c.NodeID, "client", c.ClientAddr, "raft", c.RaftAddr)
	ch := make(chan os.Signal, 1)
	signal.Notify(ch, os.Interrupt, syscall.SIGTERM)
	defer signal.Stop(ch)
	<-ch
	fmt.Fprintln(os.Stderr, "stopping", c.NodeID)
	return nil
}
