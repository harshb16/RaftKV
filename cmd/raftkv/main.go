package main

import (
	"encoding/json"
	"flag"
	"fmt"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"raftkv/internal/raft"
	"raftkv/internal/server"
	"sort"
	"syscall"
	"time"
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
	args := os.Args[1:]
	mode := "serve"
	if len(args) > 0 && (args[0] == "status" || args[0] == "cluster" || args[0] == "info") {
		mode = args[0]
		args = args[1:]
	}
	if e := f.Parse(args); e != nil {
		return e
	}
	c, e := server.ReadConfig(*config)
	if e != nil {
		return e
	}
	if mode != "serve" {
		return inspect(c, mode)
	}
	slog.SetDefault(slog.New(slog.NewJSONHandler(os.Stderr, nil)))
	rt, e := server.Launch(c)
	if e != nil {
		return e
	}
	defer rt.Close()
	slog.Info("node started", "node", c.NodeID, "client", c.ClientAddr, "raft", c.RaftAddr)
	ch := make(chan os.Signal, 1)
	signal.Notify(ch, os.Interrupt, syscall.SIGTERM)
	defer signal.Stop(ch)
	<-ch
	fmt.Fprintln(os.Stderr, "stopping", c.NodeID)
	return nil
}

func inspect(c server.Config, mode string) error {
	ids := []string{}
	for id := range c.Peers {
		ids = append(ids, id)
	}
	sort.Strings(ids)
	client := &http.Client{Timeout: time.Second}
	defer client.CloseIdleConnections()
	if mode != "info" {
		fmt.Printf("%-10s %-10s %6s %8s %8s %6s %8s\n", "NODE", "ROLE", "TERM", "COMMIT", "APPLIED", "LOG", "SNAPSHOT")
	}
	for _, id := range ids {
		r, e := client.Get("http://" + c.Peers[id] + "/status")
		var s raft.Status
		if e == nil {
			e = json.NewDecoder(r.Body).Decode(&s)
			_ = r.Body.Close()
		}
		if e != nil {
			fmt.Printf("%-10s offline\n", id)
			continue
		}
		if mode == "info" {
			b, _ := json.Marshal(s)
			fmt.Println(string(b))
		} else {
			fmt.Printf("%-10s %-10s %6d %8d %8d %6d %8d\n", id, s.Role, s.Term, s.Commit, s.Applied, s.LogLength, s.SnapshotIndex)
			if mode == "cluster" && s.Role == "leader" {
				fmt.Println("leader client address:", c.Clients[id])
			}
		}
	}
	return nil
}
