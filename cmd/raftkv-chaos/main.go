package main

import (
	"encoding/json"
	"flag"
	"fmt"
	"os"
	"raftkv/internal/harness"
	"time"
)

func main() {
	size := flag.Int("nodes", 3, "cluster size: 3 or 5")
	duration := flag.Duration("duration", 10*time.Second, "fault workload duration")
	clients := flag.Int("clients", 4, "concurrent clients")
	seed := flag.Int64("seed", 42, "workload and link seed")
	path := flag.String("history", "run/history.json", "recorded history output")
	flag.Parse()
	if *clients < 1 || *duration <= 0 {
		fmt.Fprintln(os.Stderr, "clients and duration must be positive")
		os.Exit(1)
	}
	dir, e := os.MkdirTemp("", "raftkv-chaos-*")
	if e != nil {
		panic(e)
	}
	defer os.RemoveAll(dir)
	c, e := harness.New(dir, *size, 16, *seed)
	if e != nil {
		panic(e)
	}
	defer c.Close()
	start := time.Now()
	rec, report, e := harness.Chaos(c, *duration, *clients, *seed, func(s string) { fmt.Printf("[%6.2fs] %s\n", time.Since(start).Seconds(), s) })
	if err := os.MkdirAll("run", 0700); err != nil {
		panic(err)
	}
	if err := rec.Write(*path); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
	b, _ := json.MarshalIndent(report, "", "  ")
	fmt.Println(string(b))
	if e != nil {
		fmt.Fprintln(os.Stderr, e)
		os.Exit(1)
	}
}
