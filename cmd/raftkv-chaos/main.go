package main

import (
	"encoding/json"
	"flag"
	"fmt"
	"github.com/anishathalye/porcupine"
	"os"
	"path/filepath"
	"raftkv/internal/harness"
	"raftkv/internal/history"
	"time"
)

func main() {
	size := flag.Int("nodes", 3, "cluster size: 3 or 5")
	duration := flag.Duration("duration", 10*time.Second, "fault workload duration")
	clients := flag.Int("clients", 4, "concurrent clients")
	seed := flag.Int64("seed", 42, "workload and link seed")
	path := flag.String("history", "run/history.json", "recorded history output")
	eventsPath := flag.String("events", "run/events.jsonl", "cluster event JSONL output")
	checkPath := flag.String("check", "", "check a recorded history instead of launching a cluster")
	flag.Parse()
	if *checkPath != "" {
		b, e := os.ReadFile(*checkPath)
		if e != nil {
			panic(e)
		}
		var records []history.Record
		if e = json.Unmarshal(b, &records); e != nil {
			panic(e)
		}
		result, e := history.Check(records, 30*time.Second)
		fmt.Println("linearizability:", result)
		if e != nil || result != porcupine.Ok {
			fmt.Fprintln(os.Stderr, e)
			os.Exit(1)
		}
		return
	}
	for _, p := range []string{*path, *eventsPath} {
		if e := os.MkdirAll(filepath.Dir(p), 0700); e != nil {
			panic(e)
		}
	}
	events, e := os.Create(*eventsPath)
	if e != nil {
		panic(e)
	}
	defer events.Close()
	eventEncoder := json.NewEncoder(events)
	var eventError error
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
	rec, report, e := harness.Chaos(c, *duration, *clients, *seed, func(s string) {
		fmt.Printf("[%6.2fs] %s\n", time.Since(start).Seconds(), s)
		if err := eventEncoder.Encode(map[string]any{"elapsed_ms": time.Since(start).Milliseconds(), "event": s, "seed": *seed}); err != nil {
			eventError = err
		}
	})
	if err := os.MkdirAll("run", 0700); err != nil {
		panic(err)
	}
	if err := rec.Write(*path); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
	b, _ := json.MarshalIndent(report, "", "  ")
	fmt.Println(string(b))
	if eventError != nil {
		fmt.Fprintln(os.Stderr, eventError)
		os.Exit(1)
	}
	if e != nil {
		fmt.Fprintln(os.Stderr, e)
		os.Exit(1)
	}
}
