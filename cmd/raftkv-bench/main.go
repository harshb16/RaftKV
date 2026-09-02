package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"os"
	"raftkv/internal/benchmark"
	"raftkv/internal/harness"
	"runtime"
	"time"
)

type report struct {
	Go        string               `json:"go"`
	Platform  string               `json:"platform"`
	Transport string               `json:"transport"`
	Results   []benchmark.Result   `json:"results"`
	Recovery  []benchmark.Recovery `json:"recovery"`
}

func main() {
	ops := flag.Int("operations", 300, "operations per workload")
	output := flag.String("output", "run/benchmark.json", "JSON output path")
	flag.Parse()
	if *ops < 1 {
		fmt.Fprintln(os.Stderr, "operations must be positive")
		os.Exit(1)
	}
	r := report{runtime.Version(), runtime.GOOS + "/" + runtime.GOARCH, "in-process fault transport, durable WAL fsync", nil, nil}
	for _, size := range []int{3, 5} {
		dir, e := os.MkdirTemp("", "raftkv-bench-*")
		if e != nil {
			panic(e)
		}
		c, e := harness.New(dir, size, 64, 42)
		if e != nil {
			panic(e)
		}
		ctx, cancel := context.WithTimeout(context.Background(), 4*time.Second)
		_, e = c.WaitLeader(ctx, "")
		cancel()
		if e != nil {
			panic(e)
		}
		for _, clients := range []int{1, 8} {
			for _, op := range []string{"SET", "GET"} {
				result := benchmark.Workload(c, clients, *ops, op)
				r.Results = append(r.Results, result)
				fmt.Printf("nodes=%d clients=%d %s %.1f ops/s p50=%.2fms p95=%.2fms p99=%.2fms errors=%d\n", size, clients, op, result.OpsPerSecond, result.P50MS, result.P95MS, result.P99MS, result.Errors)
			}
		}
		recovery, e := benchmark.Recover(c)
		c.Close()
		_ = os.RemoveAll(dir)
		if e != nil {
			panic(e)
		}
		r.Recovery = append(r.Recovery, recovery)
		fmt.Printf("recovery: %+v\n", recovery)
	}
	b, _ := json.MarshalIndent(r, "", "  ")
	if e := os.MkdirAll("run", 0700); e != nil {
		panic(e)
	}
	if e := os.WriteFile(*output, append(b, '\n'), 0600); e != nil {
		panic(e)
	}
}
