// Package benchmark measures the consensus + fsync path with an in-process
// transport. Results exclude RESP parsing, HTTP serialization and kernel RTT.
package benchmark

import (
	"context"
	"fmt"
	"raftkv/internal/harness"
	"raftkv/internal/statemachine"
	"sort"
	"sync"
	"time"
)

type Result struct {
	Nodes        int     `json:"nodes"`
	Clients      int     `json:"clients"`
	Operation    string  `json:"operation"`
	Completed    int     `json:"completed"`
	Errors       int     `json:"errors"`
	Seconds      float64 `json:"seconds"`
	OpsPerSecond float64 `json:"ops_per_second"`
	P50MS        float64 `json:"p50_ms"`
	P95MS        float64 `json:"p95_ms"`
	P99MS        float64 `json:"p99_ms"`
}

func Workload(c *harness.Cluster, clients, operations int, op string) Result {
	start := time.Now()
	var mu sync.Mutex
	var wg sync.WaitGroup
	latencies := []time.Duration{}
	errors := 0
	for client := 0; client < clients; client++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			for i := id; i < operations; i += clients {
				args := []string{"GET", "benchmark"}
				if op == "SET" {
					args = []string{"SET", "benchmark", fmt.Sprint(i)}
				}
				ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
				begin := time.Now()
				_, e := c.Execute(ctx, statemachine.Command{Args: args})
				elapsed := time.Since(begin)
				cancel()
				mu.Lock()
				if e != nil {
					errors++
				} else {
					latencies = append(latencies, elapsed)
				}
				mu.Unlock()
			}
		}(client)
	}
	wg.Wait()
	elapsed := time.Since(start)
	sort.Slice(latencies, func(i, j int) bool { return latencies[i] < latencies[j] })
	percentile := func(p int) float64 {
		if len(latencies) == 0 {
			return 0
		}
		i := (len(latencies)*p+99)/100 - 1
		return float64(latencies[i]) / float64(time.Millisecond)
	}
	return Result{len(c.IDs), clients, op, len(latencies), errors, elapsed.Seconds(), float64(len(latencies)) / elapsed.Seconds(), percentile(50), percentile(95), percentile(99)}
}

type Recovery struct {
	Nodes              int     `json:"nodes"`
	FailoverMS         float64 `json:"failover_ms"`
	LogReplayMS        float64 `json:"log_replay_ms"`
	SnapshotMS         float64 `json:"snapshot_ms"`
	ReplaySnapshotUsed bool    `json:"replay_snapshot_used"`
	SnapshotUsed       bool    `json:"snapshot_used"`
}

func Recover(c *harness.Cluster) (Recovery, error) {
	out := Recovery{Nodes: len(c.IDs)}
	ctx, cancel := context.WithTimeout(context.Background(), 8*time.Second)
	defer cancel()
	l, e := c.WaitLeader(ctx, "")
	if e != nil {
		return out, e
	}
	id := l.Status().ID
	start := time.Now()
	c.Crash(id)
	if _, e = c.WaitLeader(ctx, id); e != nil {
		return out, e
	}
	if _, e = c.Execute(ctx, statemachine.Command{Args: []string{"SET", "recovery", "ok"}}); e != nil {
		return out, e
	}
	out.FailoverMS = float64(time.Since(start)) / float64(time.Millisecond)
	if e = c.Restart(id); e != nil {
		return out, e
	}
	if e = c.Converge(ctx); e != nil {
		return out, e
	}
	l = c.Leader("")
	for _, p := range c.IDs {
		if p != l.Status().ID {
			id = p
			break
		}
	}
	lag := c.Nodes()[id]
	base := lag.Status().SnapshotIndex
	c.Network.Isolate(id, c.IDs)
	for i := 0; i < 5; i++ {
		if _, e = c.Execute(ctx, statemachine.Command{Args: []string{"INCR", "recovery-counter"}}); e != nil {
			return out, e
		}
	}
	start = time.Now()
	c.Network.Heal()
	if e = c.Converge(ctx); e != nil {
		return out, e
	}
	out.LogReplayMS = float64(time.Since(start)) / float64(time.Millisecond)
	out.ReplaySnapshotUsed = lag.Status().SnapshotIndex > base
	c.Crash(id)
	for i := 0; i < 150; i++ {
		if _, e = c.Execute(ctx, statemachine.Command{Args: []string{"INCR", "recovery-counter"}}); e != nil {
			return out, e
		}
	}
	start = time.Now()
	if e = c.Restart(id); e != nil {
		return out, e
	}
	if e = c.Converge(ctx); e != nil {
		return out, e
	}
	out.SnapshotMS = float64(time.Since(start)) / float64(time.Millisecond)
	out.SnapshotUsed = c.Nodes()[id].Status().SnapshotIndex > base
	return out, nil
}
