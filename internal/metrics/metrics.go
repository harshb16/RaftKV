package metrics

import (
	"fmt"
	"io"
	"raftkv/internal/raft"
	"sort"
	"sync/atomic"
	"time"
)

var bounds = []time.Duration{time.Millisecond, 5 * time.Millisecond, 10 * time.Millisecond, 50 * time.Millisecond, 100 * time.Millisecond, 500 * time.Millisecond, time.Second, 5 * time.Second}

type Registry struct {
	count    atomic.Uint64
	failures atomic.Uint64
	ns       atomic.Uint64
	buckets  [8]atomic.Uint64
	start    time.Time
}

func New() *Registry { return &Registry{start: time.Now()} }
func (r *Registry) Observe(elapsed time.Duration, failed bool) {
	r.count.Add(1)
	r.ns.Add(uint64(elapsed))
	if failed {
		r.failures.Add(1)
	}
	for i, b := range bounds {
		if elapsed <= b {
			r.buckets[i].Add(1)
		}
	}
}
func (r *Registry) Write(w io.Writer, s raft.Status) {
	fmt.Fprintln(w, "# TYPE raftkv_node_role gauge")
	for _, role := range []string{"follower", "candidate", "leader"} {
		v := 0
		if s.Role == role {
			v = 1
		}
		fmt.Fprintf(w, "raftkv_node_role{role=%q} %d\n", role, v)
	}
	fmt.Fprintf(w, "raftkv_leader_info{leader=%q} 1\n", s.Leader)
	for _, v := range []struct {
		name  string
		value uint64
	}{{"term", s.Term}, {"commit_index", s.Commit}, {"applied_index", s.Applied}, {"log_length", uint64(s.LogLength)}, {"snapshot_index", s.SnapshotIndex}, {"elections_total", s.ElectionCount}, {"rpc_received_total", s.RPCCount}} {
		fmt.Fprintf(w, "raftkv_%s %d\n", v.name, v.value)
	}
	peers := []string{}
	for p := range s.Lag {
		peers = append(peers, p)
	}
	sort.Strings(peers)
	for _, p := range peers {
		fmt.Fprintf(w, "raftkv_replication_lag{peer=%q} %d\n", p, s.Lag[p])
	}
	fmt.Fprintf(w, "# TYPE raftkv_commands_total counter\nraftkv_commands_total %d\nraftkv_command_errors_total %d\nraftkv_commands_per_second %g\n", r.count.Load(), r.failures.Load(), float64(r.count.Load())/time.Since(r.start).Seconds())
	fmt.Fprintln(w, "# TYPE raftkv_command_duration_seconds histogram")
	for i, b := range bounds {
		fmt.Fprintf(w, "raftkv_command_duration_seconds_bucket{le=%q} %d\n", fmt.Sprint(b.Seconds()), r.buckets[i].Load())
	}
	fmt.Fprintf(w, "raftkv_command_duration_seconds_bucket{le=\"+Inf\"} %d\nraftkv_command_duration_seconds_sum %g\nraftkv_command_duration_seconds_count %d\n", r.count.Load(), float64(r.ns.Load())/1e9, r.count.Load())
}
