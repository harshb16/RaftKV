// Package harness runs real Raft engines and WALs with an instrumentable network.
package harness

import (
	"context"
	"fmt"
	"path/filepath"
	"raftkv/internal/raft"
	"raftkv/internal/statemachine"
	"raftkv/internal/storage"
	"raftkv/internal/transport"
	"reflect"
	"sync"
	"time"
)

type Cluster struct {
	mu        sync.Mutex
	IDs       []string
	Network   *transport.Sim
	nodes     map[string]*raft.Node
	stores    map[string]*storage.WAL
	dir       string
	threshold uint64
	seed      int64
}

func New(dir string, size int, threshold uint64, seed int64) (*Cluster, error) {
	if size != 1 && size != 3 && size != 5 {
		return nil, fmt.Errorf("cluster size must be 1, 3, or 5")
	}
	c := &Cluster{dir: dir, threshold: threshold, seed: seed, Network: transport.NewSim(uint64(seed)), nodes: map[string]*raft.Node{}, stores: map[string]*storage.WAL{}}
	for i := 1; i <= size; i++ {
		c.IDs = append(c.IDs, fmt.Sprintf("node%d", i))
	}
	for _, id := range c.IDs {
		if e := c.Restart(id); e != nil {
			c.Close()
			return nil, e
		}
	}
	return c, nil
}
func (c *Cluster) Restart(id string) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	valid := false
	for _, p := range c.IDs {
		if p == id {
			valid = true
		}
	}
	if !valid {
		return fmt.Errorf("unknown node")
	}
	if c.nodes[id] != nil {
		return nil
	}
	s, e := storage.Open(filepath.Join(c.dir, id))
	if e != nil {
		return e
	}
	n, e := raft.New(raft.Config{ID: id, Peers: c.IDs, ElectionMin: 180 * time.Millisecond, ElectionMax: 360 * time.Millisecond, Heartbeat: 30 * time.Millisecond, RPCTimeout: 150 * time.Millisecond, SnapshotThreshold: c.threshold, Seed: c.seed + int64(id[len(id)-1])}, s, c.Network)
	if e != nil {
		_ = s.Close()
		return e
	}
	c.nodes[id] = n
	c.stores[id] = s
	c.Network.Register(id, n)
	n.Start()
	return nil
}
func (c *Cluster) Crash(id string) {
	c.mu.Lock()
	defer c.mu.Unlock()
	n := c.nodes[id]
	if n == nil {
		return
	}
	c.Network.Remove(id)
	n.Stop()
	_ = c.stores[id].Close()
	delete(c.nodes, id)
	delete(c.stores, id)
}
func (c *Cluster) Close() {
	for _, id := range c.IDs {
		c.Crash(id)
	}
}
func (c *Cluster) Nodes() map[string]*raft.Node {
	c.mu.Lock()
	defer c.mu.Unlock()
	ns := map[string]*raft.Node{}
	for id, n := range c.nodes {
		ns[id] = n
	}
	return ns
}
func (c *Cluster) Leader(exclude string) *raft.Node {
	var best *raft.Node
	var term uint64
	for id, n := range c.Nodes() {
		s := n.Status()
		if id != exclude && s.Role == "leader" && (best == nil || s.Term > term) {
			best = n
			term = s.Term
		}
	}
	return best
}
func (c *Cluster) WaitLeader(ctx context.Context, exclude string) (*raft.Node, error) {
	for {
		if n := c.Leader(exclude); n != nil {
			return n, nil
		}
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case <-time.After(10 * time.Millisecond):
		}
	}
}

// Execute retries only leader discovery, never an operation already submitted.
// A timed-out mutation has an unknown outcome and must appear in the history.
func (c *Cluster) Execute(ctx context.Context, cmd statemachine.Command) (statemachine.Result, error) {
	n, e := c.WaitLeader(ctx, "")
	if e != nil {
		return statemachine.Result{}, e
	}
	return n.Execute(ctx, cmd)
}
func (c *Cluster) Converge(ctx context.Context) error {
	for {
		ns := c.Nodes()
		ok := len(ns) == len(c.IDs)
		var data map[string]string
		var index uint64
		for _, n := range ns {
			s := n.Status()
			d := n.State()
			if data == nil {
				data = d
				index = s.Commit
			}
			if s.Commit != index || s.Applied != index || s.LastIndex != index || !reflect.DeepEqual(data, d) {
				ok = false
			}
		}
		if ok && index > 0 {
			return nil
		}
		select {
		case <-ctx.Done():
			return fmt.Errorf("convergence: %w", ctx.Err())
		case <-time.After(20 * time.Millisecond):
		}
	}
}
