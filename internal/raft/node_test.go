package raft

import (
	"context"
	"fmt"
	"raftkv/internal/statemachine"
	"sync"
	"testing"
	"time"
)

type wire struct {
	mu      sync.Mutex
	nodes   map[string]*Node
	blocked map[string]bool
}

func (w *wire) get(a, b string) (*Node, error) {
	w.mu.Lock()
	defer w.mu.Unlock()
	if w.blocked[a] || w.blocked[b] || w.nodes[b] == nil {
		return nil, fmt.Errorf("disconnected")
	}
	return w.nodes[b], nil
}
func (w *wire) Vote(_ context.Context, a, b string, r VoteRequest) (VoteResponse, error) {
	n, e := w.get(a, b)
	if e != nil {
		return VoteResponse{}, e
	}
	return n.RequestVote(r), nil
}
func (w *wire) Append(_ context.Context, a, b string, r AppendRequest) (AppendResponse, error) {
	n, e := w.get(a, b)
	if e != nil {
		return AppendResponse{}, e
	}
	return n.AppendEntries(r), nil
}
func (w *wire) Snapshot(_ context.Context, a, b string, r SnapshotRequest) (SnapshotResponse, error) {
	n, e := w.get(a, b)
	if e != nil {
		return SnapshotResponse{}, e
	}
	return n.InstallSnapshot(r), nil
}
func cluster(t *testing.T) ([]*Node, *wire) {
	t.Helper()
	w := &wire{nodes: map[string]*Node{}, blocked: map[string]bool{}}
	var ns []*Node
	for i, id := range []string{"a", "b", "c"} {
		n, e := New(Config{ID: id, Peers: []string{"a", "b", "c"}, ElectionMin: 100 * time.Millisecond, ElectionMax: 200 * time.Millisecond, Heartbeat: 20 * time.Millisecond, Seed: int64(i + 1)}, &MemoryStore{}, w)
		if e != nil {
			t.Fatal(e)
		}
		w.nodes[id] = n
		ns = append(ns, n)
	}
	for _, n := range ns {
		n.Start()
	}
	t.Cleanup(func() {
		for _, n := range ns {
			n.Stop()
		}
	})
	return ns, w
}
func awaitLeader(t *testing.T, ns []*Node, exclude string) *Node {
	t.Helper()
	end := time.Now().Add(3 * time.Second)
	for time.Now().Before(end) {
		for _, n := range ns {
			s := n.Status()
			if s.Role == "leader" && s.ID != exclude {
				return n
			}
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatal("no leader")
	return nil
}
func execute(t *testing.T, n *Node, args ...string) statemachine.Result {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	r, e := n.Execute(ctx, statemachine.Command{Args: args})
	if e != nil {
		t.Fatal(e)
	}
	return r
}
func TestElectionAndReplication(t *testing.T) {
	ns, _ := cluster(t)
	l := awaitLeader(t, ns, "")
	term := l.Status().Term
	execute(t, l, "SET", "x", "42")
	time.Sleep(350 * time.Millisecond)
	leaders := 0
	for _, n := range ns {
		if n.Status().Role == "leader" {
			leaders++
		}
		if n.Status().Term != term || n.State()["x"] != "42" {
			t.Fatal(n.Status(), n.State())
		}
	}
	if leaders != 1 {
		t.Fatal(leaders)
	}
	if r := execute(t, l, "GET", "x"); r.Text != "42" {
		t.Fatal(r)
	}
}
func TestPartitionSafetyAndRepair(t *testing.T) {
	ns, w := cluster(t)
	l := awaitLeader(t, ns, "")
	execute(t, l, "SET", "x", "old")
	w.mu.Lock()
	w.blocked[l.cfg.ID] = true
	w.mu.Unlock()
	ctx, cancel := context.WithTimeout(context.Background(), 150*time.Millisecond)
	defer cancel()
	if _, e := l.Execute(ctx, statemachine.Command{Args: []string{"SET", "x", "orphan"}}); e == nil {
		t.Fatal("isolated leader committed")
	}
	next := awaitLeader(t, ns, l.cfg.ID)
	execute(t, next, "SET", "x", "new")
	ctx2, c2 := context.WithTimeout(context.Background(), 80*time.Millisecond)
	defer c2()
	if _, e := l.Execute(ctx2, statemachine.Command{Args: []string{"GET", "x"}}); e == nil {
		t.Fatal("isolated leader read")
	}
	w.mu.Lock()
	w.blocked[l.cfg.ID] = false
	w.mu.Unlock()
	end := time.Now().Add(time.Second)
	for time.Now().Before(end) {
		ok := true
		for _, n := range ns {
			if n.State()["x"] != "new" {
				ok = false
			}
		}
		if ok {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatal("did not converge")
}
func TestVoteAndStaleRPC(t *testing.T) {
	n, e := New(Config{ID: "a", Peers: []string{"a"}}, &MemoryStore{}, nil)
	if e != nil {
		t.Fatal(e)
	}
	r := n.RequestVote(VoteRequest{Term: 7, Candidate: "b"})
	if !r.Granted {
		t.Fatal(r)
	}
	r = n.RequestVote(VoteRequest{Term: 7, Candidate: "c"})
	if r.Granted {
		t.Fatal("double vote")
	}
	a := n.AppendEntries(AppendRequest{Term: 6, Leader: "c"})
	if a.Success || a.Term != 7 {
		t.Fatal(a)
	}
}
