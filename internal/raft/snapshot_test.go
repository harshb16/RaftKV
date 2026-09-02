package raft

import (
	"context"
	"raftkv/internal/statemachine"
	"raftkv/internal/storage"
	"testing"
	"time"
)

func TestSnapshotRestoreAndTransfer(t *testing.T) {
	ns, w := cluster(t)
	for _, n := range ns {
		n.mu.Lock()
		n.cfg.SnapshotThreshold = 5
		n.mu.Unlock()
	}
	l := awaitLeader(t, ns, "")
	var lag *Node
	for _, n := range ns {
		if n != l {
			lag = n
			break
		}
	}
	w.mu.Lock()
	w.blocked[lag.cfg.ID] = true
	w.mu.Unlock()
	for i := 0; i < 30; i++ {
		execute(t, l, "INCR", "x")
	}
	if l.Status().SnapshotIndex < 25 || l.Status().LogLength >= 5 {
		t.Fatal(l.Status())
	}
	w.mu.Lock()
	w.blocked[lag.cfg.ID] = false
	w.mu.Unlock()
	end := time.Now().Add(3 * time.Second)
	for time.Now().Before(end) {
		if lag.State()["x"] == "30" && lag.Status().SnapshotIndex >= 25 {
			break
		}
		time.Sleep(10 * time.Millisecond)
	}
	if lag.State()["x"] != "30" {
		t.Fatal(lag.Status(), lag.State())
	}
	d := t.TempDir()
	s, e := storage.Open(d)
	if e != nil {
		t.Fatal(e)
	}
	cfg := Config{ID: "solo", Peers: []string{"solo"}, SnapshotThreshold: 3, ElectionMin: 100 * time.Millisecond, ElectionMax: 200 * time.Millisecond, Heartbeat: 20 * time.Millisecond}
	n, e := New(cfg, s, nil)
	if e != nil {
		t.Fatal(e)
	}
	n.Start()
	awaitLeader(t, []*Node{n}, "")
	execute(t, n, "SET", "\xff", "\xfe")
	execute(t, n, "INCR", "counter")
	if n.Status().SnapshotIndex == 0 {
		t.Fatal("no snapshot")
	}
	n.Stop()
	_ = s.Close()
	s, e = storage.Open(d)
	if e != nil {
		t.Fatal(e)
	}
	defer s.Close()
	n, e = New(cfg, s, nil)
	if e != nil {
		t.Fatal(e)
	}
	defer n.Stop()
	if n.State()["\xff"] != "\xfe" || n.State()["counter"] != "1" {
		t.Fatal(n.State())
	}
}
func TestStaleSnapshotDoesNotRollback(t *testing.T) {
	n, _ := New(Config{ID: "a", Peers: []string{"a"}}, &MemoryStore{}, nil)
	defer n.Stop()
	n.Start()
	awaitLeader(t, []*Node{n}, "")
	execute(t, n, "SET", "x", "latest")
	m := statemachine.New()
	r := n.InstallSnapshot(SnapshotRequest{Term: n.Status().Term + 1, Leader: "b", Index: 1, LastTerm: 1, Data: m.Snapshot()})
	if !r.Success || n.State()["x"] != "latest" {
		t.Fatal(r, n.State())
	}
	ctx, cancel := context.WithTimeout(context.Background(), time.Millisecond)
	defer cancel()
	if _, e := n.Execute(ctx, statemachine.Command{Args: []string{"GET", "x"}}); e == nil {
		t.Fatal("follower read")
	}
}
