package raft

import (
	"context"
	"raftkv/internal/statemachine"
	"raftkv/internal/storage"
	"testing"
	"time"
)

func TestDurableVoteAndEntries(t *testing.T) {
	d := t.TempDir()
	s, e := storage.Open(d)
	if e != nil {
		t.Fatal(e)
	}
	cfg := Config{ID: "a", Peers: []string{"a"}}
	n, e := New(cfg, s, nil)
	if e != nil {
		t.Fatal(e)
	}
	if !n.RequestVote(VoteRequest{Term: 9, Candidate: "b"}).Granted {
		t.Fatal("vote")
	}
	r := n.AppendEntries(AppendRequest{Term: 9, Leader: "b", Entries: []Entry{{1, 9, statemachine.Command{Args: []string{"SET", "x", "saved"}}}}, Commit: 1})
	if !r.Success {
		t.Fatal(r)
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
	if n.Status().Term != 9 || n.State()["x"] != "saved" {
		t.Fatal(n.Status(), n.State())
	}
	if n.RequestVote(VoteRequest{Term: 9, Candidate: "c", LastIndex: 1, LastTerm: 9}).Granted {
		t.Fatal("forgot vote")
	}
	n.Start()
	end := time.Now().Add(2 * time.Second)
	for n.Status().Role != "leader" && time.Now().Before(end) {
		time.Sleep(10 * time.Millisecond)
	}
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	if _, e = n.Execute(ctx, statemachine.Command{Args: []string{"GET", "x"}}); e != nil {
		t.Fatal(e)
	}
}
