package raft

import (
	"context"
	"fmt"
	"raftkv/internal/statemachine"
	"testing"
)

func TestCommitOnlyCurrentTerm(t *testing.T) {
	n, _ := New(Config{ID: "a", Peers: []string{"a", "b", "c"}}, &MemoryStore{}, nil)
	defer n.Stop()
	n.mu.Lock()
	defer n.mu.Unlock()
	n.d.Term = 2
	n.role = "leader"
	n.d.Log = []Entry{{Index: 1, Term: 1}}
	n.match["b"] = 1
	n.advanceCommit()
	if n.d.Commit != 0 {
		t.Fatal("committed old term using replica count")
	}
	n.d.Log = append(n.d.Log, Entry{Index: 2, Term: 2})
	n.match["b"] = 2
	n.advanceCommit()
	if n.d.Commit != 2 || n.applied != 2 {
		t.Fatal(n.d.Commit, n.applied)
	}
}
func TestHeartbeatDoesNotCommitDivergentSuffix(t *testing.T) {
	n, _ := New(Config{ID: "a", Peers: []string{"a", "b", "c"}}, &MemoryStore{}, nil)
	defer n.Stop()
	r := n.AppendEntries(AppendRequest{Term: 1, Leader: "b", Entries: []Entry{{Index: 1, Term: 1}, {Index: 2, Term: 1, Command: statemachine.Command{Args: []string{"SET", "x", "orphan"}}}}})
	if !r.Success {
		t.Fatal(r)
	}
	r = n.AppendEntries(AppendRequest{Term: 2, Leader: "c", PrevIndex: 1, PrevTerm: 1, Commit: 2})
	if !r.Success || n.Status().Commit != 1 || len(n.State()) != 0 {
		t.Fatal(r, n.Status(), n.State())
	}
	r = n.AppendEntries(AppendRequest{Term: 2, Leader: "c", PrevIndex: 1, PrevTerm: 1, Entries: []Entry{{Index: 2, Term: 2, Command: statemachine.Command{Args: []string{"SET", "x", "correct"}}}}, Commit: 2})
	if !r.Success || n.State()["x"] != "correct" {
		t.Fatal(r, n.State())
	}
	r = n.AppendEntries(AppendRequest{Term: 2, Leader: "c", PrevIndex: 1, PrevTerm: 1, Entries: []Entry{{Index: 2, Term: 2, Command: statemachine.Command{Args: []string{"SET", "x", "correct"}}}}, Commit: 2})
	if !r.Success || n.Status().LastIndex != 2 {
		t.Fatal("duplicate entry", r)
	}
}

type delayedResponse struct {
	wire
	started chan struct{}
	reply   chan AppendResponse
}

func (d *delayedResponse) Append(_ context.Context, from, to string, r AppendRequest) (AppendResponse, error) {
	close(d.started)
	return <-d.reply, nil
}
func TestStaleReplicationResponseFenced(t *testing.T) {
	d := &delayedResponse{started: make(chan struct{}), reply: make(chan AppendResponse, 1)}
	n, _ := New(Config{ID: "a", Peers: []string{"a", "b", "c"}}, &MemoryStore{}, d)
	defer n.Stop()
	n.mu.Lock()
	n.d.Term = 3
	n.role = "leader"
	n.d.Log = []Entry{{Index: 1, Term: 3}}
	n.next["b"] = 1
	n.mu.Unlock()
	done := make(chan struct{})
	go func() { n.replicate("b"); close(done) }()
	<-d.started
	n.mu.Lock()
	n.stepDown(4, "c")
	n.mu.Unlock()
	d.reply <- AppendResponse{Term: 3, Success: true, Match: 1}
	<-done
	n.mu.Lock()
	defer n.mu.Unlock()
	if n.match["b"] != 0 || n.d.Commit != 0 || n.role != "follower" {
		t.Fatal("stale response modified new term")
	}
}

type failingStore struct{ MemoryStore }

func (f *failingStore) Save([]byte) error { return fmt.Errorf("disk failed") }
func TestStorageFailureNeverAcknowledgesVote(t *testing.T) {
	n, _ := New(Config{ID: "a", Peers: []string{"a"}}, &failingStore{}, nil)
	defer n.Stop()
	r := n.RequestVote(VoteRequest{Term: 1, Candidate: "b"})
	if r.Granted || n.Status().Error == "" {
		t.Fatal(r, n.Status())
	}
}

func TestCompactedFollowerConflictHintCanMoveForward(t *testing.T) {
	w := &wire{nodes: map[string]*Node{}, blocked: map[string]bool{}}
	empty := statemachine.New().Snapshot()
	ld := Durable{Term: 3, BaseIndex: 128, BaseTerm: 1, Snapshot: empty, Commit: 200}
	for i := uint64(129); i <= 250; i++ {
		term := uint64(1)
		if i > 200 {
			term = 2
		}
		ld.Log = append(ld.Log, Entry{Index: i, Term: term})
	}
	fd := Durable{Term: 3, BaseIndex: 200, BaseTerm: 1, Snapshot: empty, Commit: 200}
	for i := uint64(201); i <= 240; i++ {
		fd.Log = append(fd.Log, Entry{Index: i, Term: 1})
	}
	ls, fs := &MemoryStore{}, &MemoryStore{}
	_ = ls.Save(encode(ld))
	_ = fs.Save(encode(fd))
	cfg := Config{ID: "a", Peers: []string{"a", "b", "c"}}
	l, e := New(cfg, ls, w)
	if e != nil {
		t.Fatal(e)
	}
	defer l.Stop()
	cfg.ID = "b"
	f, e := New(cfg, fs, w)
	if e != nil {
		t.Fatal(e)
	}
	defer f.Stop()
	w.nodes["a"] = l
	w.nodes["b"] = f
	l.mu.Lock()
	l.role = "leader"
	l.next["b"] = 241
	l.mu.Unlock()
	l.replicate("b")
	l.replicate("b")
	l.mu.Lock()
	next := l.next["b"]
	l.mu.Unlock()
	if next != 201 {
		t.Fatalf("expected forward snapshot hint 201, got %d", next)
	}
	l.replicate("b")
	if f.Status().LastIndex != 250 {
		t.Fatal(f.Status())
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.term(201) != 2 {
		t.Fatal("conflicting tail not repaired")
	}
}
