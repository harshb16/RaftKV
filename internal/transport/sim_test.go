package transport

import (
	"context"
	"raftkv/internal/raft"
	"sync"
	"testing"
	"time"
)

type recording struct {
	mu    sync.Mutex
	terms []uint64
}

func (r *recording) RequestVote(v raft.VoteRequest) raft.VoteResponse {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.terms = append(r.terms, v.Term)
	return raft.VoteResponse{Term: v.Term, Granted: true}
}
func (r *recording) AppendEntries(raft.AppendRequest) raft.AppendResponse {
	return raft.AppendResponse{}
}
func (r *recording) InstallSnapshot(raft.SnapshotRequest) raft.SnapshotResponse {
	return raft.SnapshotResponse{}
}
func TestDirectedLossDuplicateAndDelayedCrash(t *testing.T) {
	s := NewSim(4)
	a, b := &recording{}, &recording{}
	s.Register("a", a)
	s.Register("b", b)
	s.Set("a", "b", Rule{DuplicateEvery: 1, DropResponseEvery: 1})
	if _, e := s.Vote(context.Background(), "a", "b", raft.VoteRequest{Term: 1}); e == nil {
		t.Fatal("response wasn't lost")
	}
	if len(b.terms) != 2 {
		t.Fatal(b.terms)
	}
	if _, e := s.Vote(context.Background(), "b", "a", raft.VoteRequest{Term: 2}); e != nil {
		t.Fatal("reverse link affected", e)
	}
	s.Set("a", "b", Rule{MaxDelay: time.Second})
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, e := s.Vote(ctx, "a", "b", raft.VoteRequest{Term: 3}); e == nil {
		t.Fatal("ignored canceled delay")
	}
}
func TestConcurrentMessagesCanReorder(t *testing.T) {
	s := NewSim(1)
	a, b := &recording{}, &recording{}
	s.Register("a", a)
	s.Register("b", b)
	s.Set("a", "b", Rule{MaxDelay: 150 * time.Millisecond})
	var wg sync.WaitGroup
	for i := 1; i <= 10; i++ {
		wg.Add(1)
		go func(term uint64) {
			defer wg.Done()
			_, _ = s.Vote(context.Background(), "a", "b", raft.VoteRequest{Term: term})
		}(uint64(i))
		time.Sleep(time.Millisecond)
	}
	wg.Wait()
	b.mu.Lock()
	defer b.mu.Unlock()
	ordered := true
	for i := 1; i < len(b.terms); i++ {
		if b.terms[i] < b.terms[i-1] {
			ordered = false
		}
	}
	if ordered {
		t.Fatal("expected out-of-order arrival", b.terms)
	}
}
