package transport

import (
	"context"
	"fmt"
	"hash/fnv"
	"raftkv/internal/raft"
	"sync"
	"time"
)

// Rule describes a directed link. Delay alternates with dispatch sequence so
// concurrent RPCs can arrive out of order. Decisions are repeatable per link.
type Rule struct {
	Blocked           bool
	DropEvery         uint64
	DropResponseEvery uint64
	DuplicateEvery    uint64
	MaxDelay          time.Duration
}
type link struct{ from, to string }
type endpoint struct {
	handler    raft.Handler
	generation uint64
}
type Sim struct {
	mu         sync.Mutex
	seed       uint64
	nodes      map[string]endpoint
	rules      map[link]Rule
	counts     map[link]uint64
	generation uint64
}

func NewSim(seed uint64) *Sim {
	return &Sim{seed: seed, nodes: map[string]endpoint{}, rules: map[link]Rule{}, counts: map[link]uint64{}}
}
func (s *Sim) Register(id string, h raft.Handler) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.generation++
	s.nodes[id] = endpoint{h, s.generation}
}
func (s *Sim) Remove(id string) { s.mu.Lock(); defer s.mu.Unlock(); delete(s.nodes, id) }
func (s *Sim) Set(from, to string, r Rule) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.rules[link{from, to}] = r
}
func (s *Sim) Heal() { s.mu.Lock(); defer s.mu.Unlock(); s.rules = map[link]Rule{} }
func (s *Sim) Isolate(id string, peers []string) {
	for _, p := range peers {
		if p != id {
			s.Set(id, p, Rule{Blocked: true})
			s.Set(p, id, Rule{Blocked: true})
		}
	}
}
func (s *Sim) dispatch(ctx context.Context, from, to string, f func(raft.Handler)) error {
	s.mu.Lock()
	key := link{from, to}
	s.counts[key]++
	seq := s.counts[key]
	rule := s.rules[key]
	ep, ok := s.nodes[to]
	_, source := s.nodes[from]
	s.mu.Unlock()
	if !ok || !source || rule.Blocked || rule.DropEvery > 0 && seq%rule.DropEvery == 0 {
		return fmt.Errorf("simulated message loss %s -> %s", from, to)
	}
	if rule.MaxDelay > 0 {
		h := fnv.New64a()
		_, _ = h.Write([]byte(from + "/" + to))
		delay := time.Duration((h.Sum64() ^ s.seed ^ (seq * 0x9e3779b97f4a7c15)) % uint64(rule.MaxDelay))
		timer := time.NewTimer(delay)
		defer timer.Stop()
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-timer.C:
		}
	}
	if e := ctx.Err(); e != nil {
		return e
	}
	s.mu.Lock()
	current, live := s.nodes[to]
	blocked := s.rules[key].Blocked
	_, source = s.nodes[from]
	s.mu.Unlock()
	if !live || !source || current.generation != ep.generation || blocked {
		return fmt.Errorf("endpoint crashed or link partitioned")
	}
	f(ep.handler)
	if rule.DuplicateEvery > 0 && seq%rule.DuplicateEvery == 0 {
		f(ep.handler)
	}
	if rule.DropResponseEvery > 0 && seq%rule.DropResponseEvery == 0 {
		return fmt.Errorf("simulated response loss")
	}
	return nil
}
func (s *Sim) Vote(ctx context.Context, from, to string, r raft.VoteRequest) (out raft.VoteResponse, e error) {
	e = s.dispatch(ctx, from, to, func(h raft.Handler) { out = h.RequestVote(r) })
	return
}
func (s *Sim) Append(ctx context.Context, from, to string, r raft.AppendRequest) (out raft.AppendResponse, e error) {
	e = s.dispatch(ctx, from, to, func(h raft.Handler) { out = h.AppendEntries(r) })
	return
}
func (s *Sim) Snapshot(ctx context.Context, from, to string, r raft.SnapshotRequest) (out raft.SnapshotResponse, e error) {
	e = s.dispatch(ctx, from, to, func(h raft.Handler) { out = h.InstallSnapshot(r) })
	return
}
