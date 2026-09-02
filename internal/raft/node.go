// Package raft implements static-membership Raft. One mutex owns protocol state;
// RPC I/O runs outside it, and each follower has one replication worker.
package raft

import (
	"context"
	"crypto/sha256"
	"encoding/json"
	"errors"
	"fmt"
	"math/rand"
	"raftkv/internal/statemachine"
	"sync"
	"time"
)

var ErrStopped = errors.New("CLUSTERDOWN node stopped")

type outcome struct {
	result statemachine.Result
	err    error
}
type Node struct {
	mu        sync.Mutex
	cfg       Config
	store     Store
	transport Transport
	d         Durable
	m         *statemachine.Machine
	role      string
	leader    string
	applied   uint64
	next      map[string]uint64
	match     map[string]uint64
	pending   map[uint64]chan outcome
	wake      map[string]chan struct{}
	rng       *rand.Rand
	deadline  time.Time
	ctx       context.Context
	cancel    context.CancelFunc
	wg        sync.WaitGroup
	stopped   bool
	started   bool
	fatal     error
	elections uint64
	rpcs      uint64
}

func New(c Config, s Store, t Transport) (*Node, error) {
	if c.ID == "" || len(c.Peers) == 0 {
		return nil, fmt.Errorf("empty identity or membership")
	}
	seen := map[string]bool{}
	for _, p := range c.Peers {
		if p == "" || seen[p] {
			return nil, fmt.Errorf("invalid membership")
		}
		seen[p] = true
	}
	if !seen[c.ID] {
		return nil, fmt.Errorf("membership excludes self")
	}
	if c.ElectionMin == 0 {
		c.ElectionMin = 300 * time.Millisecond
	}
	if c.ElectionMax == 0 {
		c.ElectionMax = 600 * time.Millisecond
	}
	if c.Heartbeat == 0 {
		c.Heartbeat = 50 * time.Millisecond
	}
	if c.RPCTimeout == 0 {
		c.RPCTimeout = 250 * time.Millisecond
	}
	if c.ElectionMax <= c.ElectionMin || c.ElectionMin <= 2*c.Heartbeat {
		return nil, fmt.Errorf("invalid election timing")
	}
	ctx, cancel := context.WithCancel(context.Background())
	n := &Node{cfg: c, store: s, transport: t, m: statemachine.New(), role: "follower", next: map[string]uint64{}, match: map[string]uint64{}, pending: map[uint64]chan outcome{}, wake: map[string]chan struct{}{}, rng: rand.New(rand.NewSource(c.Seed)), ctx: ctx, cancel: cancel}
	b, e := s.Load()
	if e != nil {
		cancel()
		return nil, e
	}
	if len(b) > 0 {
		if e = json.Unmarshal(b, &n.d); e != nil {
			cancel()
			return nil, e
		}
	}
	if n.d.Commit < n.d.BaseIndex || n.d.Commit > n.last() {
		cancel()
		return nil, fmt.Errorf("invalid recovered commit index")
	}
	for i, e := range n.d.Log {
		if e.Index != n.d.BaseIndex+uint64(i)+1 || e.Term > n.d.Term {
			cancel()
			return nil, fmt.Errorf("invalid recovered log")
		}
	}
	if e = n.m.Restore(n.d.Snapshot); e != nil {
		cancel()
		return nil, e
	}
	n.applied = n.d.BaseIndex
	for n.applied < n.d.Commit {
		n.applied++
		e := n.entry(n.applied)
		if len(e.Command.Args) > 0 {
			n.m.Apply(e.Command)
		}
	}
	n.resetDeadline()
	return n, nil
}
func (n *Node) Start() {
	n.mu.Lock()
	defer n.mu.Unlock()
	if n.started || n.stopped {
		return
	}
	n.started = true
	n.wg.Add(1)
	go n.tick()
	for _, p := range n.cfg.Peers {
		if p != n.cfg.ID {
			n.wake[p] = make(chan struct{}, 1)
			n.wg.Add(1)
			go n.replicator(p, n.wake[p])
		}
	}
}
func (n *Node) Stop() {
	n.mu.Lock()
	n.stopped = true
	n.cancel()
	n.failPending(ErrStopped)
	n.mu.Unlock()
	n.wg.Wait()
}
func (n *Node) last() uint64 { return n.d.BaseIndex + uint64(len(n.d.Log)) }
func (n *Node) term(i uint64) uint64 {
	if i == n.d.BaseIndex {
		return n.d.BaseTerm
	}
	if i < n.d.BaseIndex || i > n.last() {
		return ^uint64(0)
	}
	return n.d.Log[i-n.d.BaseIndex-1].Term
}
func (n *Node) entry(i uint64) Entry { return n.d.Log[i-n.d.BaseIndex-1] }
func (n *Node) resetDeadline() {
	n.deadline = time.Now().Add(n.cfg.ElectionMin + time.Duration(n.rng.Int63n(int64(n.cfg.ElectionMax-n.cfg.ElectionMin))))
}
func (n *Node) failPending(e error) {
	for i, ch := range n.pending {
		ch <- outcome{err: e}
		delete(n.pending, i)
	}
}
func (n *Node) persist(compact bool) bool {
	var e error
	if compact {
		e = n.store.Compact(encode(n.d))
	} else {
		e = n.store.Save(encode(n.d))
	}
	if e != nil {
		if n.cfg.Logger != nil {
			n.cfg.Logger.Error("storage failure", "node", n.cfg.ID, "error", e)
		}
		n.fatal = e
		n.stopped = true
		n.cancel()
		n.failPending(fmt.Errorf("CLUSTERDOWN storage failure: %w", e))
		return false
	}
	return true
}
func (n *Node) stepDown(term uint64, leader string) bool {
	if term > n.d.Term {
		n.d.Term = term
		n.d.Vote = ""
		if !n.persist(false) {
			return false
		}
	}
	if n.role != "follower" {
		n.failPending(fmt.Errorf("TRYAGAIN leadership changed; outcome may be unknown"))
	}
	if n.cfg.Logger != nil && (n.role != "follower" || n.leader != leader) {
		n.cfg.Logger.Info("became follower", "node", n.cfg.ID, "term", n.d.Term, "leader", leader)
	}
	n.role = "follower"
	n.leader = leader
	return true
}
func (n *Node) notify() {
	for _, ch := range n.wake {
		select {
		case ch <- struct{}{}:
		default:
		}
	}
}
func (n *Node) tick() {
	defer n.wg.Done()
	ticker := time.NewTicker(10 * time.Millisecond)
	defer ticker.Stop()
	for {
		select {
		case <-n.ctx.Done():
			return
		case <-ticker.C:
			n.mu.Lock()
			if !n.stopped && n.role != "leader" && time.Now().After(n.deadline) {
				n.elect()
			}
			n.mu.Unlock()
		}
	}
}

// elect is called with the lock. Term and self-vote are durable before requests.
func (n *Node) elect() {
	n.role = "candidate"
	n.leader = ""
	n.d.Term++
	n.d.Vote = n.cfg.ID
	n.elections++
	n.resetDeadline()
	if !n.persist(false) {
		return
	}
	term := n.d.Term
	votes := 1
	if votes > len(n.cfg.Peers)/2 {
		n.becomeLeader()
		return
	}
	req := VoteRequest{term, n.cfg.ID, n.last(), n.term(n.last())}
	for _, p := range n.cfg.Peers {
		if p == n.cfg.ID {
			continue
		}
		n.wg.Add(1)
		go func(peer string) {
			defer n.wg.Done()
			ctx, cancel := context.WithTimeout(n.ctx, n.cfg.RPCTimeout)
			defer cancel()
			r, e := n.transport.Vote(ctx, n.cfg.ID, peer, req)
			n.mu.Lock()
			defer n.mu.Unlock()
			if n.stopped || e != nil {
				return
			}
			if r.Term > n.d.Term {
				n.stepDown(r.Term, "")
				return
			}
			if n.role != "candidate" || n.d.Term != term || r.Term != term {
				return
			}
			if r.Granted {
				votes++
				if votes > len(n.cfg.Peers)/2 {
					n.becomeLeader()
				}
			}
		}(p)
	}
}
func (n *Node) becomeLeader() {
	n.role = "leader"
	if n.cfg.Logger != nil {
		n.cfg.Logger.Info("became leader", "node", n.cfg.ID, "term", n.d.Term)
	}
	n.leader = n.cfg.ID
	for _, p := range n.cfg.Peers {
		n.next[p] = n.last() + 1
		n.match[p] = 0
	}
	n.d.Log = append(n.d.Log, Entry{Index: n.last() + 1, Term: n.d.Term})
	if !n.persist(false) {
		return
	}
	n.advanceCommit()
	n.notify()
}
func (n *Node) RequestVote(r VoteRequest) VoteResponse {
	n.mu.Lock()
	defer n.mu.Unlock()
	n.rpcs++
	if n.stopped {
		return VoteResponse{Term: n.d.Term}
	}
	if r.Term > n.d.Term {
		n.stepDown(r.Term, "")
	}
	out := VoteResponse{Term: n.d.Term}
	if n.stopped || r.Term != n.d.Term {
		return out
	}
	lastTerm := n.term(n.last())
	up := r.LastTerm > lastTerm || r.LastTerm == lastTerm && r.LastIndex >= n.last()
	if up && (n.d.Vote == "" || n.d.Vote == r.Candidate) {
		n.d.Vote = r.Candidate
		if n.persist(false) {
			n.resetDeadline()
			out.Granted = true
		}
	}
	return out
}
func (n *Node) AppendEntries(r AppendRequest) AppendResponse {
	n.mu.Lock()
	defer n.mu.Unlock()
	n.rpcs++
	out := AppendResponse{Term: n.d.Term, Next: n.last() + 1}
	if n.stopped || r.Term < n.d.Term {
		return out
	}
	if !n.stepDown(r.Term, r.Leader) {
		return AppendResponse{Term: n.d.Term}
	}
	n.resetDeadline()
	out.Term = n.d.Term
	if r.PrevIndex < n.d.BaseIndex {
		out.Next = n.d.BaseIndex + 1
		return out
	}
	if r.PrevIndex > n.last() {
		out.Next = n.last() + 1
		return out
	}
	if n.term(r.PrevIndex) != r.PrevTerm {
		term := n.term(r.PrevIndex)
		i := r.PrevIndex
		for i > n.d.BaseIndex && n.term(i-1) == term {
			i--
		}
		out.Next = i
		return out
	}
	changed := false
	for j, e := range r.Entries {
		if e.Index != r.PrevIndex+uint64(j)+1 || e.Term > r.Term {
			return out
		}
		if e.Index <= n.last() {
			if n.term(e.Index) == e.Term {
				continue
			}
			if e.Index <= n.d.Commit {
				return out
			}
			n.d.Log = n.d.Log[:e.Index-n.d.BaseIndex-1]
		}
		n.d.Log = append(n.d.Log, e)
		changed = true
	}
	match := r.PrevIndex + uint64(len(r.Entries))
	commit := r.Commit
	if commit > match {
		commit = match
	}
	if commit > n.d.Commit {
		n.d.Commit = commit
		changed = true
	}
	if changed && !n.persist(false) {
		return out
	}
	n.apply()
	out.Success = !n.stopped
	out.Match = match
	out.Next = match + 1
	return out
}
func (n *Node) advanceCommit() {
	for i := n.last(); i > n.d.Commit; i-- {
		if n.term(i) != n.d.Term {
			continue
		}
		count := 1
		for _, p := range n.cfg.Peers {
			if p != n.cfg.ID && n.match[p] >= i {
				count++
			}
		}
		if count > len(n.cfg.Peers)/2 {
			n.d.Commit = i
			if n.persist(false) {
				n.apply()
				n.notify()
			}
			break
		}
	}
}
func (n *Node) apply() {
	for n.applied < n.d.Commit {
		n.applied++
		e := n.entry(n.applied)
		var r statemachine.Result
		if len(e.Command.Args) > 0 {
			r = n.m.Apply(e.Command)
		}
		if ch, ok := n.pending[n.applied]; ok {
			ch <- outcome{result: r}
			delete(n.pending, n.applied)
		}
	}
	n.maybeSnapshot()
}
func (n *Node) replicator(peer string, wake <-chan struct{}) {
	defer n.wg.Done()
	ticker := time.NewTicker(n.cfg.Heartbeat)
	defer ticker.Stop()
	for {
		select {
		case <-n.ctx.Done():
			return
		case <-ticker.C:
		case <-wake:
		}
		n.replicate(peer)
	}
}
func (n *Node) replicate(peer string) {
	n.mu.Lock()
	if n.stopped || n.role != "leader" {
		n.mu.Unlock()
		return
	}
	term := n.d.Term
	if n.next[peer] <= n.d.BaseIndex {
		n.replicateSnapshot(peer, term)
		return
	}
	next := n.next[peer]
	if next == 0 {
		next = 1
	}
	prev := next - 1
	entries := []Entry{}
	if next <= n.last() {
		end := n.last() + 1
		if end > next+64 {
			end = next + 64
		}
		entries = append(entries, n.d.Log[next-n.d.BaseIndex-1:end-n.d.BaseIndex-1]...)
	}
	req := AppendRequest{term, n.cfg.ID, prev, n.term(prev), entries, n.d.Commit}
	n.mu.Unlock()
	ctx, cancel := context.WithTimeout(n.ctx, n.cfg.RPCTimeout)
	defer cancel()
	r, e := n.transport.Append(ctx, n.cfg.ID, peer, req)
	n.mu.Lock()
	defer n.mu.Unlock()
	if n.stopped || e != nil {
		return
	}
	if r.Term > n.d.Term {
		n.stepDown(r.Term, "")
		return
	}
	if n.role != "leader" || n.d.Term != term || r.Term != term {
		return
	}
	if r.Success {
		if r.Match > n.match[peer] {
			n.match[peer] = r.Match
		}
		n.next[peer] = n.match[peer] + 1
		n.advanceCommit()
		if n.next[peer] <= n.last() {
			select {
			case n.wake[peer] <- struct{}{}:
			default:
			}
		}
	} else {
		if r.Next < next {
			n.next[peer] = r.Next
		} else if next > 1 {
			n.next[peer] = next - 1
		}
		select {
		case n.wake[peer] <- struct{}{}:
		default:
		}
	}
}

// Execute logs reads too. A read's application is its linearization point. An
// isolated former leader cannot serve stale data because it cannot commit it.
func (n *Node) Execute(ctx context.Context, c statemachine.Command) (statemachine.Result, error) {
	if e := statemachine.Validate(c); e != nil {
		return statemachine.Result{}, e
	}
	n.mu.Lock()
	if n.stopped {
		n.mu.Unlock()
		return statemachine.Result{}, ErrStopped
	}
	if n.role != "leader" {
		leader := n.leader
		n.mu.Unlock()
		return statemachine.Result{}, fmt.Errorf("MOVED %s", leader)
	}
	c.Args = append([]string(nil), c.Args...)
	i := n.last() + 1
	ch := make(chan outcome, 1)
	n.pending[i] = ch
	n.d.Log = append(n.d.Log, Entry{i, n.d.Term, c})
	if !n.persist(false) {
		n.mu.Unlock()
		return statemachine.Result{}, ErrStopped
	}
	n.advanceCommit()
	n.notify()
	n.mu.Unlock()
	select {
	case o := <-ch:
		return o.result, o.err
	case <-ctx.Done():
		n.mu.Lock()
		if n.pending[i] == ch {
			delete(n.pending, i)
		}
		n.mu.Unlock()
		return statemachine.Result{}, fmt.Errorf("TRYAGAIN deadline exceeded; outcome may be unknown")
	}
}
func (n *Node) Status() Status {
	n.mu.Lock()
	defer n.mu.Unlock()
	lag := map[string]uint64{}
	if n.role == "leader" {
		for _, p := range n.cfg.Peers {
			if p != n.cfg.ID {
				lag[p] = n.last() - n.match[p]
			}
		}
	}
	s := Status{StateHash: fmt.Sprintf("%x", sha256.Sum256(n.m.Snapshot())), ID: n.cfg.ID, Role: n.role, Term: n.d.Term, Leader: n.leader, Commit: n.d.Commit, Applied: n.applied, LogLength: len(n.d.Log), LastIndex: n.last(), SnapshotIndex: n.d.BaseIndex, ElectionCount: n.elections, RPCCount: n.rpcs, Lag: lag}
	if n.fatal != nil {
		s.Error = n.fatal.Error()
	}
	return s
}
func (n *Node) State() map[string]string {
	n.mu.Lock()
	defer n.mu.Unlock()
	d := map[string]string{}
	for k, v := range n.m.Data {
		d[k] = v
	}
	return d
}
