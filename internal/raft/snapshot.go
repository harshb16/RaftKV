package raft

import (
	"context"
	"raftkv/internal/statemachine"
)

// Called after ordered application, with the mutex held. The WAL atomically
// checkpoints the snapshot and remaining log as one crash-consistent unit.
func (n *Node) maybeSnapshot() {
	if n.stopped || n.cfg.SnapshotThreshold == 0 || n.applied-n.d.BaseIndex < n.cfg.SnapshotThreshold {
		return
	}
	index := n.applied
	term := n.term(index)
	n.d.Snapshot = n.m.Snapshot()
	n.d.Log = append([]Entry(nil), n.d.Log[index-n.d.BaseIndex:]...)
	n.d.BaseIndex = index
	n.d.BaseTerm = term
	n.persist(true)
}
func (n *Node) InstallSnapshot(r SnapshotRequest) SnapshotResponse {
	n.mu.Lock()
	defer n.mu.Unlock()
	n.rpcs++
	out := SnapshotResponse{Term: n.d.Term}
	if n.stopped || r.Term < n.d.Term {
		return out
	}
	if !n.stepDown(r.Term, r.Leader) {
		return SnapshotResponse{Term: n.d.Term}
	}
	out.Term = n.d.Term
	n.resetDeadline()
	if r.Index <= n.d.Commit {
		out.Success = true
		return out
	}
	if r.LastTerm > r.Term {
		return out
	}
	m := statemachine.New()
	if m.Restore(r.Data) != nil {
		return out
	}
	var suffix []Entry
	if r.Index <= n.last() && r.Index >= n.d.BaseIndex && n.term(r.Index) == r.LastTerm {
		suffix = append(suffix, n.d.Log[r.Index-n.d.BaseIndex:]...)
	}
	n.d.BaseIndex = r.Index
	n.d.BaseTerm = r.LastTerm
	n.d.Snapshot = append([]byte(nil), r.Data...)
	n.d.Log = suffix
	n.d.Commit = r.Index
	if !n.persist(true) {
		return out
	}
	n.m = m
	n.applied = r.Index
	out.Success = true
	return out
}

// Entered with the lock held and returns with it released, like replicate's I/O
// path. The response is fenced by the sending term before touching nextIndex.
func (n *Node) replicateSnapshot(peer string, term uint64) {
	r := SnapshotRequest{Term: term, Leader: n.cfg.ID, Index: n.d.BaseIndex, LastTerm: n.d.BaseTerm, Data: append([]byte(nil), n.d.Snapshot...)}
	n.mu.Unlock()
	ctx, cancel := context.WithTimeout(n.ctx, n.cfg.RPCTimeout)
	defer cancel()
	out, e := n.transport.Snapshot(ctx, n.cfg.ID, peer, r)
	n.mu.Lock()
	defer n.mu.Unlock()
	if n.stopped || e != nil {
		return
	}
	if out.Term > n.d.Term {
		n.stepDown(out.Term, "")
		return
	}
	if n.role != "leader" || n.d.Term != term || out.Term != term {
		return
	}
	if out.Success {
		if r.Index > n.match[peer] {
			n.match[peer] = r.Index
		}
		n.next[peer] = n.match[peer] + 1
		n.advanceCommit()
		select {
		case n.wake[peer] <- struct{}{}:
		default:
		}
	}
}
