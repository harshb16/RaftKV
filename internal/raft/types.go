package raft

import (
	"context"
	"encoding/json"
	"log/slog"
	"raftkv/internal/statemachine"
	"sync"
	"time"
)

type Entry struct {
	Index   uint64               `json:"index"`
	Term    uint64               `json:"term"`
	Command statemachine.Command `json:"command"`
}
type Durable struct {
	Term      uint64
	Vote      string
	BaseIndex uint64
	BaseTerm  uint64
	Snapshot  []byte
	Log       []Entry
	Commit    uint64
}
type Store interface {
	Load() ([]byte, error)
	Save([]byte) error
	Compact([]byte) error
	Close() error
}
type MemoryStore struct {
	mu sync.Mutex
	b  []byte
}

func (m *MemoryStore) Load() ([]byte, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	return append([]byte(nil), m.b...), nil
}
func (m *MemoryStore) Save(b []byte) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.b = append([]byte(nil), b...)
	return nil
}
func (m *MemoryStore) Compact(b []byte) error { return m.Save(b) }
func (m *MemoryStore) Close() error           { return nil }

type VoteRequest struct {
	Term      uint64
	Candidate string
	LastIndex uint64
	LastTerm  uint64
}
type VoteResponse struct {
	Term    uint64
	Granted bool
}
type AppendRequest struct {
	Term      uint64
	Leader    string
	PrevIndex uint64
	PrevTerm  uint64
	Entries   []Entry
	Commit    uint64
}
type AppendResponse struct {
	Term    uint64
	Success bool
	Match   uint64
	Next    uint64
}
type SnapshotRequest struct {
	Term     uint64
	Leader   string
	Index    uint64
	LastTerm uint64
	Data     []byte
}
type SnapshotResponse struct {
	Term    uint64
	Success bool
}
type Handler interface {
	RequestVote(VoteRequest) VoteResponse
	AppendEntries(AppendRequest) AppendResponse
	InstallSnapshot(SnapshotRequest) SnapshotResponse
}
type Transport interface {
	Vote(context.Context, string, string, VoteRequest) (VoteResponse, error)
	Append(context.Context, string, string, AppendRequest) (AppendResponse, error)
	Snapshot(context.Context, string, string, SnapshotRequest) (SnapshotResponse, error)
}
type Config struct {
	Logger            *slog.Logger
	ID                string
	Peers             []string
	ElectionMin       time.Duration
	ElectionMax       time.Duration
	Heartbeat         time.Duration
	RPCTimeout        time.Duration
	SnapshotThreshold uint64
	Seed              int64
}
type Status struct {
	StateHash     string            `json:"state_hash"`
	ID            string            `json:"id"`
	Role          string            `json:"role"`
	Term          uint64            `json:"term"`
	Leader        string            `json:"leader"`
	Commit        uint64            `json:"commit"`
	Applied       uint64            `json:"applied"`
	LogLength     int               `json:"log_length"`
	LastIndex     uint64            `json:"last_index"`
	SnapshotIndex uint64            `json:"snapshot_index"`
	ElectionCount uint64            `json:"election_count"`
	RPCCount      uint64            `json:"rpc_count"`
	Lag           map[string]uint64 `json:"lag"`
	Error         string            `json:"error,omitempty"`
}

func encode(d Durable) []byte {
	b, e := json.Marshal(d)
	if e != nil {
		panic(e)
	}
	return b
}
