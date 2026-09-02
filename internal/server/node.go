package server

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"net"
	"net/http"
	"os"
	"raftkv/internal/raft"
	"raftkv/internal/statemachine"
	"raftkv/internal/storage"
	"raftkv/internal/transport"
	"sort"
	"strings"
	"time"
)

type Config struct {
	NodeID            string            `json:"node_id"`
	ClientAddr        string            `json:"client_addr"`
	RaftAddr          string            `json:"raft_addr"`
	DataDir           string            `json:"data_dir"`
	Peers             map[string]string `json:"peers"`
	Clients           map[string]string `json:"clients"`
	SnapshotThreshold uint64            `json:"snapshot_threshold"`
}

func ReadConfig(path string) (Config, error) {
	var c Config
	b, e := os.ReadFile(path)
	if e != nil {
		return c, e
	}
	if e = json.Unmarshal(b, &c); e != nil {
		return c, e
	}
	if c.NodeID == "" || c.ClientAddr == "" || c.RaftAddr == "" || c.DataDir == "" {
		return c, fmt.Errorf("config requires node_id, client_addr, raft_addr, data_dir")
	}
	if c.Peers[c.NodeID] != c.RaftAddr {
		return c, fmt.Errorf("self raft address must match peers")
	}
	for id := range c.Peers {
		if c.Clients[id] == "" {
			return c, fmt.Errorf("missing client address for %s", id)
		}
	}
	return c, nil
}

type Runtime struct {
	Node      *raft.Node
	cfg       Config
	wal       *storage.WAL
	http      *http.Server
	listener  net.Listener
	client    *Server
	transport *transport.HTTP
}

func Launch(c Config) (*Runtime, error) {
	wal, e := storage.Open(c.DataDir)
	if e != nil {
		return nil, e
	}
	ids := []string{}
	for id := range c.Peers {
		ids = append(ids, id)
	}
	sort.Strings(ids)
	t := transport.NewHTTP(c.Peers)
	n, e := raft.New(raft.Config{ID: c.NodeID, Peers: ids, Seed: time.Now().UnixNano(), SnapshotThreshold: c.SnapshotThreshold}, wal, t)
	if e != nil {
		_ = wal.Close()
		return nil, e
	}
	l, e := net.Listen("tcp", c.ClientAddr)
	if e != nil {
		_ = wal.Close()
		return nil, e
	}
	rl, e := net.Listen("tcp", c.RaftAddr)
	if e != nil {
		_ = l.Close()
		_ = wal.Close()
		return nil, e
	}
	rt := &Runtime{Node: n, cfg: c, wal: wal, listener: l, transport: t}
	rt.client = New(rt)
	mux := http.NewServeMux()
	transport.Register(mux, n)
	mux.HandleFunc("/status", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(n.Status())
	})
	rt.http = &http.Server{Handler: mux, ReadHeaderTimeout: 2 * time.Second, ReadTimeout: 10 * time.Second, WriteTimeout: 10 * time.Second}
	n.Start()
	go func() {
		if e := rt.http.Serve(rl); e != nil && e != http.ErrServerClosed {
			slog.Error("RPC server stopped", "error", e)
		}
	}()
	go func() {
		if e := rt.client.Serve(l); e != nil && !strings.Contains(e.Error(), "closed") {
			slog.Error("RESP server stopped", "error", e)
		}
	}()
	return rt, nil
}
func (r *Runtime) Close() {
	_ = r.listener.Close()
	r.client.Close()
	r.Node.Stop()
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	_ = r.http.Shutdown(ctx)
	r.transport.Client.CloseIdleConnections()
	_ = r.wal.Close()
}
func (r *Runtime) Execute(ctx context.Context, c statemachine.Command) (statemachine.Result, error) {
	if len(c.Args) == 0 {
		return statemachine.Result{}, fmt.Errorf("ERR empty command")
	}
	switch strings.ToUpper(c.Args[0]) {
	case "INFO":
		if len(c.Args) > 2 {
			return statemachine.Result{}, fmt.Errorf("ERR wrong number of arguments for INFO")
		}
		s := r.Node.Status()
		return statemachine.Bulk(fmt.Sprintf("# RaftKV\r\nrole:%s\r\nnode_id:%s\r\nterm:%d\r\nleader:%s\r\ncommit_index:%d\r\napplied_index:%d\r\nlog_length:%d\r\nsnapshot_index:%d\r\n", s.Role, s.ID, s.Term, s.Leader, s.Commit, s.Applied, s.LogLength, s.SnapshotIndex)), nil
	case "ROLE":
		if len(c.Args) != 1 {
			return statemachine.Result{}, fmt.Errorf("ERR wrong number of arguments for ROLE")
		}
		s := r.Node.Status()
		role := "slave"
		if s.Role == "leader" {
			role = "master"
		}
		if role == "master" {
			return statemachine.Result{Kind: "array", Values: []statemachine.Result{statemachine.Bulk(role), statemachine.Int(int64(s.Commit)), {Kind: "array"}}}, nil
		}
		return statemachine.Result{Kind: "array", Values: []statemachine.Result{statemachine.Bulk(role), statemachine.Bulk(r.cfg.Clients[s.Leader]), statemachine.Int(0), statemachine.Bulk(s.Role), statemachine.Int(int64(s.Applied))}}, nil
	}
	out, e := r.Node.Execute(ctx, c)
	if e != nil && strings.HasPrefix(e.Error(), "MOVED ") {
		s := r.Node.Status()
		if addr := r.cfg.Clients[s.Leader]; addr != "" {
			e = fmt.Errorf("MOVED %s", addr)
		} else {
			e = fmt.Errorf("CLUSTERDOWN no known leader")
		}
	}
	return out, e
}
