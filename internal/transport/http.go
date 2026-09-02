package transport

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"raftkv/internal/raft"
	"time"
)

const MaxRPC = 256 << 20

type HTTP struct {
	Peers  map[string]string
	Client *http.Client
}

func NewHTTP(peers map[string]string) *HTTP {
	return &HTTP{Peers: peers, Client: &http.Client{Timeout: 2 * time.Second, Transport: &http.Transport{MaxIdleConnsPerHost: 4, IdleConnTimeout: 30 * time.Second}}}
}
func (t *HTTP) call(ctx context.Context, to, path string, in, out any) error {
	addr, ok := t.Peers[to]
	if !ok {
		return fmt.Errorf("unknown peer %q", to)
	}
	b, e := json.Marshal(in)
	if e != nil {
		return e
	}
	req, e := http.NewRequestWithContext(ctx, "POST", "http://"+addr+path, bytes.NewReader(b))
	if e != nil {
		return e
	}
	req.Header.Set("Content-Type", "application/json")
	r, e := t.Client.Do(req)
	if e != nil {
		return e
	}
	defer r.Body.Close()
	if r.StatusCode != 200 {
		return fmt.Errorf("RPC status %s", r.Status)
	}
	return json.NewDecoder(io.LimitReader(r.Body, MaxRPC)).Decode(out)
}
func (t *HTTP) Vote(ctx context.Context, from, to string, r raft.VoteRequest) (out raft.VoteResponse, e error) {
	e = t.call(ctx, to, "/raft/vote", r, &out)
	return
}
func (t *HTTP) Append(ctx context.Context, from, to string, r raft.AppendRequest) (out raft.AppendResponse, e error) {
	e = t.call(ctx, to, "/raft/append", r, &out)
	return
}
func (t *HTTP) Snapshot(ctx context.Context, from, to string, r raft.SnapshotRequest) (out raft.SnapshotResponse, e error) {
	e = t.call(ctx, to, "/raft/snapshot", r, &out)
	return
}
func Register(mux *http.ServeMux, h raft.Handler) {
	mux.HandleFunc("/raft/vote", rpc(h.RequestVote))
	mux.HandleFunc("/raft/append", rpc(h.AppendEntries))
	mux.HandleFunc("/raft/snapshot", rpc(h.InstallSnapshot))
}
func rpc[A, B any](f func(A) B) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if r.Method != "POST" {
			http.Error(w, "POST required", 405)
			return
		}
		var a A
		d := json.NewDecoder(http.MaxBytesReader(w, r.Body, MaxRPC))
		if e := d.Decode(&a); e != nil {
			http.Error(w, "invalid RPC", 400)
			return
		}
		var extra any
		if d.Decode(&extra) != io.EOF {
			http.Error(w, "trailing data", 400)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(f(a))
	}
}
