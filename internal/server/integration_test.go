package server

import (
	"bufio"
	"fmt"
	"net"
	"raftkv/internal/resp"
	"raftkv/internal/statemachine"
	"testing"
	"time"
)

func free(t *testing.T) string {
	t.Helper()
	l, e := net.Listen("tcp", "127.0.0.1:0")
	if e != nil {
		t.Fatal(e)
	}
	a := l.Addr().String()
	_ = l.Close()
	return a
}
func TestRealRESPCluster(t *testing.T) {
	peers := map[string]string{}
	clients := map[string]string{}
	for i := 1; i <= 3; i++ {
		id := fmt.Sprint(i)
		peers[id] = free(t)
		clients[id] = free(t)
	}
	var ns []*Runtime
	for i := 1; i <= 3; i++ {
		id := fmt.Sprint(i)
		n, e := Launch(Config{NodeID: id, ClientAddr: clients[id], RaftAddr: peers[id], Peers: peers, Clients: clients, DataDir: t.TempDir()})
		if e != nil {
			t.Fatal(e)
		}
		ns = append(ns, n)
		t.Cleanup(n.Close)
	}
	var leader *Runtime
	end := time.Now().Add(3 * time.Second)
	for time.Now().Before(end) {
		for _, n := range ns {
			if n.Node.Status().Role == "leader" {
				leader = n
				break
			}
		}
		if leader != nil {
			break
		}
		time.Sleep(10 * time.Millisecond)
	}
	if leader == nil {
		t.Fatal("no leader")
	}
	call := func(addr string, args ...string) statemachine.Result {
		t.Helper()
		c, e := net.DialTimeout("tcp", addr, time.Second)
		if e != nil {
			t.Fatal(e)
		}
		defer c.Close()
		_ = c.SetDeadline(time.Now().Add(2 * time.Second))
		if e = resp.WriteCommand(c, args...); e != nil {
			t.Fatal(e)
		}
		r, e := resp.ReadResult(bufio.NewReader(c))
		if e != nil {
			t.Fatal(e)
		}
		return r
	}
	if r := call(leader.cfg.ClientAddr, "SET", "\xffkey", "\x00\xff\r\n"); r.Kind != "simple" {
		t.Fatal(r)
	}
	if r := call(leader.cfg.ClientAddr, "GET", "\xffkey"); r.Text != "\x00\xff\r\n" {
		t.Fatal(r)
	}
	for _, n := range ns {
		if n != leader {
			r := call(n.cfg.ClientAddr, "GET", "\xffkey")
			if r.Kind != "error" {
				t.Fatal(r)
			}
		}
	}
	if r := call(leader.cfg.ClientAddr, "PING"); r.Text != "PONG" {
		t.Fatal(r)
	}
	if r := call(leader.cfg.ClientAddr, "ECHO", "\x00\xff\r\n"); r.Text != "\x00\xff\r\n" || r.Kind != "bulk" {
		t.Fatal(r)
	}
}
