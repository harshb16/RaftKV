package server

import (
	"bufio"
	"context"
	"io"
	"net"
	"raftkv/internal/metrics"
	"raftkv/internal/resp"
	"raftkv/internal/statemachine"
	"strings"
	"sync"
	"time"
)

type Executor interface {
	Execute(context.Context, statemachine.Command) (statemachine.Result, error)
}
type Server struct {
	Stats  *metrics.Registry
	exec   Executor
	mu     sync.Mutex
	conns  map[net.Conn]bool
	closed bool
	wg     sync.WaitGroup
}

func New(e Executor) *Server { return &Server{exec: e, conns: map[net.Conn]bool{}} }
func (s *Server) Serve(l net.Listener) error {
	for {
		c, e := l.Accept()
		if e != nil {
			return e
		}
		s.mu.Lock()
		if s.closed {
			s.mu.Unlock()
			_ = c.Close()
			continue
		}
		s.conns[c] = true
		s.wg.Add(1)
		s.mu.Unlock()
		go s.serve(c)
	}
}
func (s *Server) Close() {
	s.mu.Lock()
	s.closed = true
	for c := range s.conns {
		_ = c.Close()
	}
	s.mu.Unlock()
	s.wg.Wait()
}
func (s *Server) serve(c net.Conn) {
	defer s.wg.Done()
	defer func() { _ = c.Close(); s.mu.Lock(); delete(s.conns, c); s.mu.Unlock() }()
	r := bufio.NewReader(c)
	w := bufio.NewWriter(c)
	for {
		_ = c.SetReadDeadline(time.Now().Add(5 * time.Minute))
		cmd, e := resp.ReadCommand(r)
		if e != nil {
			if e != io.EOF {
				_ = resp.Write(w, statemachine.Error("ERR protocol error: "+e.Error()))
				_ = w.Flush()
			}
			return
		}
		started := time.Now()
		result := statemachine.Result{}
		a := cmd.Args
		if len(a) > 0 && strings.EqualFold(a[0], "PING") {
			if len(a) == 1 {
				result = statemachine.Result{Kind: "simple", Text: "PONG"}
			} else if len(a) == 2 {
				result = statemachine.Bulk(a[1])
			} else {
				result = statemachine.Error("ERR wrong number of arguments for PING")
			}
		} else if len(a) > 0 && strings.EqualFold(a[0], "ECHO") {
			if len(a) == 2 {
				result = statemachine.Bulk(a[1])
			} else {
				result = statemachine.Error("ERR wrong number of arguments for ECHO")
			}
		} else {
			ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
			result, e = s.exec.Execute(ctx, cmd)
			cancel()
			if e != nil {
				result = statemachine.Error(e.Error())
			}
		}
		if s.Stats != nil {
			s.Stats.Observe(time.Since(started), result.Kind == "error")
		}
		_ = c.SetWriteDeadline(time.Now().Add(5 * time.Second))
		if resp.Write(w, result) != nil || w.Flush() != nil {
			return
		}
	}
}
