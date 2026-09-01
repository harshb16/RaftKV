// Package resp is a bounded, binary-safe RESP2 codec.
package resp

import (
	"bufio"
	"fmt"
	"io"
	"raftkv/internal/statemachine"
	"strconv"
	"strings"
)

const MaxBulk = 8 << 20
const MaxArgs = 1024

func line(r *bufio.Reader) (string, error) {
	var b []byte
	for {
		p, more, err := r.ReadLine()
		if err != nil {
			return "", err
		}
		b = append(b, p...)
		if len(b) > 64<<10 {
			return "", fmt.Errorf("protocol line too large")
		}
		if !more {
			return string(b), nil
		}
	}
}
func ReadCommand(r *bufio.Reader) (statemachine.Command, error) {
	s, err := line(r)
	if err != nil {
		return statemachine.Command{}, err
	}
	if len(s) == 0 {
		return statemachine.Command{}, fmt.Errorf("empty command")
	}
	if s[0] != '*' {
		return statemachine.Command{Args: strings.Fields(s)}, nil
	}
	n, err := strconv.Atoi(s[1:])
	if err != nil || n <= 0 || n > MaxArgs {
		return statemachine.Command{}, fmt.Errorf("invalid array size")
	}
	a := make([]string, n)
	total := 0
	for i := range a {
		s, err = line(r)
		if err != nil {
			return statemachine.Command{}, err
		}
		if len(s) == 0 || s[0] != '$' {
			return statemachine.Command{}, fmt.Errorf("expected bulk string")
		}
		size, e := strconv.Atoi(s[1:])
		total += size
		if e != nil || size < 0 || size > MaxBulk || total > MaxBulk {
			return statemachine.Command{}, fmt.Errorf("invalid bulk size")
		}
		b := make([]byte, size+2)
		if _, err = io.ReadFull(r, b); err != nil {
			return statemachine.Command{}, err
		}
		if b[size] != '\r' || b[size+1] != '\n' {
			return statemachine.Command{}, fmt.Errorf("invalid bulk terminator")
		}
		a[i] = string(b[:size])
	}
	return statemachine.Command{Args: a}, nil
}
func Write(w io.Writer, r statemachine.Result) error {
	var err error
	switch r.Kind {
	case "simple":
		_, err = fmt.Fprintf(w, "+%s\r\n", sanitize(r.Text))
	case "error":
		_, err = fmt.Fprintf(w, "-%s\r\n", sanitize(r.Text))
	case "integer":
		_, err = fmt.Fprintf(w, ":%d\r\n", r.Integer)
	case "null":
		_, err = io.WriteString(w, "$-1\r\n")
	case "bulk":
		_, err = fmt.Fprintf(w, "$%d\r\n%s\r\n", len(r.Text), r.Text)
	case "array":
		_, err = fmt.Fprintf(w, "*%d\r\n", len(r.Values))
		if err == nil {
			for _, v := range r.Values {
				if err = Write(w, v); err != nil {
					break
				}
			}
		}
	default:
		return fmt.Errorf("unknown result kind %q", r.Kind)
	}
	return err
}
func sanitize(s string) string { return strings.NewReplacer("\r", " ", "\n", " ").Replace(s) }
func WriteCommand(w io.Writer, args ...string) error {
	v := make([]statemachine.Result, len(args))
	for i, a := range args {
		v[i] = statemachine.Bulk(a)
	}
	return Write(w, statemachine.Result{Kind: "array", Values: v})
}

// ReadResult is used by the demo/benchmark client.
func ReadResult(r *bufio.Reader) (statemachine.Result, error) {
	s, err := line(r)
	if err != nil {
		return statemachine.Result{}, err
	}
	if len(s) == 0 {
		return statemachine.Result{}, fmt.Errorf("empty response")
	}
	switch s[0] {
	case '+':
		return statemachine.Result{Kind: "simple", Text: s[1:]}, nil
	case '-':
		return statemachine.Error(s[1:]), nil
	case ':':
		n, e := strconv.ParseInt(s[1:], 10, 64)
		return statemachine.Int(n), e
	case '$':
		n, e := strconv.Atoi(s[1:])
		if e != nil || n < -1 || n > MaxBulk {
			return statemachine.Result{}, fmt.Errorf("invalid bulk")
		}
		if n == -1 {
			return statemachine.Result{Kind: "null"}, nil
		}
		b := make([]byte, n+2)
		_, e = io.ReadFull(r, b)
		if e == nil && string(b[n:]) != "\r\n" {
			e = fmt.Errorf("invalid terminator")
		}
		return statemachine.Bulk(string(b[:n])), e
	case '*':
		n, e := strconv.Atoi(s[1:])
		if e != nil || n < 0 || n > MaxArgs {
			return statemachine.Result{}, fmt.Errorf("invalid array")
		}
		v := make([]statemachine.Result, n)
		for i := range v {
			v[i], e = ReadResult(r)
			if e != nil {
				return statemachine.Result{}, e
			}
		}
		return statemachine.Result{Kind: "array", Values: v}, nil
	}
	return statemachine.Result{}, fmt.Errorf("unknown response")
}
