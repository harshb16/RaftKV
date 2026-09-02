// Package statemachine implements the deterministic Redis string command subset.
package statemachine

import (
	"encoding/json"
	"fmt"
	"sort"
	"strconv"
	"strings"
)

type Command struct {
	Args []string `json:"args"`
}
type Result struct {
	Kind    string   `json:"kind"`
	Text    string   `json:"text,omitempty"`
	Integer int64    `json:"integer,omitempty"`
	Values  []Result `json:"values,omitempty"`
}

func Error(s string) Result { return Result{Kind: "error", Text: s} }
func Bulk(s string) Result  { return Result{Kind: "bulk", Text: s} }
func Int(n int64) Result    { return Result{Kind: "integer", Integer: n} }

type Machine struct{ Data map[string]string }

func New() *Machine { return &Machine{Data: map[string]string{}} }
func Validate(c Command) error {
	a := c.Args
	if len(a) == 0 {
		return fmt.Errorf("ERR empty command")
	}
	n := len(a)
	ok := false
	switch strings.ToUpper(a[0]) {
	case "GET", "INCR":
		ok = n == 2
	case "SET":
		ok = n == 3
	case "DEL", "EXISTS", "MGET":
		ok = n >= 2
	case "MSET":
		ok = n >= 3 && n%2 == 1
	case "DBSIZE":
		ok = n == 1
	default:
		return fmt.Errorf("ERR unknown command '%s'", a[0])
	}
	if !ok {
		return fmt.Errorf("ERR wrong number of arguments for '%s' command", a[0])
	}
	return nil
}

// Apply has no clock, randomness, I/O, or external effects. The Raft lock owns it.
func (m *Machine) Apply(c Command) Result {
	if err := Validate(c); err != nil {
		return Error(err.Error())
	}
	a := c.Args
	switch strings.ToUpper(a[0]) {
	case "SET":
		m.Data[a[1]] = a[2]
		return Result{Kind: "simple", Text: "OK"}
	case "MSET":
		for i := 1; i < len(a); i += 2 {
			m.Data[a[i]] = a[i+1]
		}
		return Result{Kind: "simple", Text: "OK"}
	case "GET":
		if v, ok := m.Data[a[1]]; ok {
			return Bulk(v)
		}
		return Result{Kind: "null"}
	case "MGET":
		r := Result{Kind: "array"}
		for _, k := range a[1:] {
			if v, ok := m.Data[k]; ok {
				r.Values = append(r.Values, Bulk(v))
			} else {
				r.Values = append(r.Values, Result{Kind: "null"})
			}
		}
		return r
	case "DEL":
		var n int64
		for _, k := range a[1:] {
			if _, ok := m.Data[k]; ok {
				delete(m.Data, k)
				n++
			}
		}
		return Int(n)
	case "EXISTS":
		var n int64
		for _, k := range a[1:] {
			if _, ok := m.Data[k]; ok {
				n++
			}
		}
		return Int(n)
	case "INCR":
		var v int64
		var err error
		if s, ok := m.Data[a[1]]; ok {
			v, err = strconv.ParseInt(s, 10, 64)
			if err != nil || strconv.FormatInt(v, 10) != s {
				return Error("ERR value is not an integer or out of range")
			}
		}
		if v == int64(9223372036854775807) {
			return Error("ERR increment would overflow")
		}
		v++
		m.Data[a[1]] = strconv.FormatInt(v, 10)
		return Int(v)
	case "DBSIZE":
		return Int(int64(len(m.Data)))
	}
	panic("validated command missing implementation")
}

// JSON strings cannot preserve invalid UTF-8; byte slices use base64 on disk/RPC.
func (c Command) MarshalJSON() ([]byte, error) {
	a := make([][]byte, len(c.Args))
	for i, s := range c.Args {
		a[i] = []byte(s)
	}
	return json.Marshal(a)
}
func (c *Command) UnmarshalJSON(b []byte) error {
	var a [][]byte
	if e := json.Unmarshal(b, &a); e != nil {
		return e
	}
	c.Args = make([]string, len(a))
	for i, b := range a {
		c.Args[i] = string(b)
	}
	return nil
}
func (m *Machine) Snapshot() []byte {
	keys := make([]string, 0, len(m.Data))
	for k := range m.Data {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	pairs := make([][2][]byte, 0, len(keys))
	for _, k := range keys {
		pairs = append(pairs, [2][]byte{[]byte(k), []byte(m.Data[k])})
	}
	b, e := json.Marshal(pairs)
	if e != nil {
		panic(e)
	}
	return b
}
func (m *Machine) Restore(b []byte) error {
	d := map[string]string{}
	if len(b) > 0 {
		var pairs [][2][]byte
		if e := json.Unmarshal(b, &pairs); e != nil {
			return e
		}
		if pairs == nil {
			return fmt.Errorf("null snapshot")
		}
		for _, p := range pairs {
			d[string(p[0])] = string(p[1])
		}
	}
	m.Data = d
	return nil
}
