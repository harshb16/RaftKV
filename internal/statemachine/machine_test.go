package statemachine

import (
	"encoding/json"
	"testing"
)

func TestCommands(t *testing.T) {
	m := New()
	cases := []struct {
		a []string
		r Result
	}{
		{[]string{"GET", "x"}, Result{Kind: "null"}}, {[]string{"INCR", "x"}, Int(1)}, {[]string{"MSET", "a", "hello", "b", "\x00\r\n"}, Result{Kind: "simple", Text: "OK"}}, {[]string{"MGET", "a", "missing", "b"}, Result{Kind: "array", Values: []Result{Bulk("hello"), {Kind: "null"}, Bulk("\x00\r\n")}}}, {[]string{"DEL", "a", "a"}, Int(1)}, {[]string{"EXISTS", "b", "b"}, Int(2)}, {[]string{"SET", "x", "9223372036854775807"}, Result{Kind: "simple", Text: "OK"}}, {[]string{"INCR", "x"}, Error("ERR increment would overflow")}}
	for _, c := range cases {
		got := m.Apply(Command{c.a})
		if fmtResult(got) != fmtResult(c.r) {
			t.Fatalf("%v: got %+v want %+v", c.a, got, c.r)
		}
	}
	b := m.Snapshot()
	n := New()
	if err := n.Restore(b); err != nil || string(n.Snapshot()) != string(b) {
		t.Fatal("snapshot round trip")
	}
}
func fmtResult(r Result) string { b, _ := json.Marshal(r); return string(b) }
