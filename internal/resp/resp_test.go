package resp

import (
	"bufio"
	"bytes"
	"raftkv/internal/statemachine"
	"testing"
)

func TestBinaryPipeline(t *testing.T) {
	var b bytes.Buffer
	_ = WriteCommand(&b, "SET", "x", "\x00\r\n")
	_ = WriteCommand(&b, "GET", "x")
	r := bufio.NewReader(&b)
	a, e := ReadCommand(r)
	if e != nil || a.Args[2] != "\x00\r\n" {
		t.Fatal(a, e)
	}
	a, e = ReadCommand(r)
	if e != nil || a.Args[0] != "GET" {
		t.Fatal(a, e)
	}
}
func TestRejectMalformed(t *testing.T) {
	for _, s := range []string{"*999999\r\n", "*1\r\n$-2\r\n", "*1\r\n$1\r\nxzz"} {
		if _, e := ReadCommand(bufio.NewReader(bytes.NewBufferString(s))); e == nil {
			t.Fatal(s)
		}
	}
}
func TestResultRoundTrip(t *testing.T) {
	var b bytes.Buffer
	want := statemachine.Bulk("\x00\r\n")
	if e := Write(&b, want); e != nil {
		t.Fatal(e)
	}
	got, e := ReadResult(bufio.NewReader(&b))
	if e != nil || got.Text != want.Text {
		t.Fatal(got, e)
	}
}
