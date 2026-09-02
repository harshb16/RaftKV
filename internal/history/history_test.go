package history

import (
	"github.com/anishathalye/porcupine"
	"raftkv/internal/statemachine"
	"testing"
	"time"
)

func record(args []string, call, end int64, result statemachine.Result) Record {
	return Record{Command: statemachine.Command{Args: args}, Call: call, Return: end, Output: Output{Result: result}}
}
func TestCheckerRejectsStaleRead(t *testing.T) {
	rs := []Record{record([]string{"SET", "x", "1"}, 1, 2, statemachine.Result{Kind: "simple", Text: "OK"}), record([]string{"GET", "x"}, 3, 4, statemachine.Result{Kind: "null"})}
	result, e := Check(rs, time.Second)
	if e != nil || result != porcupine.Illegal {
		t.Fatal(result, e)
	}
}
func TestCheckerUnknownOutcome(t *testing.T) {
	for _, result := range []statemachine.Result{{Kind: "null"}, statemachine.Bulk("1")} {
		rs := []Record{record([]string{"SET", "x", "1"}, 1, 10, statemachine.Result{}), record([]string{"GET", "x"}, 3, 4, result)}
		rs[0].Output.Unknown = true
		r, e := Check(rs, time.Second)
		if e != nil || r != porcupine.Ok {
			t.Fatal(r, e)
		}
	}
}
func TestCheckerDetectsDuplicateIncrement(t *testing.T) {
	rs := []Record{record([]string{"INCR", "x"}, 1, 2, statemachine.Int(1)), record([]string{"GET", "x"}, 3, 4, statemachine.Bulk("2"))}
	r, e := Check(rs, time.Second)
	if e != nil || r != porcupine.Illegal {
		t.Fatal(r, e)
	}
}
