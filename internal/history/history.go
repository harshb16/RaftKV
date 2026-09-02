// Package history records operation intervals and checks the Redis string subset
// with an independent sequential specification and Porcupine.
package history

import (
	"encoding/json"
	"fmt"
	"github.com/anishathalye/porcupine"
	"os"
	"raftkv/internal/statemachine"
	"reflect"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"
)

type Output struct {
	Result   statemachine.Result `json:"result"`
	Unknown  bool                `json:"unknown,omitempty"`
	Rejected bool                `json:"rejected,omitempty"`
	Error    string              `json:"error,omitempty"`
}
type Record struct {
	Client  int                  `json:"client"`
	Command statemachine.Command `json:"command"`
	Call    int64                `json:"call_ns"`
	Return  int64                `json:"return_ns"`
	Output  Output               `json:"output"`
}
type Recorder struct {
	mu      sync.Mutex
	start   time.Time
	records []Record
}

func New() *Recorder           { return &Recorder{start: time.Now()} }
func (r *Recorder) Now() int64 { return time.Since(r.start).Nanoseconds() }
func (r *Recorder) Add(client int, c statemachine.Command, call int64, result statemachine.Result, err error) {
	o := Output{Result: result}
	if err != nil {
		o.Error = err.Error()
		o.Rejected = strings.HasPrefix(o.Error, "MOVED") || strings.HasPrefix(o.Error, "CLUSTERDOWN node stopped")
		o.Unknown = !o.Rejected
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	c.Args = append([]string(nil), c.Args...)
	r.records = append(r.records, Record{client, c, call, r.Now(), o})
}

// Unknown operations are pending through the end of observation: a timeout
// does not mean the Raft entry failed, nor constrain its eventual commit time.
func (r *Recorder) Records() []Record {
	r.mu.Lock()
	defer r.mu.Unlock()
	end := r.Now() + 1
	out := append([]Record(nil), r.records...)
	for i := range out {
		if out[i].Output.Unknown {
			out[i].Return = end
		}
	}
	return out
}
func (r *Recorder) Write(path string) error {
	b, e := json.MarshalIndent(r.Records(), "", "  ")
	if e != nil {
		return e
	}
	return os.WriteFile(path, append(b, '\n'), 0600)
}

type value struct {
	Present bool
	Text    string
}

func transition(v value, c statemachine.Command) (value, statemachine.Result) {
	a := c.Args
	switch strings.ToUpper(a[0]) {
	case "GET":
		if v.Present {
			return v, statemachine.Bulk(v.Text)
		}
		return v, statemachine.Result{Kind: "null"}
	case "SET":
		return value{true, a[2]}, statemachine.Result{Kind: "simple", Text: "OK"}
	case "DEL":
		if v.Present {
			return value{}, statemachine.Int(1)
		}
		return v, statemachine.Int(0)
	case "INCR":
		var i int64
		var e error
		if v.Present {
			i, e = strconv.ParseInt(v.Text, 10, 64)
			if e != nil || strconv.FormatInt(i, 10) != v.Text {
				return v, statemachine.Error("ERR value is not an integer or out of range")
			}
		}
		if i == int64(9223372036854775807) {
			return v, statemachine.Error("ERR increment would overflow")
		}
		i++
		return value{true, strconv.FormatInt(i, 10)}, statemachine.Int(i)
	}
	panic("unsupported history command")
}
func model() porcupine.Model {
	return porcupine.Model{
		Partition: func(ops []porcupine.Operation) [][]porcupine.Operation {
			parts := map[string][]porcupine.Operation{}
			for _, o := range ops {
				k := o.Input.(statemachine.Command).Args[1]
				parts[k] = append(parts[k], o)
			}
			keys := []string{}
			for k := range parts {
				keys = append(keys, k)
			}
			sort.Strings(keys)
			out := [][]porcupine.Operation{}
			for _, k := range keys {
				out = append(out, parts[k])
			}
			return out
		},
		Init: func() any { return []value{{}} }, Equal: func(a, b any) bool { return reflect.DeepEqual(a, b) },
		Step: func(state, input, output any) (bool, any) {
			states := state.([]value)
			c := input.(statemachine.Command)
			o := output.(Output)
			if o.Rejected {
				return true, states
			}
			possible := map[value]bool{}
			for _, v := range states {
				next, result := transition(v, c)
				if o.Unknown {
					possible[v] = true
					possible[next] = true
				} else if reflect.DeepEqual(result, o.Result) {
					possible[next] = true
				}
			}
			out := make([]value, 0, len(possible))
			for v := range possible {
				out = append(out, v)
			}
			sort.Slice(out, func(i, j int) bool {
				if out[i].Present != out[j].Present {
					return !out[i].Present
				}
				return out[i].Text < out[j].Text
			})
			return len(out) > 0, out
		},
		DescribeOperation: func(in, out any) string { return fmt.Sprintf("%v => %+v", in.(statemachine.Command).Args, out) },
	}
}
func Check(records []Record, timeout time.Duration) (porcupine.CheckResult, error) {
	ops := make([]porcupine.Operation, 0, len(records))
	for _, r := range records {
		a := r.Command.Args
		if len(a) < 2 {
			return porcupine.Illegal, fmt.Errorf("history requires single-key GET/SET/DEL/INCR")
		}
		op := strings.ToUpper(a[0])
		if op != "GET" && op != "SET" && op != "DEL" && op != "INCR" || op == "SET" && len(a) != 3 || op != "SET" && len(a) != 2 {
			return porcupine.Illegal, fmt.Errorf("unsupported checker command %v", a)
		}
		ops = append(ops, porcupine.Operation{ClientId: r.Client, Input: r.Command, Call: r.Call, Output: r.Output, Return: r.Return})
	}
	result, _ := porcupine.CheckOperationsVerbose(model(), ops, timeout)
	return result, nil
}
