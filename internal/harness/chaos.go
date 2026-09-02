package harness

import (
	"context"
	"fmt"
	"github.com/anishathalye/porcupine"
	"math/rand"
	"raftkv/internal/history"
	"raftkv/internal/statemachine"
	"raftkv/internal/transport"
	"sync"
	"time"
)

type ChaosReport struct {
	Seed            int64  `json:"seed"`
	Operations      int    `json:"operations"`
	Completed       int    `json:"completed"`
	Unknown         int    `json:"unknown"`
	Rejected        int    `json:"rejected"`
	Events          int    `json:"events"`
	Linearizability string `json:"linearizability"`
	Converged       bool   `json:"converged"`
}

func Chaos(c *Cluster, duration time.Duration, clients int, seed int64, log func(string)) (*history.Recorder, ChaosReport, error) {
	report := ChaosReport{Seed: seed}
	rec := history.New()
	initial, cancel := context.WithTimeout(context.Background(), 4*time.Second)
	_, e := c.WaitLeader(initial, "")
	cancel()
	if e != nil {
		return rec, report, e
	}
	ctx, stop := context.WithTimeout(context.Background(), duration)
	defer stop()
	var wg sync.WaitGroup
	for client := 0; client < clients; client++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			rng := rand.New(rand.NewSource(seed + int64(id)*733))
			for ctx.Err() == nil {
				key := fmt.Sprintf("key%d", rng.Intn(4))
				args := []string{}
				switch rng.Intn(4) {
				case 0:
					args = []string{"GET", key}
				case 1:
					args = []string{"SET", key, fmt.Sprint(rng.Intn(100))}
				case 2:
					args = []string{"DEL", key}
				case 3:
					args = []string{"INCR", key}
				}
				cmd := statemachine.Command{Args: args}
				call := rec.Now()
				opctx, cancel := context.WithTimeout(context.Background(), 350*time.Millisecond)
				result, e := c.Execute(opctx, cmd)
				cancel()
				rec.Add(id, cmd, call, result, e)
				select {
				case <-ctx.Done():
					return
				case <-time.After(5 * time.Millisecond):
				}
			}
		}(client)
	}
	rng := rand.New(rand.NewSource(seed))
	tick := time.NewTicker(300 * time.Millisecond)
	defer tick.Stop()
	dead := ""
	lastLeader := ""
	for ctx.Err() == nil {
		select {
		case <-ctx.Done():
		case <-tick.C:
			c.Network.Heal()
			if dead != "" {
				if e = c.Restart(dead); e != nil {
					stop()
					wg.Wait()
					return rec, report, e
				}
				log("restart " + dead)
				report.Events++
				dead = ""
			}
			if l := c.Leader(""); l != nil {
				s := l.Status()
				leader := fmt.Sprintf("%s term=%d", s.ID, s.Term)
				if s.Role == "leader" && leader != lastLeader {
					log("leader=" + leader)
					lastLeader = leader
				}
			}
			id := c.IDs[rng.Intn(len(c.IDs))]
			switch rng.Intn(5) {
			case 0:
				if l := c.Leader(""); l != nil {
					id = l.Status().ID
				}
				c.Crash(id)
				dead = id
				log("crash " + id)
			case 1:
				c.Network.Isolate(id, c.IDs)
				log("isolate " + id)
			case 2:
				for _, a := range c.IDs {
					for _, b := range c.IDs {
						if a != b {
							c.Network.Set(a, b, transport.Rule{DropEvery: 9, DuplicateEvery: 4, DropResponseEvery: 11, MaxDelay: 35 * time.Millisecond})
						}
					}
				}
				log("delay/drop/duplicate messages")
			case 3:
				for _, p := range c.IDs {
					if p != id {
						c.Network.Set(id, p, transport.Rule{Blocked: true})
					}
				}
				log("asymmetric isolation " + id)
			case 4:
				log("heal all links")
			}
			report.Events++
		}
	}
	wg.Wait()
	c.Network.Heal()
	if dead != "" {
		if e = c.Restart(dead); e != nil {
			return rec, report, e
		}
	}
	settle, cancel := context.WithTimeout(context.Background(), 6*time.Second)
	defer cancel()
	if _, e = c.WaitLeader(settle, ""); e != nil {
		return rec, report, e
	}
	if e = c.Converge(settle); e != nil {
		return rec, report, e
	}
	// Final observations constrain any pending writes before ending the history.
	for i := 0; i < 4; i++ {
		cmd := statemachine.Command{Args: []string{"GET", fmt.Sprintf("key%d", i)}}
		call := rec.Now()
		result, e := c.Execute(settle, cmd)
		rec.Add(clients, cmd, call, result, e)
		if e != nil {
			return rec, report, e
		}
	}
	if e = c.Converge(settle); e != nil {
		return rec, report, e
	}
	report.Converged = true
	rs := rec.Records()
	report.Operations = len(rs)
	for _, r := range rs {
		if r.Output.Unknown {
			report.Unknown++
		} else if r.Output.Rejected {
			report.Rejected++
		} else {
			report.Completed++
		}
	}
	result, e := history.Check(rs, 10*time.Second)
	report.Linearizability = string(result)
	if e != nil {
		return rec, report, e
	}
	if result != porcupine.Ok {
		return rec, report, fmt.Errorf("linearizability check: %s", result)
	}
	return rec, report, nil
}
