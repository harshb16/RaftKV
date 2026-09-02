package tests

import (
	"context"
	"fmt"
	"raftkv/internal/harness"
	"raftkv/internal/statemachine"
	"raftkv/internal/transport"
	"testing"
	"time"
)

func setup(t *testing.T, size int, threshold uint64) *harness.Cluster {
	t.Helper()
	c, e := harness.New(t.TempDir(), size, threshold, 91)
	if e != nil {
		t.Fatal(e)
	}
	t.Cleanup(c.Close)
	return c
}
func timeout(t *testing.T, d time.Duration) context.Context {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), d)
	t.Cleanup(cancel)
	return ctx
}
func submit(t *testing.T, c *harness.Cluster, args ...string) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	if _, e := c.Execute(ctx, statemachine.Command{Args: args}); e != nil {
		t.Fatal(e)
	}
}
func TestFiveNodeMinorityAndHeal(t *testing.T) {
	c := setup(t, 5, 8)
	old, e := c.WaitLeader(timeout(t, 3*time.Second), "")
	if e != nil {
		t.Fatal(e)
	}
	id := old.Status().ID
	var mate string
	for _, p := range c.IDs {
		if p != id {
			mate = p
			break
		}
	}
	submit(t, c, "SET", "x", "before")
	for _, a := range []string{id, mate} {
		for _, b := range c.IDs {
			if b != id && b != mate {
				c.Network.Set(a, b, transport.Rule{Blocked: true})
				c.Network.Set(b, a, transport.Rule{Blocked: true})
			}
		}
	}
	next, e := c.WaitLeader(timeout(t, 3*time.Second), id)
	if e != nil {
		t.Fatal(e)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 120*time.Millisecond)
	defer cancel()
	if _, e = old.Execute(ctx, statemachine.Command{Args: []string{"SET", "x", "orphan"}}); e == nil {
		t.Fatal("minority commit")
	}
	ctx2, c2 := context.WithTimeout(context.Background(), 2*time.Second)
	defer c2()
	if _, e = next.Execute(ctx2, statemachine.Command{Args: []string{"SET", "x", "after"}}); e != nil {
		t.Fatal(e)
	}
	c.Network.Heal()
	if e = c.Converge(timeout(t, 4*time.Second)); e != nil {
		t.Fatal(e)
	}
	for _, n := range c.Nodes() {
		if n.State()["x"] != "after" {
			t.Fatal(n.State())
		}
	}
}
func TestCrashReelectionAndRestart(t *testing.T) {
	c := setup(t, 3, 6)
	l, e := c.WaitLeader(timeout(t, 3*time.Second), "")
	if e != nil {
		t.Fatal(e)
	}
	submit(t, c, "SET", "x", "durable")
	id := l.Status().ID
	c.Crash(id)
	if _, e = c.WaitLeader(timeout(t, 3*time.Second), id); e != nil {
		t.Fatal(e)
	}
	for i := 0; i < 20; i++ {
		submit(t, c, "SET", fmt.Sprint(i), fmt.Sprint(i))
	}
	if e = c.Restart(id); e != nil {
		t.Fatal(e)
	}
	if e = c.Converge(timeout(t, 4*time.Second)); e != nil {
		t.Fatal(e)
	}
	if c.Nodes()[id].Status().SnapshotIndex == 0 {
		t.Fatal("lagging node did not install snapshot")
	}
}
func TestLossDuplicationDelayAndAsymmetry(t *testing.T) {
	c := setup(t, 3, 10)
	l, e := c.WaitLeader(timeout(t, 3*time.Second), "")
	if e != nil {
		t.Fatal(e)
	}
	id := l.Status().ID
	var follower string
	for _, p := range c.IDs {
		if p != id {
			follower = p
			break
		}
	}
	c.Network.Set(follower, id, transport.Rule{Blocked: true})
	for _, p := range c.IDs {
		if p != id {
			c.Network.Set(id, p, transport.Rule{DropEvery: 5, DropResponseEvery: 7, DuplicateEvery: 3, MaxDelay: 20 * time.Millisecond})
		}
	}
	for i := 0; i < 30; i++ {
		submit(t, c, "INCR", "x")
	}
	c.Network.Heal()
	if e = c.Converge(timeout(t, 4*time.Second)); e != nil {
		t.Fatal(e)
	}
	for _, n := range c.Nodes() {
		if n.State()["x"] != "30" {
			t.Fatal(n.State())
		}
	}
}
func TestEntireClusterRestart(t *testing.T) {
	c := setup(t, 3, 5)
	_, e := c.WaitLeader(timeout(t, 3*time.Second), "")
	if e != nil {
		t.Fatal(e)
	}
	for i := 0; i < 13; i++ {
		submit(t, c, "INCR", "x")
	}
	if e = c.Converge(timeout(t, 3*time.Second)); e != nil {
		t.Fatal(e)
	}
	for _, id := range c.IDs {
		c.Crash(id)
	}
	for _, id := range c.IDs {
		if e = c.Restart(id); e != nil {
			t.Fatal(e)
		}
	}
	_, e = c.WaitLeader(timeout(t, 3*time.Second), "")
	if e != nil {
		t.Fatal(e)
	}
	submit(t, c, "INCR", "x")
	if e = c.Converge(timeout(t, 3*time.Second)); e != nil {
		t.Fatal(e)
	}
	for _, n := range c.Nodes() {
		if n.State()["x"] != "14" {
			t.Fatal(n.State())
		}
	}
}
