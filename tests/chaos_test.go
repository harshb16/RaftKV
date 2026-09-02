package tests

import (
	"fmt"
	"raftkv/internal/harness"
	"testing"
	"time"
)

func TestChaosLinearizability(t *testing.T) {
	for _, size := range []int{3, 5} {
		t.Run(fmt.Sprint(size), func(t *testing.T) {
			c := setup(t, size, 12)
			_, report, e := harness.Chaos(c, 1800*time.Millisecond, 4, 2026, func(s string) { t.Log(s) })
			if e != nil {
				t.Fatal(report, e)
			}
			if report.Completed < 20 || !report.Converged || report.Events < 3 {
				t.Fatal(report)
			}
			t.Logf("%+v", report)
		})
	}
}
