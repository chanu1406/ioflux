package cpustat

import (
	"testing"
	"time"
)

func TestRusageMonotonic(t *testing.T) {
	a := Now()
	// Burn some CPU so user time advances measurably.
	deadline := time.Now().Add(20 * time.Millisecond)
	x := 1
	for time.Now().Before(deadline) {
		x = (x*1103515245 + 12345) & 0x7FFFFFFF
	}
	_ = x
	b := Now()

	if b.UserNS < a.UserNS {
		t.Errorf("UserNS went backwards: %d -> %d", a.UserNS, b.UserNS)
	}
	if b.SysNS < a.SysNS {
		t.Errorf("SysNS went backwards: %d -> %d", a.SysNS, b.SysNS)
	}
}

func TestSubDelta(t *testing.T) {
	a := Sample{UserNS: 100, SysNS: 50}
	b := Sample{UserNS: 150, SysNS: 80}
	d := b.Sub(a)
	if d.UserNS != 50 || d.SysNS != 30 {
		t.Errorf("Sub: got %+v, want {50 30}", d)
	}
}

func TestNonZeroAfterBusyLoop(t *testing.T) {
	a := Now()
	deadline := time.Now().Add(50 * time.Millisecond)
	x := 1
	for time.Now().Before(deadline) {
		x = (x*1103515245 + 12345) & 0x7FFFFFFF
	}
	_ = x
	b := Now()
	delta := b.Sub(a)
	// User time should have advanced for a CPU-bound loop.
	if delta.UserNS == 0 {
		t.Errorf("expected non-zero UserNS delta after 50ms busy loop; got %+v", delta)
	}
}
