package replay_test

import (
	"testing"
	"time"

	"github.com/chanuollala/ioflux/pkg/metrics"
	"github.com/chanuollala/ioflux/pkg/replay"
	"github.com/chanuollala/ioflux/pkg/results"
	"github.com/chanuollala/ioflux/pkg/trace"
)

// recorderWith returns a recorder holding ops READ completions, each moving
// bytesPerOp bytes. Used to synthesize WorkerOutputs with known totals so the
// straggler math can be checked exactly.
func recorderWith(t *testing.T, ops int, bytesPerOp int64) *metrics.Recorder {
	t.Helper()
	r := metrics.NewRecorder()
	for i := 0; i < ops; i++ {
		r.Record(trace.OpRead, 1000, bytesPerOp, false)
	}
	return r
}

func approxEqual(a, b, eps float64) bool {
	d := a - b
	if d < 0 {
		d = -d
	}
	return d <= eps
}

// TestBuildResults_IdleWorkersExcluded pins the idle-worker fix: a worker with no
// assigned streams (LastDoneNS == 0) must not collapse the straggler window's
// first-done time to zero, which previously zeroed first-done throughput.
func TestBuildResults_IdleWorkersExcluded(t *testing.T) {
	opts := replay.SchedulerOpts{Mode: "asap", PlanInfo: results.PlanInfo{Engine: "mem", Mode: "asap"}}
	hdr := trace.Header{}
	oneSec := int64(time.Second)

	// One active worker (1 stream / 3 workers leaves 2 idle).
	active := &replay.WorkerOutput{
		Hostname:     "active",
		Recorder:     recorderWith(t, 10, 1<<20),
		ActualNumOps: 10,
		FirstDoneNS:  2e8,
		LastDoneNS:   oneSec,
	}
	idle1 := &replay.WorkerOutput{Hostname: "idle1", Recorder: metrics.NewRecorder()}
	idle2 := &replay.WorkerOutput{Hostname: "idle2", Recorder: metrics.NewRecorder()}

	res := replay.BuildResults([]*replay.WorkerOutput{active, idle1, idle2}, opts, hdr, 0)

	sw := res.Straggler
	if sw == nil {
		t.Fatal("Straggler is nil for a multi-host run")
	}
	if sw.FirstDoneNS != oneSec {
		t.Errorf("FirstDoneNS=%d, want %d (idle workers must not collapse it to 0)", sw.FirstDoneNS, oneSec)
	}
	if sw.LastDoneNS != oneSec {
		t.Errorf("LastDoneNS=%d, want %d", sw.LastDoneNS, oneSec)
	}
	if sw.SkewNS != 0 {
		t.Errorf("SkewNS=%d, want 0 (only one active worker)", sw.SkewNS)
	}
	if sw.FirstDoneOpsPerSec == 0 {
		t.Error("FirstDoneOpsPerSec=0; idle workers zeroed the first-done throughput")
	}
	if !approxEqual(sw.FirstDoneOpsPerSec, sw.LastDoneOpsPerSec, 1e-6) {
		t.Errorf("first-done %.3f != last-done %.3f ops/s for a single active worker",
			sw.FirstDoneOpsPerSec, sw.LastDoneOpsPerSec)
	}
	if !approxEqual(sw.FirstDoneOpsPerSec, 10, 1e-6) {
		t.Errorf("FirstDoneOpsPerSec=%.3f, want 10 (10 ops / 1s)", sw.FirstDoneOpsPerSec)
	}
	if len(res.Hosts) != 3 {
		t.Errorf("Hosts=%d, want 3 (idle workers still listed)", len(res.Hosts))
	}
}

// TestBuildResults_StragglerFirstDoneThroughput pins the first-done throughput
// definition: it must count only work completed by the earliest worker's finish,
// not the whole run's work divided by the shorter window.
func TestBuildResults_StragglerFirstDoneThroughput(t *testing.T) {
	opts := replay.SchedulerOpts{Mode: "asap", PlanInfo: results.PlanInfo{Engine: "mem", Mode: "asap"}}
	hdr := trace.Header{}
	oneSec := int64(time.Second)

	// Worker A finishes at 1s with 20 ops; straggler B finishes at 2s with 20 ops.
	// By 1s, A is done (20 ops) and B is ~halfway (≈10 ops): 30 ops/s first-done.
	a := &replay.WorkerOutput{Hostname: "a", Recorder: recorderWith(t, 20, 1<<20), ActualNumOps: 20, LastDoneNS: oneSec}
	b := &replay.WorkerOutput{Hostname: "b", Recorder: recorderWith(t, 20, 1<<20), ActualNumOps: 20, LastDoneNS: 2 * oneSec}

	res := replay.BuildResults([]*replay.WorkerOutput{a, b}, opts, hdr, 0)
	sw := res.Straggler
	if sw == nil {
		t.Fatal("Straggler is nil")
	}
	if sw.SkewNS != oneSec {
		t.Errorf("SkewNS=%d, want %d", sw.SkewNS, oneSec)
	}
	if !approxEqual(sw.FirstDoneOpsPerSec, 30, 1e-6) {
		t.Errorf("FirstDoneOpsPerSec=%.3f, want 30 (20 + 20·0.5)", sw.FirstDoneOpsPerSec)
	}
	if !approxEqual(sw.LastDoneOpsPerSec, 20, 1e-6) {
		t.Errorf("LastDoneOpsPerSec=%.3f, want 20 (40 ops / 2s)", sw.LastDoneOpsPerSec)
	}
	if sw.FirstDoneOpsPerSec <= sw.LastDoneOpsPerSec {
		t.Errorf("first-done %.3f should exceed last-done %.3f", sw.FirstDoneOpsPerSec, sw.LastDoneOpsPerSec)
	}
	// The old, wrong formula was totalOps/firstDone = 40. The honest number must
	// be strictly below it because not all work was done by first-done.
	naive := float64(res.OpsCompleted) // / 1s
	if sw.FirstDoneOpsPerSec >= naive {
		t.Errorf("FirstDoneOpsPerSec=%.3f must be < naive total/firstDone=%.1f (completed-by-first-done only)",
			sw.FirstDoneOpsPerSec, naive)
	}
}
