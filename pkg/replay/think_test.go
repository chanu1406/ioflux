package replay_test

import (
	"bytes"
	"context"
	"strings"
	"testing"
	"time"

	"github.com/chanuollala/ioflux/pkg/replay"
	"github.com/chanuollala/ioflux/pkg/trace"
)

// buildThinkTrace builds a single-stream read trace whose ops each took durNS
// at the source and arrived gapNS after the previous one finished. withDur
// controls whether the durations are recorded at all.
func buildThinkTrace(t *testing.T, nReads int, durNS, gapNS int64, withDur bool) (*bytes.Buffer, trace.Header) {
	t.Helper()
	tgt := 0
	h := int64(1)
	off := int64(0)
	readLen := int64(4096)

	hdr := trace.Header{
		Version:       trace.TraceFormatVersion,
		Kind:          trace.TraceSynthetic,
		TimeUnit:      trace.TimeUnitNanoseconds,
		CaptureMethod: trace.CaptureSynthetic,
		Targets:       []trace.TargetInfo{{ID: 0, Name: "target", Kind: trace.TargetFile, Size: 1 << 20}},
		Summary:       trace.Summary{NumOps: int64(nReads + 2), NumStreams: 1},
	}

	var buf bytes.Buffer
	tw := trace.NewWriter(&buf)
	if err := tw.WriteHeader(hdr); err != nil {
		t.Fatalf("WriteHeader: %v", err)
	}
	writeOp := func(op trace.Op) {
		if err := tw.WriteOp(op); err != nil {
			t.Fatalf("WriteOp: %v", err)
		}
	}

	dur := func(d int64) *int64 {
		if !withDur {
			return nil
		}
		return trace.Ptr(d)
	}

	opID := int64(0)
	writeOp(trace.Op{T: 0, OpID: &opID, S: 0, Op: trace.OpOpen, Tgt: &tgt, H: &h, Mode: trace.ModeRead, Dur: dur(0)})
	opID++

	// Read i finishes at i*(durNS+gapNS)+durNS and the next arrives gapNS later.
	ts := int64(0)
	for i := 0; i < nReads; i++ {
		writeOp(trace.Op{T: ts, OpID: trace.Ptr(opID), S: 0, Op: trace.OpRead, H: &h, Off: &off, Len: &readLen, Dur: dur(durNS)})
		opID++
		ts += durNS + gapNS
	}
	writeOp(trace.Op{T: ts, OpID: &opID, S: 0, Op: trace.OpClose, H: &h, Dur: dur(0)})

	return &buf, hdr
}

func runThinkTrace(t *testing.T, buf *bytes.Buffer, hdr trace.Header, mode string, readDelay time.Duration) time.Duration {
	t.Helper()
	r, err := trace.NewReader(buf)
	if err != nil {
		t.Fatalf("NewReader: %v", err)
	}
	exec, err := replay.Prepare(replay.Plan{
		Engine:      memEngineSlowRead(hdr, readDelay),
		EngineName:  "mem-slow",
		Mode:        mode,
		MaxInflight: 1000,
	}, r)
	if err != nil {
		t.Fatalf("Prepare(%s): %v", mode, err)
	}
	res, err := exec.Run(context.Background())
	if err != nil {
		t.Fatalf("Run(%s): %v", mode, err)
	}
	return time.Duration(res.DurationNS)
}

// Think mode must add the source's idle gaps back into the run. Against a
// backend 20x slower than the captured one, asap collapses to pure service time
// while think keeps each gap, so the difference between the two is the total
// think time the source spent between operations.
func TestThink_RestoresSourceGaps(t *testing.T) {
	const (
		nReads    = 5
		sourceDur = int64(1 * time.Millisecond)
		gap       = int64(10 * time.Millisecond)
		delay     = 20 * time.Millisecond
	)

	buf, hdr := buildThinkTrace(t, nReads, sourceDur, gap, true)
	thinkDur := runThinkTrace(t, buf, hdr, "think", delay)

	buf, hdr = buildThinkTrace(t, nReads, sourceDur, gap, true)
	asapDur := runThinkTrace(t, buf, hdr, "asap", delay)

	// Four gaps separate five reads; the close adds one more.
	wantExtra := time.Duration(gap * nReads)
	got := thinkDur - asapDur

	if got < wantExtra/2 {
		t.Errorf("think - asap = %v, want at least %v: the source's gaps were not replayed",
			got, wantExtra/2)
	}
	if got > wantExtra*3 {
		t.Errorf("think - asap = %v, want at most %v: more than the source's gaps was waited out",
			got, wantExtra*3)
	}
}

// Against a backend slower than the captured one, timeline falls further behind
// its fixed schedule with every op and the run is flagged as unfaithful. Think
// mode has no fixed schedule to fall behind: each arrival is measured from the
// previous completion, so a slower backend stretches the run instead. This is
// what makes the mode usable for comparing backends of different speeds.
func TestThink_SlowBackendStretchesRatherThanFallsBehind(t *testing.T) {
	const (
		nReads    = 10
		sourceDur = int64(1 * time.Millisecond)
		gap       = int64(5 * time.Millisecond)
		delay     = 10 * time.Millisecond // 10x the source's service time
	)

	run := func(mode string) (time.Duration, int64, bool, string) {
		buf, hdr := buildThinkTrace(t, nReads, sourceDur, gap, true)
		r, err := trace.NewReader(buf)
		if err != nil {
			t.Fatalf("NewReader: %v", err)
		}
		exec, err := replay.Prepare(replay.Plan{
			Engine:      memEngineSlowRead(hdr, delay),
			EngineName:  "mem-slow",
			Mode:        mode,
			MaxInflight: 1000,
		}, r)
		if err != nil {
			t.Fatalf("Prepare(%s): %v", mode, err)
		}
		res, err := exec.Run(context.Background())
		if err != nil {
			t.Fatalf("Run(%s): %v", mode, err)
		}
		return time.Duration(res.DurationNS), res.ScheduleDrift.P99NS,
			res.Fidelity.LowFidelity, res.Fidelity.LowFidelityCategory
	}

	tlDur, tlDrift, tlLow, tlCat := run("timeline")
	thDur, thDrift, thLow, _ := run("think")

	if !tlLow || tlCat != "behind_schedule" {
		t.Errorf("timeline: low=%v category=%q, want a behind_schedule flag on a backend "+
			"10x slower than the source", tlLow, tlCat)
	}
	if thLow {
		t.Errorf("think: run flagged low fidelity, but it has no fixed schedule to fall behind")
	}
	if thDrift >= tlDrift {
		t.Errorf("think drift p99 %dns >= timeline %dns, want think far lower", thDrift, tlDrift)
	}

	// Ten ops at 10ms service plus nine 5ms gaps is ~145ms; timeline just runs
	// them back to back at ~100ms, having discarded the gaps it fell behind.
	if thDur <= tlDur {
		t.Errorf("think ran in %v and timeline in %v, want think longer: the gaps are still there",
			thDur, tlDur)
	}
}

// The gap is measured from when the previous op completed, not from the
// difference between timestamps. Two traces with identical arrival times but
// different source durations therefore describe different amounts of idle time,
// and must replay at different speeds.
func TestThink_GapIsMeasuredFromCompletion(t *testing.T) {
	const (
		nReads  = 5
		spacing = int64(20 * time.Millisecond)
		delay   = 2 * time.Millisecond
	)

	// Same arrival cadence in both, split differently between work and idle.
	busyDur, busyGap := spacing-int64(2*time.Millisecond), int64(2*time.Millisecond)
	idleDur, idleGap := int64(2*time.Millisecond), spacing-int64(2*time.Millisecond)

	buf, hdr := buildThinkTrace(t, nReads, busyDur, busyGap, true)
	busy := runThinkTrace(t, buf, hdr, "think", delay)

	buf, hdr = buildThinkTrace(t, nReads, idleDur, idleGap, true)
	idle := runThinkTrace(t, buf, hdr, "think", delay)

	if idle <= busy {
		t.Errorf("mostly-idle trace replayed in %v, mostly-busy in %v: want the idle one slower, "+
			"since think time is the gap after completion rather than between arrivals", idle, busy)
	}
}

// A trace with no recorded durations cannot express think time. Replaying it in
// think mode would silently be an asap run, so it is refused instead.
func TestThink_RejectsTraceWithoutDurations(t *testing.T) {
	buf, _ := buildThinkTrace(t, 3, int64(time.Millisecond), int64(time.Millisecond), false)

	r, err := trace.NewReader(buf)
	if err != nil {
		t.Fatalf("NewReader: %v", err)
	}
	_, err = replay.Prepare(replay.Plan{
		Engine:      memEngineSlowRead(trace.Header{}, 0),
		EngineName:  "mem",
		Mode:        "think",
		MaxInflight: 16,
	}, r)
	if err == nil {
		t.Fatal("Prepare accepted a think-mode replay of a trace carrying no durations")
	}
	if !strings.Contains(err.Error(), "durations") {
		t.Errorf("error = %v, want it to name the missing durations", err)
	}
}

// Overlapping source records produce a negative gap. It clamps to zero rather
// than pulling the schedule backwards, so the run stays close to asap.
func TestThink_NegativeGapClampsToZero(t *testing.T) {
	const nReads = 5
	const delay = 2 * time.Millisecond

	// Each op is recorded as lasting far longer than the interval before the
	// next arrives, so every derived gap is negative.
	buf, hdr := buildThinkTrace(t, nReads, int64(50*time.Millisecond), -int64(40*time.Millisecond), true)
	thinkDur := runThinkTrace(t, buf, hdr, "think", delay)

	if max := 200 * time.Millisecond; thinkDur > max {
		t.Errorf("duration = %v, want under %v: a negative gap should not become a wait", thinkDur, max)
	}
}

// A stream's first op has no predecessor, so it keeps its recorded offset from
// run start rather than issuing immediately.
func TestThink_FirstOpKeepsItsOffset(t *testing.T) {
	const startOffset = 60 * time.Millisecond

	tgt := 0
	h := int64(1)
	off := int64(0)
	readLen := int64(4096)
	hdr := trace.Header{
		Version:       trace.TraceFormatVersion,
		Kind:          trace.TraceSynthetic,
		TimeUnit:      trace.TimeUnitNanoseconds,
		CaptureMethod: trace.CaptureSynthetic,
		Targets:       []trace.TargetInfo{{ID: 0, Name: "target", Kind: trace.TargetFile, Size: 1 << 20}},
		Summary:       trace.Summary{NumOps: 3, NumStreams: 1},
	}

	var buf bytes.Buffer
	tw := trace.NewWriter(&buf)
	if err := tw.WriteHeader(hdr); err != nil {
		t.Fatalf("WriteHeader: %v", err)
	}
	base := int64(startOffset)
	for i, op := range []trace.Op{
		{T: base, OpID: trace.Ptr(int64(0)), S: 0, Op: trace.OpOpen, Tgt: &tgt, H: &h, Mode: trace.ModeRead, Dur: trace.Ptr(int64(0))},
		{T: base, OpID: trace.Ptr(int64(1)), S: 0, Op: trace.OpRead, H: &h, Off: &off, Len: &readLen, Dur: trace.Ptr(int64(0))},
		{T: base, OpID: trace.Ptr(int64(2)), S: 0, Op: trace.OpClose, H: &h, Dur: trace.Ptr(int64(0))},
	} {
		if err := tw.WriteOp(op); err != nil {
			t.Fatalf("WriteOp %d: %v", i, err)
		}
	}

	dur := runThinkTrace(t, &buf, hdr, "think", 0)

	if dur < startOffset/2 {
		t.Errorf("duration = %v, want at least %v: the stream started before its recorded offset",
			dur, startOffset/2)
	}
}
