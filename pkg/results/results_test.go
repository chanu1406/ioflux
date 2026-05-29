package results_test

import (
	"bytes"
	"encoding/json"
	"testing"

	"github.com/chanuollala/ioflux/pkg/metrics"
	"github.com/chanuollala/ioflux/pkg/results"
	"github.com/chanuollala/ioflux/pkg/trace"
)

func TestBuildAndWriteJSON(t *testing.T) {
	rec := metrics.NewRecorder()
	rec.Record(trace.OpRead, 500_000, 4096, false)
	rec.Record(trace.OpRead, 1_000_000, 4096, false)
	rec.Record(trace.OpOpen, 200_000, 0, false)
	rec.Record(trace.OpClose, 150_000, 0, false)

	plan := results.PlanInfo{
		TracePath:  "test.ioflux",
		Engine:     "mem",
		Mode:       "asap",
		TraceKind:  "synthetic",
		NumStreams: 2,
		NumOps:     4,
		TotalBytes: 8192,
	}
	r := results.Build(plan, results.RunEnv{}, rec, 10_000_000)

	if r.OpsCompleted != 4 {
		t.Errorf("OpsCompleted=%d, want 4", r.OpsCompleted)
	}
	if r.BytesMoved != 8192 {
		t.Errorf("BytesMoved=%d, want 8192", r.BytesMoved)
	}
	if r.Errors != 0 {
		t.Errorf("Errors=%d, want 0", r.Errors)
	}
	if r.DurationNS != 10_000_000 {
		t.Errorf("DurationNS=%d, want 10000000", r.DurationNS)
	}
	if r.Plan != plan {
		t.Errorf("Plan mismatch: got %+v, want %+v", r.Plan, plan)
	}

	pm := r.PerOpMap()
	for _, opType := range []string{"READ", "OPEN", "CLOSE"} {
		if _, ok := pm[opType]; !ok {
			t.Errorf("per_op_stats missing %s", opType)
		}
	}

	readStats := pm["READ"]
	if readStats.Count != 2 {
		t.Errorf("READ count=%d, want 2", readStats.Count)
	}
	// p50 ≤ p90 ≤ p99 ≤ max
	if !(readStats.P50NS <= readStats.P90NS &&
		readStats.P90NS <= readStats.P99NS &&
		readStats.P99NS <= readStats.MaxNS) {
		t.Errorf("READ percentiles not monotonic: p50=%d p90=%d p99=%d max=%d",
			readStats.P50NS, readStats.P90NS, readStats.P99NS, readStats.MaxNS)
	}

	// WriteJSON should produce valid JSON.
	var buf bytes.Buffer
	if err := results.WriteJSON(&buf, r); err != nil {
		t.Fatalf("WriteJSON: %v", err)
	}
	var check map[string]any
	if err := json.Unmarshal(buf.Bytes(), &check); err != nil {
		t.Fatalf("output is not valid JSON: %v", err)
	}
	for _, key := range []string{"generated_at", "plan", "ops_completed", "bytes_moved", "errors", "per_op_stats"} {
		if _, ok := check[key]; !ok {
			t.Errorf("results JSON missing top-level key %q", key)
		}
	}
}

// TestPerOpStatsMonotonic verifies monotonicity for a larger sample.
func TestPerOpStatsMonotonic(t *testing.T) {
	rec := metrics.NewRecorder()
	for i := int64(1); i <= 100; i++ {
		rec.Record(trace.OpRead, i*1_000_000, 512, false)
	}
	r := results.Build(results.PlanInfo{}, results.RunEnv{}, rec, 0)
	pm := r.PerOpMap()
	s := pm["READ"]
	if !(s.P50NS <= s.P90NS && s.P90NS <= s.P99NS && s.P99NS <= s.MaxNS) {
		t.Errorf("not monotonic: p50=%d p90=%d p99=%d max=%d", s.P50NS, s.P90NS, s.P99NS, s.MaxNS)
	}
}
