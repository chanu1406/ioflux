package results_test

import (
	"bytes"
	"encoding/json"
	"os"
	"path/filepath"
	"reflect"
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
	if !reflect.DeepEqual(r.Plan, plan) {
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

// TestBuildPropagatesHistogramOverflows verifies that a Recorder's
// HistogramOverflows counter (samples that fell outside the histogram's
// trackable range) reaches Results, so a run with a lost latency sample
// cannot be mistaken for a clean one downstream.
func TestBuildPropagatesHistogramOverflows(t *testing.T) {
	rec := metrics.NewRecorder()
	rec.Record(trace.OpRead, 1_000_000, 4096, false)
	rec.Record(trace.OpRead, 200_000_000_000, 4096, false) // exceeds the 100s trackable range

	r := results.Build(results.PlanInfo{}, results.RunEnv{}, rec, 10_000_000)

	if r.HistogramOverflows != 1 {
		t.Errorf("HistogramOverflows=%d, want 1", r.HistogramOverflows)
	}
	// The overflowing op still completed and counts toward OpsCompleted; only
	// its latency sample was excluded from the percentile histogram.
	if r.OpsCompleted != 2 {
		t.Errorf("OpsCompleted=%d, want 2", r.OpsCompleted)
	}
	if r.Errors != 0 {
		t.Errorf("Errors=%d, want 0 (a histogram overflow is not an op error)", r.Errors)
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

// --- Atomic results output ---

// TestWriteJSONFileRoundTrip verifies the happy path writes parseable results
// with the expected permissions and leaves no temporary file behind.
func TestWriteJSONFileRoundTrip(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "results.json")
	want := &results.Results{OpsCompleted: 42, BytesMoved: 4096}

	if err := results.WriteJSONFile(path, want); err != nil {
		t.Fatalf("WriteJSONFile: %v", err)
	}

	var got results.Results
	raw, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	if err := json.Unmarshal(raw, &got); err != nil {
		t.Fatalf("results file does not parse: %v", err)
	}
	if got.OpsCompleted != want.OpsCompleted || got.BytesMoved != want.BytesMoved {
		t.Fatalf("round trip mismatch: got %+v", got)
	}

	fi, err := os.Stat(path)
	if err != nil {
		t.Fatal(err)
	}
	if perm := fi.Mode().Perm(); perm != 0o644 {
		t.Errorf("results file mode = %v, want 0644", perm)
	}
	assertNoLeftovers(t, dir, "results.json")
}

// TestWriteJSONFileReplacesPrevious verifies a second write fully replaces the
// first rather than overlaying it — a shorter document must not leave a tail of
// the longer one behind.
func TestWriteJSONFileReplacesPrevious(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "results.json")

	long := &results.Results{OpsCompleted: 1}
	for i := 0; i < 50; i++ {
		long.PerOpStats = append(long.PerOpStats, results.PerOpStats{OpType: "READ", Count: int64(i)})
	}
	if err := results.WriteJSONFile(path, long); err != nil {
		t.Fatalf("first write: %v", err)
	}
	if err := results.WriteJSONFile(path, &results.Results{OpsCompleted: 2}); err != nil {
		t.Fatalf("second write: %v", err)
	}

	var got results.Results
	raw, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	if err := json.Unmarshal(raw, &got); err != nil {
		t.Fatalf("results file does not parse after replace: %v", err)
	}
	if got.OpsCompleted != 2 {
		t.Errorf("OpsCompleted=%d, want 2", got.OpsCompleted)
	}
	if len(got.PerOpStats) != 0 {
		t.Errorf("stale per-op stats survived the replace: %v", got.PerOpStats)
	}
	assertNoLeftovers(t, dir, "results.json")
}

// TestWriteJSONFilePreservesPreviousOnFailure is the point of writing
// atomically: when the new results cannot be written completely, the previous
// run's evidence must still be there. Creating the file directly would truncate
// it at open time — here it would even succeed, silently replacing the previous
// results in a directory the operator cannot write to.
//
// It also pins the behavior change that atomicity requires: write access to the
// output directory, not only to the results file.
func TestWriteJSONFilePreservesPreviousOnFailure(t *testing.T) {
	if os.Geteuid() == 0 {
		t.Skip("running as root bypasses directory permissions")
	}
	dir := t.TempDir()
	path := filepath.Join(dir, "results.json")

	previous := []byte(`{"ops_completed":7}` + "\n")
	if err := os.WriteFile(path, previous, 0o644); err != nil {
		t.Fatal(err)
	}
	// Deny creation of the temporary file in the output directory.
	if err := os.Chmod(dir, 0o500); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = os.Chmod(dir, 0o700) })

	err := results.WriteJSONFile(path, &results.Results{OpsCompleted: 99})
	if err == nil {
		t.Fatal("WriteJSONFile succeeded in an unwritable directory; want an error")
	}

	got, readErr := os.ReadFile(path)
	if readErr != nil {
		t.Fatal(readErr)
	}
	if !bytes.Equal(got, previous) {
		t.Fatalf("previous results were damaged by a failed write: got %q, want %q", got, previous)
	}
}

// assertNoLeftovers fails if the output directory holds anything beyond the
// expected file — a leaked temporary would confuse an archived evidence bundle.
func assertNoLeftovers(t *testing.T, dir, want string) {
	t.Helper()
	entries, err := os.ReadDir(dir)
	if err != nil {
		t.Fatal(err)
	}
	for _, e := range entries {
		if e.Name() != want {
			t.Errorf("unexpected leftover file in output directory: %q", e.Name())
		}
	}
}
