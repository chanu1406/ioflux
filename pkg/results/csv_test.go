package results_test

import (
	"bytes"
	"encoding/csv"
	"os"
	"path/filepath"
	"testing"

	"github.com/chanuollala/ioflux/pkg/results"
)

// goldenResults returns a Results with a fixed timestamp and zero-value fields
// so its CSV output is deterministic.
func goldenResults() *results.Results {
	return &results.Results{
		GeneratedAt: "2024-01-01T00:00:00Z",
		Plan: results.PlanInfo{
			TracePath:   "golden.ioflux",
			TraceKind:   "synthetic",
			Profile:     "training-read",
			Engine:      "mem",
			Mode:        "asap",
			NumStreams:  1,
			NumOps:      10,
			MaxInflight: 512,
		},
		RunEnv: results.RunEnv{CacheMode: "cold"},
	}
}

// TestAppendCSV_WritesHeaderOnce verifies that two AppendCSV calls produce
// exactly one header row followed by two data rows.
func TestAppendCSV_WritesHeaderOnce(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "results.csv")

	for i := 0; i < 2; i++ {
		if err := results.AppendCSV(path, goldenResults()); err != nil {
			t.Fatalf("AppendCSV call %d: %v", i+1, err)
		}
	}

	f, err := os.Open(path)
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close()

	recs, err := csv.NewReader(f).ReadAll()
	if err != nil {
		t.Fatalf("csv.ReadAll: %v", err)
	}
	if len(recs) != 3 {
		t.Fatalf("got %d rows (header + data), want 3", len(recs))
	}
	if recs[0][0] != "timestamp" {
		t.Errorf("header[0]=%q, want timestamp", recs[0][0])
	}
	if recs[0][len(recs[0])-1] != "low_fidelity" {
		t.Errorf("header last col=%q, want low_fidelity", recs[0][len(recs[0])-1])
	}
	// Both data rows must have the same column count as the header.
	if len(recs[1]) != len(recs[0]) || len(recs[2]) != len(recs[0]) {
		t.Errorf("data row column counts %d/%d differ from header %d",
			len(recs[1]), len(recs[2]), len(recs[0]))
	}
}

func TestAppendCSV_CheckpointWriteDataOp(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "results.csv")

	res := goldenResults()
	res.Plan.TracePath = "ckpt.ioflux"
	res.Plan.Profile = "checkpoint-write"
	res.PerOpStats = []results.PerOpStats{
		{OpType: "OPEN", Count: 4, P50NS: 10, P99NS: 20, P999NS: 30},
		{OpType: "WRITE", Count: 64, P50NS: 1000, P99NS: 2000, P999NS: 3000},
		{OpType: "FSYNC", Count: 4, P50NS: 40, P99NS: 50, P999NS: 60},
	}

	if err := results.AppendCSV(path, res); err != nil {
		t.Fatalf("AppendCSV: %v", err)
	}
	f, err := os.Open(path)
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close()
	recs, err := csv.NewReader(f).ReadAll()
	if err != nil {
		t.Fatalf("csv.ReadAll: %v", err)
	}
	if len(recs) != 2 {
		t.Fatalf("got %d rows, want header + one data row", len(recs))
	}

	row := rowByColumn(recs[0], recs[1])
	if row["trace_profile"] != "checkpoint-write" {
		t.Errorf("trace_profile=%q, want checkpoint-write", row["trace_profile"])
	}
	if row["data_op"] != "WRITE" {
		t.Errorf("data_op=%q, want WRITE", row["data_op"])
	}
	if row["data_op_p50_ns"] != "1000" || row["data_op_p99_ns"] != "2000" || row["data_op_p999_ns"] != "3000" {
		t.Errorf("data-op latencies = p50 %s p99 %s p999 %s, want 1000/2000/3000",
			row["data_op_p50_ns"], row["data_op_p99_ns"], row["data_op_p999_ns"])
	}
	if row["read_p50_ns"] != "0" || row["read_p99_ns"] != "0" || row["read_p999_ns"] != "0" {
		t.Errorf("read latency columns should stay zero for a write trace, got %s/%s/%s",
			row["read_p50_ns"], row["read_p99_ns"], row["read_p999_ns"])
	}
}

func TestAppendCSV_DataOpUsesCount(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "results.csv")

	res := goldenResults()
	res.PerOpStats = []results.PerOpStats{
		{OpType: "WRITE", Count: 1, P50NS: 1000, P99NS: 2000, P999NS: 3000},
		{OpType: "READ", Count: 10, P50NS: 4000, P99NS: 5000, P999NS: 6000},
	}

	if err := results.AppendCSV(path, res); err != nil {
		t.Fatalf("AppendCSV: %v", err)
	}
	f, err := os.Open(path)
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close()
	recs, err := csv.NewReader(f).ReadAll()
	if err != nil {
		t.Fatalf("csv.ReadAll: %v", err)
	}
	row := rowByColumn(recs[0], recs[1])
	if row["data_op"] != "READ" {
		t.Errorf("data_op=%q, want READ by highest data-op count", row["data_op"])
	}
	if row["data_op_p50_ns"] != "4000" || row["data_op_p99_ns"] != "5000" || row["data_op_p999_ns"] != "6000" {
		t.Errorf("data-op latencies = p50 %s p99 %s p999 %s, want 4000/5000/6000",
			row["data_op_p50_ns"], row["data_op_p99_ns"], row["data_op_p999_ns"])
	}
}

// TestAppendCSV_ColumnsStable byte-compares AppendCSV output to the golden file.
func TestAppendCSV_ColumnsStable(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "results.csv")

	if err := results.AppendCSV(path, goldenResults()); err != nil {
		t.Fatalf("AppendCSV: %v", err)
	}

	got, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	golden, err := os.ReadFile(filepath.Join("testdata", "results_golden.csv"))
	if err != nil {
		t.Fatalf("read golden: %v", err)
	}
	if !bytes.Equal(got, golden) {
		t.Errorf("CSV output does not match golden file.\ngot:\n%s\nwant:\n%s", got, golden)
	}
}

func rowByColumn(header, row []string) map[string]string {
	out := make(map[string]string, len(header))
	for i, name := range header {
		if i < len(row) {
			out[name] = row[i]
		}
	}
	return out
}
