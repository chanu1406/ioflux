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
