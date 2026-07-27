package main

import (
	"bytes"
	"encoding/csv"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"testing"

	"github.com/chanuollala/ioflux/pkg/results"
)

func runRunCLI(args []string) (int, string, string) {
	var stdout, stderr bytes.Buffer
	code := runRun(args, &stdout, &stderr)
	return code, stdout.String(), stderr.String()
}

func TestRunCmd_BasicSmoke(t *testing.T) {
	dir := t.TempDir()
	tracePath := filepath.Join(dir, "trace.ioflux")
	resultsPath := filepath.Join(dir, "results.json")

	code, _, stderr := runGenCLI([]string{
		"training-read",
		"--shards", "2",
		"--shard-size", "64KiB",
		"--record-size", "8KiB",
		"--dataloader-workers", "1",
		"--shuffle=false",
		"--seed", "1",
		"-o", tracePath,
	})
	if code != 0 {
		t.Fatalf("runGen exit=%d, stderr=%s", code, stderr)
	}

	code, stdout, stderr := runRunCLI([]string{
		"--trace", tracePath,
		"--engine", "mem",
		"--mode", "asap",
		"-o", resultsPath,
	})
	if code != 0 {
		t.Fatalf("runRun exit=%d want 0; stdout=%s stderr=%s", code, stdout, stderr)
	}
	if !strings.Contains(stdout, "wrote "+resultsPath) {
		t.Fatalf("stdout should confirm write, got %q", stdout)
	}
	got, err := os.ReadFile(resultsPath)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Contains(got, []byte(`"errors": 0`)) {
		t.Fatalf("results should contain zero op errors, got:\n%s", got)
	}
}

func TestRunCmd_LocalEngine(t *testing.T) {
	dir := t.TempDir()
	targetPath := filepath.Join(dir, "shard.dat")
	tracePath := filepath.Join(dir, "trace.ioflux")
	resultsPath := filepath.Join(dir, "results.json")

	if err := os.WriteFile(targetPath, make([]byte, 32*1024), 0o644); err != nil {
		t.Fatal(err)
	}
	content := fmt.Sprintf(`{"ioflux_trace_version":1,"kind":"synthetic","time_unit":"ns","capture_method":"synthetic","scrubbed":false,"targets":[{"id":0,"name":%q,"kind":"file","size":32768}],"summary":{"num_ops":3,"num_streams":1,"num_groups":0,"total_bytes":8192,"duration_ns":0}}
{"t":0,"op_id":0,"s":0,"op":"OPEN","tgt":0,"h":1,"mode":"r"}
{"t":1,"op_id":1,"s":0,"op":"READ","h":1,"off":0,"len":8192}
{"t":2,"op_id":2,"s":0,"op":"CLOSE","h":1}
`, targetPath)
	if err := os.WriteFile(tracePath, []byte(content), 0o600); err != nil {
		t.Fatal(err)
	}

	code, stdout, stderr := runRunCLI([]string{
		"--trace", tracePath,
		"--engine", "local",
		"--mode", "asap",
		"-o", resultsPath,
	})
	if code != 0 {
		t.Fatalf("runRun local exit=%d want 0; stdout=%s stderr=%s", code, stdout, stderr)
	}
	got, err := os.ReadFile(resultsPath)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Contains(got, []byte(`"engine": "local"`)) {
		t.Fatalf("results should record local engine, got:\n%s", got)
	}
	if !bytes.Contains(got, []byte(`"bytes_moved": 8192`)) {
		t.Fatalf("results should record local read bytes, got:\n%s", got)
	}
}

func TestRunCmd_ShortReadWritesInvalidResultAndExitsOne(t *testing.T) {
	dir := t.TempDir()
	targetPath := filepath.Join(dir, "short.dat")
	tracePath := filepath.Join(dir, "trace.ioflux")
	resultsPath := filepath.Join(dir, "results.json")

	if err := os.WriteFile(targetPath, make([]byte, 512), 0o644); err != nil {
		t.Fatal(err)
	}
	content := fmt.Sprintf(`{"ioflux_trace_version":1,"kind":"synthetic","time_unit":"ns","capture_method":"synthetic","scrubbed":false,"targets":[{"id":0,"name":%q,"kind":"file","size":1024}],"summary":{"num_ops":3,"num_streams":1,"num_groups":0,"total_bytes":1024,"duration_ns":0}}
{"t":0,"op_id":0,"s":0,"op":"OPEN","tgt":0,"h":1,"mode":"r"}
{"t":1,"op_id":1,"s":0,"op":"READ","h":1,"off":0,"len":1024}
{"t":2,"op_id":2,"s":0,"op":"CLOSE","h":1}
`, targetPath)
	if err := os.WriteFile(tracePath, []byte(content), 0o600); err != nil {
		t.Fatal(err)
	}

	code, stdout, stderr := runRunCLI([]string{
		"--trace", tracePath,
		"--engine", "local",
		"--mode", "asap",
		"-o", resultsPath,
	})
	if code != 1 {
		t.Fatalf("runRun exit=%d want 1; stdout=%s stderr=%s", code, stdout, stderr)
	}
	if !strings.Contains(stderr, "1 op(s) failed") {
		t.Fatalf("stderr should identify the invalid execution, got %q", stderr)
	}
	got, err := os.ReadFile(resultsPath)
	if err != nil {
		t.Fatal(err)
	}
	for _, want := range []string{`"errors": 1`, `"short_reads": 1`, `"bytes_moved": 512`} {
		if !bytes.Contains(got, []byte(want)) {
			t.Fatalf("results should contain %s, got:\n%s", want, got)
		}
	}
}

// TestRunCmd_HistogramOverflowInvalidatesResultAndExitsOne verifies that a
// latency sample outside the histogram's 100s trackable range invalidates the
// run (exit 1) even though every op completed successfully, mirroring the
// short-transfer honesty guardrail. It exploits timeline mode's
// intended-arrival accounting to synthesize a ~200s sample without any real
// wait: an op whose trace timestamp is far in the past has its intended
// arrival long before the run starts, so the scheduler dispatches it
// immediately, but the recorded drift/completion-lag/latency against that
// stale intended arrival is still ~200s.
func TestRunCmd_HistogramOverflowInvalidatesResultAndExitsOne(t *testing.T) {
	dir := t.TempDir()
	tracePath := filepath.Join(dir, "trace.ioflux")
	resultsPath := filepath.Join(dir, "results.json")

	const farPastNS = -200_000_000_000 // 200s before the run starts
	content := fmt.Sprintf(`{"ioflux_trace_version":1,"kind":"synthetic","time_unit":"ns","capture_method":"synthetic","scrubbed":false,"targets":[{"id":0,"name":"target-a","kind":"file","size":8192}],"summary":{"num_ops":3,"num_streams":1,"num_groups":0,"total_bytes":8192,"duration_ns":0}}
{"t":%d,"op_id":0,"s":0,"op":"OPEN","tgt":0,"h":1,"mode":"r"}
{"t":%d,"op_id":1,"s":0,"op":"READ","h":1,"off":0,"len":8192}
{"t":%d,"op_id":2,"s":0,"op":"CLOSE","h":1}
`, farPastNS, farPastNS, farPastNS)
	if err := os.WriteFile(tracePath, []byte(content), 0o600); err != nil {
		t.Fatal(err)
	}

	code, stdout, stderr := runRunCLI([]string{
		"--trace", tracePath,
		"--engine", "mem",
		"--mode", "timeline",
		"-o", resultsPath,
	})
	if code != 1 {
		t.Fatalf("runRun exit=%d want 1; stdout=%s stderr=%s", code, stdout, stderr)
	}
	if !strings.Contains(stderr, "latency sample(s) exceeded the histogram's trackable range") {
		t.Fatalf("stderr should identify the histogram overflow, got %q", stderr)
	}

	got, err := os.ReadFile(resultsPath)
	if err != nil {
		t.Fatal(err)
	}
	var res results.Results
	if err := json.Unmarshal(got, &res); err != nil {
		t.Fatalf("unmarshal results.json: %v", err)
	}
	if res.HistogramOverflows <= 0 {
		t.Errorf("HistogramOverflows=%d, want > 0", res.HistogramOverflows)
	}
	if res.Errors != 0 {
		t.Errorf("Errors=%d, want 0 (every op completed; only the latency sample overflowed)", res.Errors)
	}
	if res.OpsCompleted != 3 {
		t.Errorf("OpsCompleted=%d, want 3", res.OpsCompleted)
	}
}

func TestRunCmd_S3RequiresBucket(t *testing.T) {
	dir := t.TempDir()
	tracePath := filepath.Join(dir, "trace.ioflux")
	resultsPath := filepath.Join(dir, "results.json")

	if code, _, stderr := runGenCLI([]string{
		"training-read",
		"--shards", "1",
		"--shard-size", "64KiB",
		"--record-size", "8KiB",
		"--dataloader-workers", "1",
		"--shuffle=false",
		"--seed", "1",
		"-o", tracePath,
	}); code != 0 {
		t.Fatalf("runGen exit=%d, stderr=%s", code, stderr)
	}

	code, _, stderr := runRunCLI([]string{
		"--trace", tracePath,
		"--engine", "s3",
		"-o", resultsPath,
	})
	if code != 2 {
		t.Fatalf("runRun s3 without bucket exit=%d want 2; stderr=%s", code, stderr)
	}
	if !strings.Contains(stderr, "bucket is required") {
		t.Fatalf("stderr should mention missing bucket, got %q", stderr)
	}
}

func TestRunCmd_S3TargetMapBucketMismatch(t *testing.T) {
	dir := t.TempDir()
	tracePath := filepath.Join(dir, "trace.ioflux")
	mapPath := filepath.Join(dir, "map.yaml")
	resultsPath := filepath.Join(dir, "results.json")

	if code, _, stderr := runGenCLI([]string{
		"training-read",
		"--shards", "1",
		"--shard-size", "64KiB",
		"--record-size", "8KiB",
		"--dataloader-workers", "1",
		"--shuffle=false",
		"--seed", "1",
		"-o", tracePath,
	}); code != 0 {
		t.Fatalf("runGen exit=%d, stderr=%s", code, stderr)
	}
	if err := os.WriteFile(mapPath, []byte("target_rewrite:\n  - from: \"\"\n    to: \"s3://other-bucket/imagenet/\"\n"), 0o644); err != nil {
		t.Fatal(err)
	}

	code, _, stderr := runRunCLI([]string{
		"--trace", tracePath,
		"--engine", "s3",
		"--endpoint", "http://127.0.0.1:1",
		"--bucket", "bench",
		"--path-style",
		"--access-key", "test-access",
		"--secret-key", "test-secret",
		"--target-map", mapPath,
		"-o", resultsPath,
	})
	if code != 1 {
		t.Fatalf("runRun s3 bucket mismatch exit=%d want 1; stderr=%s", code, stderr)
	}
	if !strings.Contains(stderr, "rule targets bucket") {
		t.Fatalf("stderr should mention bucket mismatch, got %q", stderr)
	}
}

func TestRunCmd_S3EngineWithTargetMapAndPrepare(t *testing.T) {
	dir := t.TempDir()
	tracePath := filepath.Join(dir, "trace.ioflux")
	mapPath := filepath.Join(dir, "map.yaml")
	resultsPath := filepath.Join(dir, "results.json")

	if code, _, stderr := runGenCLI([]string{
		"training-read",
		"--shards", "2",
		"--shard-size", "64KiB",
		"--record-size", "8KiB",
		"--dataloader-workers", "1",
		"--shuffle=false",
		"--seed", "1",
		"-o", tracePath,
	}); code != 0 {
		t.Fatalf("runGen exit=%d, stderr=%s", code, stderr)
	}
	if err := os.WriteFile(mapPath, []byte("target_rewrite:\n  - from: \"\"\n    to: \"s3://bench/imagenet/\"\n"), 0o644); err != nil {
		t.Fatal(err)
	}

	objects := make(map[string][]byte)
	var mu sync.Mutex
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if !strings.HasPrefix(r.URL.Path, "/bench/") {
			http.NotFound(w, r)
			return
		}
		key := strings.TrimPrefix(r.URL.Path, "/bench/")

		switch r.Method {
		case http.MethodPut:
			body, err := io.ReadAll(r.Body)
			if err != nil {
				t.Errorf("ReadAll PutObject body: %v", err)
				w.WriteHeader(http.StatusInternalServerError)
				return
			}
			mu.Lock()
			objects[key] = body
			mu.Unlock()
			w.Header().Set("ETag", `"put"`)
			w.WriteHeader(http.StatusOK)
		case http.MethodHead:
			mu.Lock()
			body, ok := objects[key]
			mu.Unlock()
			if !ok {
				http.NotFound(w, r)
				return
			}
			w.Header().Set("Content-Length", strconv.Itoa(len(body)))
			w.WriteHeader(http.StatusOK)
		case http.MethodGet:
			mu.Lock()
			body, ok := objects[key]
			mu.Unlock()
			if !ok {
				http.NotFound(w, r)
				return
			}
			start, end, ok := parseTestRange(r.Header.Get("Range"), len(body))
			if !ok {
				w.WriteHeader(http.StatusRequestedRangeNotSatisfiable)
				return
			}
			w.Header().Set("Content-Length", strconv.Itoa(end-start+1))
			w.WriteHeader(http.StatusPartialContent)
			_, _ = w.Write(body[start : end+1])
		default:
			t.Errorf("unexpected S3 method %s", r.Method)
			w.WriteHeader(http.StatusMethodNotAllowed)
		}
	}))
	defer srv.Close()

	code, stdout, stderr := runRunCLI([]string{
		"--trace", tracePath,
		"--engine", "s3",
		"--endpoint", srv.URL,
		"--bucket", "bench",
		"--path-style",
		"--access-key", "test-access",
		"--secret-key", "test-secret",
		"--target-map", mapPath,
		"--prepare", "materialize-synthetic",
		"--cache-mode", "cold",
		"-o", resultsPath,
	})
	if code != 0 {
		t.Fatalf("runRun s3 exit=%d want 0; stdout=%s stderr=%s", code, stdout, stderr)
	}
	got, err := os.ReadFile(resultsPath)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Contains(got, []byte(`"engine": "s3"`)) {
		t.Fatalf("results should record s3 engine, got:\n%s", got)
	}
	if !bytes.Contains(got, []byte(`"errors": 0`)) {
		t.Fatalf("results should contain zero op errors, got:\n%s", got)
	}

	mu.Lock()
	defer mu.Unlock()
	if len(objects) != 2 {
		t.Fatalf("materialize-synthetic should PUT 2 objects, got %d (%v)", len(objects), objects)
	}
}

func TestRunCmd_WorkersFlagRemoved(t *testing.T) {
	code, _, stderr := runRunCLI([]string{"--workers", "1"})
	if code != 2 {
		t.Fatalf("exit=%d want 2; stderr=%s", code, stderr)
	}
	if !strings.Contains(stderr, "flag provided but not defined") {
		t.Fatalf("stderr should report unknown flag, got %q", stderr)
	}
	if strings.Contains(runUsage, "--workers") {
		t.Fatal("runUsage should not advertise --workers")
	}
}

func TestRunCmd_PrepareRejectsMalformedTrace(t *testing.T) {
	dir := t.TempDir()
	tracePath := filepath.Join(dir, "bad.ioflux")
	resultsPath := filepath.Join(dir, "results.json")
	content := `{"ioflux_trace_version":1,"kind":"synthetic","time_unit":"ns","capture_method":"synthetic","scrubbed":false,"targets":[{"id":0,"name":"a","kind":"file","size":1024}],"summary":{"num_ops":1,"num_streams":1,"num_groups":0,"total_bytes":0,"duration_ns":0}}
{"t":0,"op_id":0,"s":0,"op":"OPEN","tgt":99,"h":1,"mode":"r"}
`
	if err := os.WriteFile(tracePath, []byte(content), 0o600); err != nil {
		t.Fatal(err)
	}

	code, _, stderr := runRunCLI([]string{
		"--trace", tracePath,
		"--engine", "mem",
		"-o", resultsPath,
	})
	if code != 1 {
		t.Fatalf("exit=%d want 1; stderr=%s", code, stderr)
	}
	if !strings.Contains(stderr, "invalid trace") || !strings.Contains(stderr, "out of range") {
		t.Fatalf("stderr should mention validation failure, got %q", stderr)
	}
}

func TestRunCmd_TimelineFlagsAccepted(t *testing.T) {
	dir := t.TempDir()
	tracePath := filepath.Join(dir, "trace.ioflux")
	resultsPath := filepath.Join(dir, "results.json")

	if code, _, stderr := runGenCLI([]string{
		"training-read",
		"--shards", "2",
		"--shard-size", "64KiB",
		"--record-size", "8KiB",
		"--dataloader-workers", "1",
		"--shuffle=false",
		"--seed", "1",
		"-o", tracePath,
	}); code != 0 {
		t.Fatalf("runGen exit=%d, stderr=%s", code, stderr)
	}

	code, _, stderr := runRunCLI([]string{
		"--trace", tracePath,
		"--engine", "mem",
		"--mode", "timeline",
		"--max-inflight", "64",
		"-o", resultsPath,
	})
	if code != 0 {
		t.Fatalf("--mode timeline exit=%d want 0; stderr=%s", code, stderr)
	}

	code, _, stderr = runRunCLI([]string{
		"--trace", tracePath,
		"--mode", "scaled",
		"--speedup", "2.0",
		"--max-inflight", "32",
		"-o", resultsPath,
	})
	if code != 0 {
		t.Fatalf("--mode scaled exit=%d want 0; stderr=%s", code, stderr)
	}

	code, _, stderr = runRunCLI([]string{
		"--trace", tracePath,
		"--mode", "nonsense",
		"-o", resultsPath,
	})
	if code != 2 {
		t.Fatalf("--mode nonsense exit=%d want 2; stderr=%s", code, stderr)
	}
	if !strings.Contains(stderr, "unsupported mode") {
		t.Fatalf("stderr should report unsupported mode, got %q", stderr)
	}

	code, _, stderr = runRunCLI([]string{
		"--trace", tracePath,
		"--max-inflight", "0",
		"-o", resultsPath,
	})
	if code != 2 {
		t.Fatalf("--max-inflight 0 exit=%d want 2; stderr=%s", code, stderr)
	}
}

func TestRunCmd_UnmappedTargetRejected(t *testing.T) {
	dir := t.TempDir()
	tracePath := filepath.Join(dir, "trace.ioflux")
	mapPath := filepath.Join(dir, "map.yaml")
	resultsPath := filepath.Join(dir, "results.json")

	// Generate a small trace; targets will be bare names like "shard_0000.tar".
	if code, _, stderr := runGenCLI([]string{
		"training-read",
		"--shards", "2",
		"--shard-size", "64KiB",
		"--record-size", "8KiB",
		"--dataloader-workers", "1",
		"--shuffle=false",
		"--seed", "1",
		"-o", tracePath,
	}); code != 0 {
		t.Fatalf("runGen exit=%d, stderr=%s", code, stderr)
	}

	// Target map that only matches "/mnt/" prefix — will miss the bare names.
	if err := os.WriteFile(mapPath, []byte("target_rewrite:\n  - from: \"/mnt/\"\n    to: \"/tmp/\"\n"), 0o644); err != nil {
		t.Fatal(err)
	}

	code, _, stderr := runRunCLI([]string{
		"--trace", tracePath,
		"--engine", "mem",
		"--target-map", mapPath,
		"-o", resultsPath,
	})
	if code != 1 {
		t.Fatalf("exit=%d want 1 (unmatched target); stderr=%s", code, stderr)
	}
	if !strings.Contains(stderr, "matched no rule") {
		t.Fatalf("stderr should mention unmatched rule, got %q", stderr)
	}
}

func TestRunMetadataRecordsCacheState(t *testing.T) {
	dir := t.TempDir()
	tracePath := filepath.Join(dir, "trace.ioflux")
	resultsPath := filepath.Join(dir, "results.json")

	if code, _, stderr := runGenCLI([]string{
		"training-read",
		"--shards", "2",
		"--shard-size", "64KiB",
		"--record-size", "8KiB",
		"--dataloader-workers", "1",
		"--shuffle=false",
		"--seed", "1",
		"-o", tracePath,
	}); code != 0 {
		t.Fatalf("runGen exit=%d, stderr=%s", code, stderr)
	}

	code, _, stderr := runRunCLI([]string{
		"--trace", tracePath,
		"--engine", "mem",
		"--cache-mode", "warm",
		"-o", resultsPath,
	})
	if code != 0 {
		t.Fatalf("runRun exit=%d want 0; stderr=%s", code, stderr)
	}

	got, err := os.ReadFile(resultsPath)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Contains(got, []byte(`"cache_mode": "warm"`)) {
		t.Errorf("results.json should contain cache_mode=warm, got:\n%s", got)
	}
}

func TestRunUsageExitCodeDocsMentionOpErrors(t *testing.T) {
	if !strings.Contains(runUsage, "completed with op errors") {
		t.Fatalf("runUsage should document op-error exit semantics, got:\n%s", runUsage)
	}
	if strings.Contains(runUsage, "engine error)") {
		t.Fatalf("runUsage still uses old engine-error wording:\n%s", runUsage)
	}
}

// TestRunCmd_CSVAppend verifies that two runs with --csv append a header row
// and two data rows, with the header written only once.
func TestRunCmd_CSVAppend(t *testing.T) {
	dir := t.TempDir()
	tracePath := filepath.Join(dir, "trace.ioflux")
	resultsPath := filepath.Join(dir, "results.json")
	csvPath := filepath.Join(dir, "results.csv")

	if code, _, stderr := runGenCLI([]string{
		"training-read",
		"--shards", "2",
		"--shard-size", "64KiB",
		"--record-size", "8KiB",
		"--dataloader-workers", "1",
		"--shuffle=false",
		"--seed", "1",
		"-o", tracePath,
	}); code != 0 {
		t.Fatalf("runGen exit=%d, stderr=%s", code, stderr)
	}

	baseArgs := []string{
		"--trace", tracePath,
		"--engine", "mem",
		"--mode", "asap",
		"-o", resultsPath,
		"--csv", csvPath,
	}
	for i := 0; i < 2; i++ {
		if code, _, stderr := runRunCLI(baseArgs); code != 0 {
			t.Fatalf("run %d: exit=%d want 0; stderr=%s", i+1, code, stderr)
		}
	}

	f, err := os.Open(csvPath)
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close()

	recs, err := csv.NewReader(f).ReadAll()
	if err != nil {
		t.Fatalf("parse CSV: %v", err)
	}
	// header + 2 data rows = 3 records
	if len(recs) != 3 {
		t.Fatalf("got %d CSV rows (incl header), want 3", len(recs))
	}
	if recs[0][0] != "timestamp" {
		t.Errorf("header[0]=%q, want timestamp", recs[0][0])
	}
	if recs[0][len(recs[0])-1] != "low_fidelity" {
		t.Errorf("header last col=%q, want low_fidelity", recs[0][len(recs[0])-1])
	}
}

func parseTestRange(header string, size int) (int, int, bool) {
	if !strings.HasPrefix(header, "bytes=") {
		return 0, 0, false
	}
	parts := strings.Split(strings.TrimPrefix(header, "bytes="), "-")
	if len(parts) != 2 {
		return 0, 0, false
	}
	start, err := strconv.Atoi(parts[0])
	if err != nil {
		return 0, 0, false
	}
	end, err := strconv.Atoi(parts[1])
	if err != nil {
		return 0, 0, false
	}
	if start < 0 || end < start || end >= size {
		return 0, 0, false
	}
	return start, end, true
}

// oneTargetReadTrace writes a minimal single-target read trace naming target.
func oneTargetReadTrace(t *testing.T, path, target string, size int) {
	t.Helper()
	content := fmt.Sprintf(`{"ioflux_trace_version":1,"kind":"synthetic","time_unit":"ns","capture_method":"synthetic","scrubbed":false,"targets":[{"id":0,"name":%q,"kind":"file","size":%d}],"summary":{"num_ops":3,"num_streams":1,"num_groups":0,"total_bytes":%d,"duration_ns":0}}
{"t":0,"op_id":0,"s":0,"op":"OPEN","tgt":0,"h":1,"mode":"r"}
{"t":1,"op_id":1,"s":0,"op":"READ","h":1,"off":0,"len":%d}
{"t":2,"op_id":2,"s":0,"op":"CLOSE","h":1}
`, target, size, size, size)
	if err := os.WriteFile(path, []byte(content), 0o600); err != nil {
		t.Fatal(err)
	}
}

// TestRunCmd_TargetRootRejectsEscapingTarget is the end-to-end guard on the
// destructive path: materialize-synthetic opens every target CREATE|TRUNC, so a
// trace naming a path outside --target-root must fail the run rather than
// truncate real data. Trace target names are untrusted input.
func TestRunCmd_TargetRootRejectsEscapingTarget(t *testing.T) {
	base := t.TempDir()
	root := filepath.Join(base, "scratch")
	if err := os.MkdirAll(root, 0o755); err != nil {
		t.Fatal(err)
	}
	outside := filepath.Join(base, "precious.dat")
	original := []byte("real data that must survive the run")
	if err := os.WriteFile(outside, original, 0o644); err != nil {
		t.Fatal(err)
	}

	tracePath := filepath.Join(base, "trace.ioflux")
	resultsPath := filepath.Join(base, "results.json")
	// A "../" escape out of the configured root, as an imported or hand-edited
	// trace could easily contain.
	oneTargetReadTrace(t, tracePath, filepath.Join(root, "..", "precious.dat"), len(original))

	code, stdout, stderr := runRunCLI([]string{
		"--trace", tracePath,
		"--engine", "local",
		"--target-root", root,
		"--prepare", "materialize-synthetic",
		"-o", resultsPath,
	})
	if code != 1 {
		t.Fatalf("runRun exit=%d want 1; stdout=%s stderr=%s", code, stdout, stderr)
	}
	if !strings.Contains(stderr, "outside the configured root") {
		t.Fatalf("stderr should explain the containment rejection, got %q", stderr)
	}

	got, err := os.ReadFile(outside)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(got, original) {
		t.Fatalf("preparation overwrote a file outside --target-root: got %q, want %q", got, original)
	}
}

// TestRunCmd_TargetRootAllowsContainedRun verifies containment does not break a
// legitimate run whose targets live under the root.
func TestRunCmd_TargetRootAllowsContainedRun(t *testing.T) {
	base := t.TempDir()
	root := filepath.Join(base, "scratch")
	if err := os.MkdirAll(root, 0o755); err != nil {
		t.Fatal(err)
	}
	tracePath := filepath.Join(base, "trace.ioflux")
	resultsPath := filepath.Join(base, "results.json")
	const size = 8192
	oneTargetReadTrace(t, tracePath, filepath.Join(root, "shard.dat"), size)

	code, stdout, stderr := runRunCLI([]string{
		"--trace", tracePath,
		"--engine", "local",
		"--target-root", root,
		"--prepare", "materialize-synthetic",
		"-o", resultsPath,
	})
	if code != 0 {
		t.Fatalf("runRun exit=%d want 0; stdout=%s stderr=%s", code, stdout, stderr)
	}
	res := decodeResults(t, resultsPath)
	if res.BytesMoved != size {
		t.Fatalf("bytes_moved=%d, want %d", res.BytesMoved, size)
	}
	joined := strings.Join(res.RunEnv.EngineLimitations, "\n")
	if strings.Contains(joined, "no target root configured") {
		t.Fatalf("a confined run must not report itself unconfined: %q", joined)
	}
}

// TestRunCmd_NoTargetRootRecordsLimitation verifies an unconfined local run says
// so in results.json. The guardrail is not that IOFlux always confines targets —
// it is that a saved report never leaves containment unstated.
func TestRunCmd_NoTargetRootRecordsLimitation(t *testing.T) {
	base := t.TempDir()
	tracePath := filepath.Join(base, "trace.ioflux")
	resultsPath := filepath.Join(base, "results.json")
	const size = 4096
	oneTargetReadTrace(t, tracePath, filepath.Join(base, "shard.dat"), size)

	code, stdout, stderr := runRunCLI([]string{
		"--trace", tracePath,
		"--engine", "local",
		"--prepare", "materialize-synthetic",
		"-o", resultsPath,
	})
	if code != 0 {
		t.Fatalf("runRun exit=%d want 0; stdout=%s stderr=%s", code, stdout, stderr)
	}
	res := decodeResults(t, resultsPath)
	joined := strings.Join(res.RunEnv.EngineLimitations, "\n")
	if !strings.Contains(joined, "no target root configured") {
		t.Fatalf("engine_limitations=%q, want a note that targets were not confined", joined)
	}
}

// TestRunCmd_TargetRootRejectedWithHosts verifies that the combination which
// cannot work is refused up front. The containment root is not part of the
// worker protocol, so a coordinator-side root would apply to nothing on a remote
// worker — silently leaving the operator believing the run was confined.
func TestRunCmd_TargetRootRejectedWithHosts(t *testing.T) {
	base := t.TempDir()
	code, stdout, stderr := runRunCLI([]string{
		"--trace", filepath.Join(base, "trace.ioflux"),
		"--engine", "local",
		"--target-root", base,
		"--hosts", "127.0.0.1:7800",
		"-o", filepath.Join(base, "results.json"),
	})
	if code != 2 {
		t.Fatalf("runRun exit=%d want 2; stdout=%s stderr=%s", code, stdout, stderr)
	}
	if !strings.Contains(stderr, "not carried in the worker protocol") {
		t.Fatalf("stderr should explain why the combination is refused, got %q", stderr)
	}
	if !strings.Contains(stderr, "ioflux worker") {
		t.Fatalf("stderr should point at the worker-side flag, got %q", stderr)
	}
}

// TestRunCmd_TargetRootRejectedForNonLocalEngine verifies a root the engine
// cannot enforce is a usage error, not a silently ignored flag.
func TestRunCmd_TargetRootRejectedForNonLocalEngine(t *testing.T) {
	base := t.TempDir()
	tracePath := filepath.Join(base, "trace.ioflux")
	oneTargetReadTrace(t, tracePath, filepath.Join(base, "shard.dat"), 4096)

	code, stdout, stderr := runRunCLI([]string{
		"--trace", tracePath,
		"--engine", "mem",
		"--target-root", base,
		"-o", filepath.Join(base, "results.json"),
	})
	if code != 2 {
		t.Fatalf("runRun exit=%d want 2; stdout=%s stderr=%s", code, stdout, stderr)
	}
	if !strings.Contains(stderr, "only supported by the local engine") {
		t.Fatalf("stderr should explain the engine cannot enforce a root, got %q", stderr)
	}
}

// decodeResults reads a results.json written by a CLI run. (The integration
// build has its own readResults behind a build tag; this is the untagged twin.)
func decodeResults(t *testing.T, path string) results.Results {
	t.Helper()
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	var res results.Results
	if err := json.Unmarshal(data, &res); err != nil {
		t.Fatalf("parse %s: %v", path, err)
	}
	return res
}

// TestRunCmd_TargetRootCheckedBeforeCacheControls verifies containment is not
// limited to engine calls. With no --prepare, dataset preparation is a no-op and
// the next thing to touch targets is the cold-cache control, which opens files
// directly (os.Open + fadvise) rather than through the engine. An escaping
// target must still fail the run before anything outside the root is opened.
func TestRunCmd_TargetRootCheckedBeforeCacheControls(t *testing.T) {
	base := t.TempDir()
	root := filepath.Join(base, "scratch")
	if err := os.MkdirAll(root, 0o755); err != nil {
		t.Fatal(err)
	}
	outside := filepath.Join(base, "elsewhere.dat")
	if err := os.WriteFile(outside, make([]byte, 4096), 0o644); err != nil {
		t.Fatal(err)
	}

	tracePath := filepath.Join(base, "trace.ioflux")
	oneTargetReadTrace(t, tracePath, filepath.Join(root, "..", "elsewhere.dat"), 4096)

	code, stdout, stderr := runRunCLI([]string{
		"--trace", tracePath,
		"--engine", "local",
		"--target-root", root,
		"--cache-mode", "cold",
		"-o", filepath.Join(base, "results.json"),
	})
	if code != 1 {
		t.Fatalf("runRun exit=%d want 1; stdout=%s stderr=%s", code, stdout, stderr)
	}
	if !strings.Contains(stderr, "outside the configured root") {
		t.Fatalf("stderr should explain the containment rejection, got %q", stderr)
	}
}
