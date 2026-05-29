package main

import (
	"bytes"
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
