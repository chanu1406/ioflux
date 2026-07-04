package replay_test

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"sync"
	"testing"

	s3engine "github.com/chanuollala/ioflux/pkg/engine/s3"
	"github.com/chanuollala/ioflux/pkg/gen/checkpoint"
	"github.com/chanuollala/ioflux/pkg/replay"
	"github.com/chanuollala/ioflux/pkg/trace"
)

// fakeS3PutServer is a minimal S3-compatible HTTP server that records the full
// object body for every key it receives, whether delivered as a single PUT or
// reassembled from a multipart upload. It exists to prove that the replay
// executor's object-level coalesced-write dispatch (objectwrite.go) actually
// streams a handle's WRITE sequence into one correct Put call, without relying
// on the s3 package's own (already-tested) multipart unit tests.
type fakeS3PutServer struct {
	mu          sync.Mutex
	bodies      map[string][]byte // key -> full body, once known complete
	partsByKey  map[string]map[int][]byte
	uploadIDSeq int
}

func newFakeS3PutServer() *fakeS3PutServer {
	return &fakeS3PutServer{
		bodies:     make(map[string][]byte),
		partsByKey: make(map[string]map[int][]byte),
	}
}

func (f *fakeS3PutServer) body(key string) ([]byte, bool) {
	f.mu.Lock()
	defer f.mu.Unlock()
	b, ok := f.bodies[key]
	return b, ok
}

func (f *fakeS3PutServer) handler() http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		key := r.URL.Path // path-style: /bucket/key...; bucket/key split not needed, key uniqueness suffices per-path
		switch {
		case r.Method == http.MethodPost && hasQuery(r, "uploads"):
			f.mu.Lock()
			f.uploadIDSeq++
			uploadID := fmt.Sprintf("up-%d", f.uploadIDSeq)
			f.partsByKey[uploadID] = make(map[int][]byte)
			f.mu.Unlock()
			w.Header().Set("Content-Type", "application/xml")
			fmt.Fprintf(w, `<CreateMultipartUploadResult><UploadId>%s</UploadId></CreateMultipartUploadResult>`, uploadID)

		case r.Method == http.MethodPut && r.URL.Query().Get("uploadId") != "":
			uploadID := r.URL.Query().Get("uploadId")
			partNo := 0
			fmt.Sscanf(r.URL.Query().Get("partNumber"), "%d", &partNo)
			data, err := io.ReadAll(r.Body)
			if err != nil {
				http.Error(w, err.Error(), http.StatusInternalServerError)
				return
			}
			f.mu.Lock()
			f.partsByKey[uploadID][partNo] = data
			f.mu.Unlock()
			w.Header().Set("ETag", fmt.Sprintf(`"part-%d"`, partNo))
			w.WriteHeader(http.StatusOK)

		case r.Method == http.MethodPost && r.URL.Query().Get("uploadId") != "":
			uploadID := r.URL.Query().Get("uploadId")
			f.mu.Lock()
			parts := f.partsByKey[uploadID]
			var full []byte
			for i := 1; i <= len(parts); i++ {
				full = append(full, parts[i]...)
			}
			f.bodies[key] = full
			f.mu.Unlock()
			w.Header().Set("Content-Type", "application/xml")
			fmt.Fprintf(w, `<CompleteMultipartUploadResult><ETag>"done"</ETag></CompleteMultipartUploadResult>`)

		case r.Method == http.MethodPut:
			data, err := io.ReadAll(r.Body)
			if err != nil {
				http.Error(w, err.Error(), http.StatusInternalServerError)
				return
			}
			f.mu.Lock()
			f.bodies[key] = data
			f.mu.Unlock()
			w.WriteHeader(http.StatusOK)

		default:
			http.Error(w, "unexpected request", http.StatusBadRequest)
		}
	}
}

func hasQuery(r *http.Request, key string) bool {
	_, ok := r.URL.Query()[key]
	return ok
}

// TestObjectLevelCoalescedWriteReplay generates a checkpoint-write trace and
// replays it against a fake S3 server, verifying end to end that: PREPARE
// accepts the write-shaped trace instead of rejecting it, the coalesced PUT
// (routed through multipart, since the shard exceeds the configured
// threshold) reassembles to exactly the declared shard size, and the result
// records replay_equivalence as "object-level".
func TestObjectLevelCoalescedWriteReplay(t *testing.T) {
	srv := newFakeS3PutServer()
	ts := httptest.NewServer(srv.handler())
	defer ts.Close()

	p := checkpoint.DefaultParams()
	p.ModelSize = 12 << 20 // 12 MiB, one shard
	p.WriterRanks = 1
	p.WriteBlock = 1 << 20 // 1 MiB per WRITE op -> 12 WRITE ops
	p.NumCheckpoints = 1
	p.Fsync = checkpoint.FsyncPerFile
	var buf bytes.Buffer
	if err := checkpoint.Generate(p, &buf); err != nil {
		t.Fatalf("Generate: %v", err)
	}

	r, err := trace.NewReader(&buf)
	if err != nil {
		t.Fatalf("NewReader: %v", err)
	}

	eng, err := s3engine.New(s3engine.Config{
		Endpoint:           ts.URL,
		Region:             "us-east-1",
		Bucket:             "bench",
		PathStyle:          true,
		AccessKey:          "test-access",
		SecretKey:          "test-secret",
		MultipartThreshold: 8 << 20, // shard (12MiB) exceeds this -> multipart engaged
		MultipartPartSize:  5 << 20, // S3 minimum part size -> parts of 5MiB, 5MiB, 2MiB
	})
	if err != nil {
		t.Fatalf("s3.New: %v", err)
	}
	if caps := eng.Caps(); !caps.ObjectAPI || caps.PartialWrite {
		t.Fatalf("caps=%+v, want ObjectAPI=true PartialWrite=false", caps)
	}

	plan := replay.Plan{
		Engine:     eng,
		EngineName: "s3",
		Mode:       "asap",
		FillMode:   "zero", // deterministic all-zero payload, trivial to verify
	}
	exec, err := replay.Prepare(plan, r)
	if err != nil {
		t.Fatalf("Prepare should accept a fully-covered sequential checkpoint-write trace: %v", err)
	}

	res, err := exec.Run(context.Background())
	if err != nil {
		t.Fatalf("Run: %v", err)
	}
	if res.Errors != 0 {
		t.Errorf("Errors=%d, want 0", res.Errors)
	}
	if res.Plan.ReplayEquivalence != "object-level" {
		t.Errorf("ReplayEquivalence=%q, want object-level", res.Plan.ReplayEquivalence)
	}

	key := "/bench/checkpoint_0000/shard_0000.pt"
	body, ok := srv.body(key)
	if !ok {
		t.Fatalf("server never received a complete object for key %q", key)
	}
	if int64(len(body)) != p.ModelSize {
		t.Fatalf("assembled object size=%d, want %d", len(body), p.ModelSize)
	}
	for i, b := range body {
		if b != 0 {
			t.Fatalf("byte %d = %d, want 0 (FillMode=zero)", i, b)
		}
	}
}

// TestObjectLevelCoalescedWriteReplaySingleShard verifies the below-threshold
// (single PutObject, non-multipart) path through the same coalesced-write
// dispatch, so both branches of S3Engine.Put are exercised via replay.
func TestObjectLevelCoalescedWriteReplaySingleShard(t *testing.T) {
	srv := newFakeS3PutServer()
	ts := httptest.NewServer(srv.handler())
	defer ts.Close()

	p := checkpoint.DefaultParams()
	p.ModelSize = 4 << 10 // 4 KiB, well under any multipart threshold
	p.WriterRanks = 1
	p.WriteBlock = 1 << 10 // 1 KiB per WRITE op -> 4 WRITE ops
	p.NumCheckpoints = 1
	p.Fsync = checkpoint.FsyncNone
	var buf bytes.Buffer
	if err := checkpoint.Generate(p, &buf); err != nil {
		t.Fatalf("Generate: %v", err)
	}

	r, err := trace.NewReader(&buf)
	if err != nil {
		t.Fatalf("NewReader: %v", err)
	}

	eng, err := s3engine.New(s3engine.Config{
		Endpoint:  ts.URL,
		Region:    "us-east-1",
		Bucket:    "bench",
		PathStyle: true,
		AccessKey: "test-access",
		SecretKey: "test-secret",
	})
	if err != nil {
		t.Fatalf("s3.New: %v", err)
	}

	plan := replay.Plan{Engine: eng, EngineName: "s3", Mode: "asap", FillMode: "zero"}
	exec, err := replay.Prepare(plan, r)
	if err != nil {
		t.Fatalf("Prepare: %v", err)
	}
	res, err := exec.Run(context.Background())
	if err != nil {
		t.Fatalf("Run: %v", err)
	}
	if res.Errors != 0 {
		t.Errorf("Errors=%d, want 0", res.Errors)
	}

	body, ok := srv.body("/bench/checkpoint_0000/shard_0000.pt")
	if !ok {
		t.Fatal("server never received the object")
	}
	if int64(len(body)) != p.ModelSize {
		t.Fatalf("object size=%d, want %d", len(body), p.ModelSize)
	}
}
