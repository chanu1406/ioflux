package s3_test

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"strconv"
	"strings"
	"sync"
	"testing"

	"github.com/chanuollala/ioflux/pkg/engine"
	s3engine "github.com/chanuollala/ioflux/pkg/engine/s3"
)

func testConfig(endpoint string) s3engine.Config {
	return s3engine.Config{
		Endpoint:  endpoint,
		Region:    "us-east-1",
		Bucket:    "bench",
		PathStyle: true,
		AccessKey: "test-access",
		SecretKey: "test-secret",
	}
}

func TestCaps(t *testing.T) {
	eng, err := s3engine.New(testConfig("http://127.0.0.1:1"))
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	caps := eng.Caps()
	if !caps.Seekable {
		t.Error("Seekable must be true for range GETs")
	}
	if caps.PartialWrite {
		t.Error("PartialWrite must be false for S3")
	}
	if caps.Durable {
		t.Error("Durable must be false for S3")
	}
	if !caps.ObjectAPI {
		t.Error("ObjectAPI must be true")
	}
	if !caps.Multipart {
		t.Error("Multipart must be true")
	}
	if caps.OSPageCache {
		t.Error("OSPageCache must be false")
	}
}

func TestNewValidation(t *testing.T) {
	if _, err := s3engine.New(s3engine.Config{}); err == nil {
		t.Fatal("New should require Bucket")
	}
	if _, err := s3engine.New(s3engine.Config{
		Bucket:    "bench",
		AccessKey: "only-access",
	}); err == nil {
		t.Fatal("New should require access and secret keys together")
	}
	if _, err := s3engine.New(s3engine.Config{
		Bucket:            "bench",
		MultipartPartSize: 1024,
	}); err == nil {
		t.Fatal("New should reject multipart part size below S3 minimum")
	}
}

func TestSigningAndEndpoint(t *testing.T) {
	var gotMethod, gotPath, gotAuth string
	var gotBody []byte
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		gotMethod = r.Method
		gotPath = r.URL.Path
		gotAuth = r.Header.Get("Authorization")
		var err error
		gotBody, err = io.ReadAll(r.Body)
		if err != nil {
			t.Errorf("ReadAll body: %v", err)
		}
		w.Header().Set("ETag", `"single"`)
		w.WriteHeader(http.StatusOK)
	}))
	defer srv.Close()

	eng, err := s3engine.New(testConfig(srv.URL))
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	if err := eng.Put(context.Background(), "imagenet/shard_0001.tar", bytes.NewReader([]byte("abc")), 3); err != nil {
		t.Fatalf("Put: %v", err)
	}

	if gotMethod != http.MethodPut {
		t.Errorf("method=%q, want PUT", gotMethod)
	}
	if gotPath != "/bench/imagenet/shard_0001.tar" {
		t.Errorf("path=%q, want /bench/imagenet/shard_0001.tar", gotPath)
	}
	if !strings.Contains(gotAuth, "AWS4-HMAC-SHA256") {
		t.Errorf("Authorization=%q, want SigV4 header", gotAuth)
	}
	if string(gotBody) != "abc" {
		t.Errorf("body=%q, want abc", gotBody)
	}
}

func TestPutAcceptsNonSeekableReader(t *testing.T) {
	var gotBody []byte
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var err error
		gotBody, err = io.ReadAll(r.Body)
		if err != nil {
			t.Errorf("ReadAll body: %v", err)
		}
		w.Header().Set("ETag", `"single"`)
		w.WriteHeader(http.StatusOK)
	}))
	defer srv.Close()

	eng, err := s3engine.New(testConfig(srv.URL))
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	src := io.LimitReader(strings.NewReader("streamed"), 8)
	if err := eng.Put(context.Background(), "streamed.dat", src, 8); err != nil {
		t.Fatalf("Put non-seekable: %v", err)
	}
	if string(gotBody) != "streamed" {
		t.Fatalf("body=%q, want streamed", gotBody)
	}
}

func TestRangeReadViaOpen(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet {
			t.Errorf("method=%s, want GET", r.Method)
		}
		if r.URL.Path != "/bench/imagenet/shard.tar" {
			t.Errorf("path=%q, want /bench/imagenet/shard.tar", r.URL.Path)
		}
		if got := r.Header.Get("Range"); got != "bytes=3-5" {
			t.Errorf("Range=%q, want bytes=3-5", got)
		}
		w.Header().Set("Content-Length", "3")
		w.WriteHeader(http.StatusPartialContent)
		_, _ = w.Write([]byte("def"))
	}))
	defer srv.Close()

	eng, err := s3engine.New(testConfig(srv.URL))
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	h, err := eng.Open(context.Background(), "imagenet/shard.tar", engine.ModeRead, engine.OpenFlagNone)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	buf := make([]byte, 3)
	n, err := eng.Read(context.Background(), h, 3, 3, buf)
	if err != nil {
		t.Fatalf("Read: %v", err)
	}
	if n != 3 || string(buf) != "def" {
		t.Fatalf("Read got n=%d body=%q, want 3 def", n, buf)
	}
	if err := eng.Close(context.Background(), h); err != nil {
		t.Fatalf("Close: %v", err)
	}
}

func TestHeadGetDelete(t *testing.T) {
	const body = "hello-range-body"
	var sawDelete bool
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.Method {
		case http.MethodHead:
			w.Header().Set("Content-Length", strconv.Itoa(len(body)))
			w.WriteHeader(http.StatusOK)
		case http.MethodGet:
			if got := r.Header.Get("Range"); got != "bytes=6-10" {
				t.Errorf("Range=%q, want bytes=6-10", got)
			}
			w.WriteHeader(http.StatusPartialContent)
			_, _ = w.Write([]byte(body[6:11]))
		case http.MethodDelete:
			sawDelete = true
			w.WriteHeader(http.StatusNoContent)
		default:
			t.Errorf("unexpected method %s", r.Method)
			w.WriteHeader(http.StatusMethodNotAllowed)
		}
	}))
	defer srv.Close()

	eng, err := s3engine.New(testConfig(srv.URL))
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	info, err := eng.Head(context.Background(), "obj")
	if err != nil {
		t.Fatalf("Head: %v", err)
	}
	if info.Name != "obj" || info.Size != int64(len(body)) {
		t.Fatalf("Head info=%+v, want name obj size %d", info, len(body))
	}
	buf := make([]byte, 5)
	n, err := eng.Get(context.Background(), "obj", 6, 5, buf)
	if err != nil {
		t.Fatalf("Get: %v", err)
	}
	if n != 5 || string(buf) != body[6:11] {
		t.Fatalf("Get got n=%d body=%q", n, buf)
	}
	if err := eng.Delete(context.Background(), "obj"); err != nil {
		t.Fatalf("Delete: %v", err)
	}
	if !sawDelete {
		t.Fatal("server did not see DELETE")
	}
}

func TestNotFoundMapping(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusNotFound)
	}))
	defer srv.Close()

	eng, err := s3engine.New(testConfig(srv.URL))
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	if _, err := eng.Head(context.Background(), "missing"); !errors.Is(err, engine.ErrNotFound) {
		t.Fatalf("Head missing err=%v, want ErrNotFound", err)
	}
}

func TestMultipartSwitchover(t *testing.T) {
	var mu sync.Mutex
	var createSeen, completeSeen bool
	var uploadPartSizes []int
	var singlePutSeen bool

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		mu.Lock()
		defer mu.Unlock()

		switch {
		case r.Method == http.MethodPost && hasQueryKey(r, "uploads"):
			createSeen = true
			w.Header().Set("Content-Type", "application/xml")
			_, _ = fmt.Fprint(w, `<CreateMultipartUploadResult><Bucket>bench</Bucket><Key>big.dat</Key><UploadId>upload-1</UploadId></CreateMultipartUploadResult>`)
		case r.Method == http.MethodPut && r.URL.Query().Get("uploadId") == "upload-1":
			data, err := io.ReadAll(r.Body)
			if err != nil {
				t.Errorf("ReadAll upload body: %v", err)
			}
			uploadPartSizes = append(uploadPartSizes, len(data))
			partNo := r.URL.Query().Get("partNumber")
			w.Header().Set("ETag", fmt.Sprintf(`"part-%s"`, partNo))
			w.WriteHeader(http.StatusOK)
		case r.Method == http.MethodPost && r.URL.Query().Get("uploadId") == "upload-1":
			completeSeen = true
			body, err := io.ReadAll(r.Body)
			if err != nil {
				t.Errorf("ReadAll complete body: %v", err)
			}
			if !bytes.Contains(body, []byte("<PartNumber>1</PartNumber>")) ||
				!bytes.Contains(body, []byte("<PartNumber>2</PartNumber>")) {
				t.Errorf("complete XML missing parts: %s", body)
			}
			w.Header().Set("Content-Type", "application/xml")
			_, _ = fmt.Fprint(w, `<CompleteMultipartUploadResult><Bucket>bench</Bucket><Key>big.dat</Key><ETag>"done"</ETag></CompleteMultipartUploadResult>`)
		case r.Method == http.MethodPut:
			singlePutSeen = true
			w.WriteHeader(http.StatusOK)
		default:
			t.Errorf("unexpected request %s %s", r.Method, r.URL.String())
			w.WriteHeader(http.StatusBadRequest)
		}
	}))
	defer srv.Close()

	cfg := testConfig(srv.URL)
	cfg.MultipartThreshold = 1 << 20
	cfg.MultipartPartSize = 5 << 20
	eng, err := s3engine.New(cfg)
	if err != nil {
		t.Fatalf("New: %v", err)
	}

	payload := bytes.Repeat([]byte{7}, 6<<20)
	if err := eng.Put(context.Background(), "big.dat", bytes.NewReader(payload), int64(len(payload))); err != nil {
		t.Fatalf("Put multipart: %v", err)
	}

	mu.Lock()
	defer mu.Unlock()
	if !createSeen {
		t.Error("CreateMultipartUpload was not called")
	}
	if !completeSeen {
		t.Error("CompleteMultipartUpload was not called")
	}
	if singlePutSeen {
		t.Error("single PutObject was called for multipart-sized object")
	}
	if len(uploadPartSizes) != 2 {
		t.Fatalf("upload parts=%d, want 2", len(uploadPartSizes))
	}
	if uploadPartSizes[0] != 5<<20 || uploadPartSizes[1] != 1<<20 {
		t.Fatalf("part sizes=%v, want [5MiB 1MiB]", uploadPartSizes)
	}
}

func hasQueryKey(r *http.Request, key string) bool {
	_, ok := r.URL.Query()[key]
	return ok
}
