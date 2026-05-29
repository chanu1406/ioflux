//go:build integration

package s3_test

import (
	"bytes"
	"context"
	"os"
	"testing"
	"time"

	s3engine "github.com/chanuollala/ioflux/pkg/engine/s3"
)

func TestMinIORoundTrip(t *testing.T) {
	endpoint := os.Getenv("IOFLUX_MINIO_ENDPOINT")
	if endpoint == "" {
		t.Skip("IOFLUX_MINIO_ENDPOINT is not set")
	}
	bucket := os.Getenv("IOFLUX_MINIO_BUCKET")
	if bucket == "" {
		bucket = "bench"
	}

	cfg := s3engine.Config{
		Endpoint:  endpoint,
		Region:    getenv("IOFLUX_MINIO_REGION", "us-east-1"),
		Bucket:    bucket,
		PathStyle: true,
		AccessKey: getenv("IOFLUX_MINIO_ACCESS_KEY", "minioadmin"),
		SecretKey: getenv("IOFLUX_MINIO_SECRET_KEY", "minioadmin"),
	}
	eng, err := s3engine.New(cfg)
	if err != nil {
		t.Fatalf("New: %v", err)
	}

	key := "ioflux-integration-" + time.Now().UTC().Format("20060102T150405.000000000")
	body := []byte("ioflux minio round trip")
	ctx := context.Background()

	if err := eng.Put(ctx, key, bytes.NewReader(body), int64(len(body))); err != nil {
		t.Fatalf("Put: %v", err)
	}
	t.Cleanup(func() { _ = eng.Delete(context.Background(), key) })

	info, err := eng.Head(ctx, key)
	if err != nil {
		t.Fatalf("Head: %v", err)
	}
	if info.Size != int64(len(body)) {
		t.Fatalf("Head size=%d, want %d", info.Size, len(body))
	}

	buf := make([]byte, 6)
	n, err := eng.Get(ctx, key, 7, 6, buf)
	if err != nil {
		t.Fatalf("Get: %v", err)
	}
	if n != 6 || string(buf) != "minio " {
		t.Fatalf("Get got n=%d body=%q, want 6 %q", n, buf, "minio ")
	}

	if err := eng.Delete(ctx, key); err != nil {
		t.Fatalf("Delete: %v", err)
	}
}

func getenv(k, fallback string) string {
	if v := os.Getenv(k); v != "" {
		return v
	}
	return fallback
}
