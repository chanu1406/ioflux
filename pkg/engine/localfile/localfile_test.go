package localfile_test

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"testing"

	"github.com/chanuollala/ioflux/pkg/engine"
	"github.com/chanuollala/ioflux/pkg/engine/localfile"
)

func TestLocalFileCaps(t *testing.T) {
	eng := localfile.New()
	caps := eng.Caps()
	if !caps.Seekable {
		t.Error("Seekable must be true")
	}
	if !caps.PartialWrite {
		t.Error("PartialWrite must be true")
	}
	if !caps.Durable {
		t.Error("Durable must be true")
	}
	if caps.ObjectAPI {
		t.Error("ObjectAPI must be false")
	}
	if caps.Multipart {
		t.Error("Multipart must be false")
	}
}

func TestLocalFileRoundTrip(t *testing.T) {
	dir := t.TempDir()
	target := filepath.Join(dir, "test.dat")
	ctx := context.Background()

	eng := localfile.New()

	// Write via ReadWrite + Create.
	h, err := eng.Open(ctx, target, engine.ModeReadWrite, engine.OpenFlagCreate)
	if err != nil {
		t.Fatalf("Open rw: %v", err)
	}
	payload := []byte("hello ioflux local file engine")
	n, err := eng.Write(ctx, h, 0, payload)
	if err != nil {
		t.Fatalf("Write: %v", err)
	}
	if n != len(payload) {
		t.Fatalf("Write: wrote %d, want %d", n, len(payload))
	}
	if err := eng.Close(ctx, h); err != nil {
		t.Fatalf("Close after write: %v", err)
	}

	// Read back.
	h2, err := eng.Open(ctx, target, engine.ModeRead, engine.OpenFlagNone)
	if err != nil {
		t.Fatalf("Open r: %v", err)
	}
	buf := make([]byte, len(payload))
	n2, err := eng.Read(ctx, h2, 0, int64(len(payload)), buf)
	if err != nil {
		t.Fatalf("Read: %v", err)
	}
	if n2 != len(payload) {
		t.Fatalf("Read: got %d bytes, want %d", n2, len(payload))
	}
	if string(buf) != string(payload) {
		t.Fatalf("Read: got %q, want %q", buf, payload)
	}
	if err := eng.Close(ctx, h2); err != nil {
		t.Fatalf("Close after read: %v", err)
	}

	// Stat.
	info, err := eng.Stat(ctx, target)
	if err != nil {
		t.Fatalf("Stat: %v", err)
	}
	if info.Size != int64(len(payload)) {
		t.Fatalf("Stat.Size=%d, want %d", info.Size, len(payload))
	}
	if info.Name != target {
		t.Fatalf("Stat.Name=%q, want %q", info.Name, target)
	}
}

// TestLocalFilePartialReadWrite verifies offset reads and writes.
func TestLocalFilePartialReadWrite(t *testing.T) {
	dir := t.TempDir()
	target := filepath.Join(dir, "partial.dat")
	ctx := context.Background()
	eng := localfile.New()

	// Create a 1 KiB file.
	h, err := eng.Open(ctx, target, engine.ModeReadWrite, engine.OpenFlagCreate|engine.OpenFlagTrunc)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	full := make([]byte, 1024)
	for i := range full {
		full[i] = byte(i)
	}
	if _, err := eng.Write(ctx, h, 0, full); err != nil {
		t.Fatalf("Write full: %v", err)
	}
	if err := eng.Close(ctx, h); err != nil {
		t.Fatalf("Close: %v", err)
	}

	// Overwrite bytes [256:512] with 0xFF.
	h2, err := eng.Open(ctx, target, engine.ModeReadWrite, engine.OpenFlagNone)
	if err != nil {
		t.Fatalf("Open rw: %v", err)
	}
	patch := make([]byte, 256)
	for i := range patch {
		patch[i] = 0xFF
	}
	if _, err := eng.Write(ctx, h2, 256, patch); err != nil {
		t.Fatalf("Write partial: %v", err)
	}

	// Read back the patched region.
	got := make([]byte, 256)
	if _, err := eng.Read(ctx, h2, 256, 256, got); err != nil {
		t.Fatalf("Read patched: %v", err)
	}
	for i, b := range got {
		if b != 0xFF {
			t.Fatalf("Read patched[%d]=%#x, want 0xFF", i, b)
		}
	}

	// Unpatched region should be unchanged.
	orig := make([]byte, 256)
	if _, err := eng.Read(ctx, h2, 0, 256, orig); err != nil {
		t.Fatalf("Read unpatched: %v", err)
	}
	for i, b := range orig {
		if b != byte(i) {
			t.Fatalf("Read unpatched[%d]=%#x, want %#x", i, b, byte(i))
		}
	}
	if err := eng.Close(ctx, h2); err != nil {
		t.Fatalf("Close h2: %v", err)
	}
}

func TestLocalFileFsync(t *testing.T) {
	dir := t.TempDir()
	target := filepath.Join(dir, "fsync.dat")
	ctx := context.Background()
	eng := localfile.New()

	h, err := eng.Open(ctx, target, engine.ModeWrite, engine.OpenFlagCreate)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	if _, err := eng.Write(ctx, h, 0, []byte("fsync test")); err != nil {
		t.Fatalf("Write: %v", err)
	}
	if err := eng.Fsync(ctx, h); err != nil {
		t.Fatalf("Fsync: %v", err)
	}
	if err := eng.Close(ctx, h); err != nil {
		t.Fatalf("Close: %v", err)
	}
}

func TestLocalFileRejectsBadHandle(t *testing.T) {
	eng := localfile.New()
	ctx := context.Background()
	bad := engine.Handle(9999)

	if _, err := eng.Read(ctx, bad, 0, 10, make([]byte, 10)); !errors.Is(err, engine.ErrNotFound) {
		t.Errorf("Read bad handle: got %v, want ErrNotFound", err)
	}
	if _, err := eng.Write(ctx, bad, 0, []byte("x")); !errors.Is(err, engine.ErrNotFound) {
		t.Errorf("Write bad handle: got %v, want ErrNotFound", err)
	}
	if err := eng.Fsync(ctx, bad); !errors.Is(err, engine.ErrNotFound) {
		t.Errorf("Fsync bad handle: got %v, want ErrNotFound", err)
	}
	if err := eng.Close(ctx, bad); !errors.Is(err, engine.ErrNotFound) {
		t.Errorf("Close bad handle: got %v, want ErrNotFound", err)
	}
}

func TestLocalFileStatNotFound(t *testing.T) {
	eng := localfile.New()
	ctx := context.Background()

	_, err := eng.Stat(ctx, "/nonexistent-ioflux-target-that-does-not-exist")
	if !errors.Is(err, engine.ErrNotFound) {
		t.Fatalf("Stat missing: got %v, want ErrNotFound", err)
	}
}

func TestLocalFileObjectOpsUnsupported(t *testing.T) {
	eng := localfile.New()
	ctx := context.Background()

	if err := eng.Put(ctx, "k", nil, 0); !errors.Is(err, engine.ErrUnsupported) {
		t.Errorf("Put: got %v, want ErrUnsupported", err)
	}
	if _, err := eng.Get(ctx, "k", 0, 0, nil); !errors.Is(err, engine.ErrUnsupported) {
		t.Errorf("Get: got %v, want ErrUnsupported", err)
	}
	if _, err := eng.Head(ctx, "k"); !errors.Is(err, engine.ErrUnsupported) {
		t.Errorf("Head: got %v, want ErrUnsupported", err)
	}
	if err := eng.Delete(ctx, "k"); !errors.Is(err, engine.ErrUnsupported) {
		t.Errorf("Delete: got %v, want ErrUnsupported", err)
	}
}

// TestLocalFileShortReadAtEOF verifies that reading past EOF returns ErrShortRead.
func TestLocalFileShortReadAtEOF(t *testing.T) {
	dir := t.TempDir()
	target := filepath.Join(dir, "small.dat")
	ctx := context.Background()

	if err := os.WriteFile(target, []byte("hi"), 0o644); err != nil {
		t.Fatal(err)
	}
	eng := localfile.New()
	h, err := eng.Open(ctx, target, engine.ModeRead, engine.OpenFlagNone)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer eng.Close(ctx, h)

	buf := make([]byte, 100)
	n, err := eng.Read(ctx, h, 0, 100, buf)
	if !errors.Is(err, engine.ErrShortRead) {
		t.Fatalf("Read past EOF: got (%d, %v), want ErrShortRead", n, err)
	}
	if n != 2 {
		t.Fatalf("Read: n=%d, want 2", n)
	}
}

// TestLocalFileConcurrentReads verifies that multiple goroutines can read the
// same file concurrently without data races.
func TestLocalFileConcurrentReads(t *testing.T) {
	dir := t.TempDir()
	target := filepath.Join(dir, "concurrent.dat")
	const fileSize = 64 * 1024
	data := make([]byte, fileSize)
	for i := range data {
		data[i] = byte(i)
	}
	if err := os.WriteFile(target, data, 0o644); err != nil {
		t.Fatal(err)
	}

	ctx := context.Background()
	eng := localfile.New()

	const goroutines = 16
	errs := make(chan error, goroutines)
	for i := 0; i < goroutines; i++ {
		go func(off int64) {
			h, err := eng.Open(ctx, target, engine.ModeRead, engine.OpenFlagNone)
			if err != nil {
				errs <- err
				return
			}
			defer eng.Close(ctx, h)
			buf := make([]byte, 1024)
			_, err = eng.Read(ctx, h, off%int64(fileSize-1024), 1024, buf)
			if err != nil && !errors.Is(err, engine.ErrShortRead) {
				errs <- err
				return
			}
			errs <- nil
		}(int64(i * 1024))
	}
	for i := 0; i < goroutines; i++ {
		if err := <-errs; err != nil {
			t.Errorf("concurrent read error: %v", err)
		}
	}
}
