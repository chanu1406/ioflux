//go:build unix

package localfile

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"sync"
	"testing"

	"github.com/chanuollala/ioflux/pkg/engine"
)

// countingPread swaps in a pread that records every call's length, so a test can
// assert how many read syscalls one engine Read costs rather than inferring it
// from the returned byte count (which cannot distinguish one read from two).
func countingPread(t *testing.T) *[]int {
	t.Helper()
	var mu sync.Mutex
	var lens []int
	orig := pread
	pread = func(fd uintptr, p []byte, off int64) (int, error) {
		mu.Lock()
		lens = append(lens, len(p))
		mu.Unlock()
		return orig(fd, p, off)
	}
	t.Cleanup(func() { pread = orig })
	return &lens
}

func readEngineFixture(t *testing.T, size int) (*LocalFileEngine, engine.Handle, string) {
	t.Helper()
	dir := t.TempDir()
	path := filepath.Join(dir, "shard.bin")
	if err := os.WriteFile(path, make([]byte, size), 0o644); err != nil {
		t.Fatalf("WriteFile: %v", err)
	}
	e := New()
	t.Cleanup(func() { _ = e.Shutdown() })
	h, err := e.Open(context.Background(), path, engine.ModeRead, engine.OpenFlagNone)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	return e, h, path
}

// A source read that came back short must cost exactly one read syscall. Before
// this was fixed the engine used File.ReadAt, which retried until the buffer was
// full and so issued a second, zero-byte read at an offset the trace never
// contained — replaying one recorded operation as two syscalls.
func TestReadShortIssuesOneSyscall(t *testing.T) {
	const fileLen, reqLen = 3000, 4096

	e, h, _ := readEngineFixture(t, fileLen)
	lens := countingPread(t)

	buf := make([]byte, reqLen)
	n, err := e.Read(context.Background(), h, 0, reqLen, buf)
	if !errors.Is(err, engine.ErrShortRead) {
		t.Fatalf("Read err = %v, want engine.ErrShortRead", err)
	}
	if n != fileLen {
		t.Errorf("Read n = %d, want %d", n, fileLen)
	}
	if got := len(*lens); got != 1 {
		t.Errorf("short read issued %d pread(2) calls (lengths %v), want exactly 1", got, *lens)
	}
	if len(*lens) > 0 && (*lens)[0] != reqLen {
		t.Errorf("pread requested %d bytes, want the source's %d", (*lens)[0], reqLen)
	}
}

// The negative case, so the assertion above keeps its meaning: a read that is
// fully satisfied was already one syscall and must stay one.
func TestReadFullIssuesOneSyscall(t *testing.T) {
	const fileLen, reqLen = 8192, 4096

	e, h, _ := readEngineFixture(t, fileLen)
	lens := countingPread(t)

	buf := make([]byte, reqLen)
	n, err := e.Read(context.Background(), h, 0, reqLen, buf)
	if err != nil {
		t.Fatalf("Read: %v", err)
	}
	if n != reqLen {
		t.Errorf("Read n = %d, want %d", n, reqLen)
	}
	if got := len(*lens); got != 1 {
		t.Errorf("full read issued %d pread(2) calls (lengths %v), want exactly 1", got, *lens)
	}
}

// A read starting at or past end-of-file transfers nothing and must still cost
// one syscall, reported as a short read rather than an I/O error.
func TestReadAtEOFIssuesOneSyscall(t *testing.T) {
	const fileLen, reqLen = 3000, 4096

	e, h, _ := readEngineFixture(t, fileLen)
	lens := countingPread(t)

	buf := make([]byte, reqLen)
	n, err := e.Read(context.Background(), h, fileLen, reqLen, buf)
	if !errors.Is(err, engine.ErrShortRead) {
		t.Fatalf("Read err = %v, want engine.ErrShortRead", err)
	}
	if n != 0 {
		t.Errorf("Read n = %d, want 0", n)
	}
	if got := len(*lens); got != 1 {
		t.Errorf("EOF read issued %d pread(2) calls (lengths %v), want exactly 1", got, *lens)
	}
}
