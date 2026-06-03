//go:build linux

package localfile_test

import (
	"bytes"
	"context"
	"errors"
	"os"
	"path/filepath"
	"syscall"
	"testing"

	"github.com/chanuollala/ioflux/pkg/engine"
	"github.com/chanuollala/ioflux/pkg/engine/localfile"
)

// TestDirectFlagPlatform verifies that opening with OpenFlagDirect +
// WithAllowDirect sets O_DIRECT on the file descriptor.
func TestDirectFlagPlatform(t *testing.T) {
	dir := t.TempDir()
	target := filepath.Join(dir, "direct.dat")

	if err := os.WriteFile(target, make([]byte, 4096), 0o644); err != nil {
		t.Fatalf("WriteFile: %v", err)
	}

	eng := localfile.New(localfile.WithAllowDirect(true))
	ctx := context.Background()

	h, err := eng.Open(ctx, target, engine.ModeRead, engine.OpenFlagDirect)
	if err != nil {
		t.Fatalf("Open with O_DIRECT: %v", err)
	}
	defer eng.Close(ctx, h)

	fd, err := eng.FdForHandle(h)
	if err != nil {
		t.Fatalf("FdForHandle: %v", err)
	}

	flags, _, errno := syscall.Syscall(syscall.SYS_FCNTL, fd, syscall.F_GETFL, 0)
	if errno != 0 {
		t.Fatalf("fcntl(F_GETFL): %v", errno)
	}
	if flags&syscall.O_DIRECT == 0 {
		t.Errorf("O_DIRECT not set on fd %d; fcntl flags=%#x", fd, flags)
	}
}

// TestDirectFlagNotSetWhenDisabled verifies that WithAllowDirect(false) does
// NOT set O_DIRECT even when the trace flag requests it.
func TestDirectFlagNotSetWhenDisabled(t *testing.T) {
	dir := t.TempDir()
	target := filepath.Join(dir, "nodirect.dat")
	if err := os.WriteFile(target, make([]byte, 4096), 0o644); err != nil {
		t.Fatalf("WriteFile: %v", err)
	}

	eng := localfile.New()
	ctx := context.Background()

	h, err := eng.Open(ctx, target, engine.ModeRead, engine.OpenFlagDirect)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer eng.Close(ctx, h)

	fd, err := eng.FdForHandle(h)
	if err != nil {
		t.Fatalf("FdForHandle: %v", err)
	}

	flags, _, errno := syscall.Syscall(syscall.SYS_FCNTL, fd, syscall.F_GETFL, 0)
	if errno != 0 {
		t.Fatalf("fcntl(F_GETFL): %v", errno)
	}
	if flags&syscall.O_DIRECT != 0 {
		t.Errorf("O_DIRECT set despite WithAllowDirect(false); flags=%#x", flags)
	}
}

// TestDirectAlignedRead verifies that O_DIRECT reads with an unaligned
// offset/length produce correct bytes via the aligned staging path.
func TestDirectAlignedRead(t *testing.T) {
	const align = 4096
	// Create a file whose content is recognizable at each byte position.
	data := make([]byte, 3*align)
	for i := range data {
		data[i] = byte(i)
	}
	dir := t.TempDir()
	target := filepath.Join(dir, "read.dat")
	if err := os.WriteFile(target, data, 0o644); err != nil {
		t.Fatalf("WriteFile: %v", err)
	}

	eng := localfile.New(localfile.WithAllowDirect(true), localfile.WithDirectAlign(align))
	ctx := context.Background()
	h, err := eng.Open(ctx, target, engine.ModeRead, engine.OpenFlagDirect)
	if err != nil {
		t.Fatalf("Open O_DIRECT: %v", err)
	}
	defer eng.Close(ctx, h)

	// Read 100 bytes at an offset that is not block-aligned.
	const off = 17
	const length = 100
	buf := make([]byte, length)
	n, err := eng.Read(ctx, h, off, length, buf)
	if err != nil {
		t.Fatalf("Read at unaligned off=%d: %v", off, err)
	}
	if n != length {
		t.Fatalf("Read: n=%d, want %d", n, length)
	}
	if !bytes.Equal(buf, data[off:off+length]) {
		t.Errorf("Read at off=%d returned wrong bytes", off)
	}
}

// TestDirectAlignedWrite verifies that O_DIRECT writes with an unaligned
// offset/length preserve adjacent bytes via read-modify-write. It is tested
// in both ModeReadWrite and ModeWrite to cover write-only trace opens (e.g.
// checkpoint traces with O_WRONLY|O_DIRECT).
func TestDirectAlignedWrite(t *testing.T) {
	modes := []struct {
		name string
		mode engine.Mode
	}{
		{"ModeReadWrite", engine.ModeReadWrite},
		{"ModeWrite", engine.ModeWrite}, // RMW pre-read requires O_RDWR upgrade
	}
	for _, tc := range modes {
		t.Run(tc.name, func(t *testing.T) {
			const align = 4096
			data := make([]byte, 3*align)
			for i := range data {
				data[i] = byte(i % 251)
			}
			dir := t.TempDir()
			target := filepath.Join(dir, "write.dat")
			if err := os.WriteFile(target, data, 0o644); err != nil {
				t.Fatalf("WriteFile: %v", err)
			}

			eng := localfile.New(localfile.WithAllowDirect(true), localfile.WithDirectAlign(align))
			ctx := context.Background()
			h, err := eng.Open(ctx, target, tc.mode, engine.OpenFlagDirect)
			if err != nil {
				t.Fatalf("Open O_DIRECT %s: %v", tc.name, err)
			}

			const patchOff = 2000
			const patchLen = 50
			patch := bytes.Repeat([]byte{0xFF}, patchLen)
			n, err := eng.Write(ctx, h, patchOff, patch)
			if err != nil {
				t.Fatalf("Write at unaligned off=%d (%s): %v", patchOff, tc.name, err)
			}
			if n != patchLen {
				t.Fatalf("Write n=%d, want %d", n, patchLen)
			}
			if err := eng.Close(ctx, h); err != nil {
				t.Fatalf("Close: %v", err)
			}

			got, err := os.ReadFile(target)
			if err != nil {
				t.Fatalf("ReadFile: %v", err)
			}
			if len(got) != len(data) {
				t.Fatalf("file size changed: got %d, want %d", len(got), len(data))
			}
			if !bytes.Equal(got[:patchOff], data[:patchOff]) {
				t.Error("bytes before patch changed")
			}
			for i := 0; i < patchLen; i++ {
				if got[patchOff+i] != 0xFF {
					t.Errorf("patched byte [%d] = %#x, want 0xFF", patchOff+i, got[patchOff+i])
					break
				}
			}
			if !bytes.Equal(got[patchOff+patchLen:], data[patchOff+patchLen:]) {
				t.Error("bytes after patch changed")
			}
		})
	}
}

// TestDirectWriteAtEOFNoSizeGrowth verifies that a write at/near EOF does not
// leave the file larger than the intended logical size (no alignment-tail growth).
func TestDirectWriteAtEOFNoSizeGrowth(t *testing.T) {
	const align = 4096
	// A file that is not a multiple of the alignment block.
	initial := make([]byte, align+100)
	for i := range initial {
		initial[i] = byte(i)
	}
	dir := t.TempDir()
	target := filepath.Join(dir, "eof.dat")
	if err := os.WriteFile(target, initial, 0o644); err != nil {
		t.Fatalf("WriteFile: %v", err)
	}

	eng := localfile.New(localfile.WithAllowDirect(true), localfile.WithDirectAlign(align))
	ctx := context.Background()
	h, err := eng.Open(ctx, target, engine.ModeReadWrite, engine.OpenFlagDirect)
	if err != nil {
		t.Fatalf("Open O_DIRECT: %v", err)
	}

	// Write 20 bytes that straddle the end of the file (50 bytes before EOF).
	patchOff := int64(len(initial) - 50)
	patch := bytes.Repeat([]byte{0xAB}, 20)
	if _, err := eng.Write(ctx, h, patchOff, patch); err != nil {
		t.Fatalf("Write near EOF: %v", err)
	}
	if err := eng.Close(ctx, h); err != nil {
		t.Fatalf("Close: %v", err)
	}

	info, err := os.Stat(target)
	if err != nil {
		t.Fatalf("Stat: %v", err)
	}
	if info.Size() != int64(len(initial)) {
		t.Errorf("file size = %d, want %d (alignment-tail growth)", info.Size(), len(initial))
	}
}

// TestDirectFallbackOnEINVAL verifies that WithDirectFallback(true) reopens
// the file with buffered I/O and records a limitation when O_DIRECT is
// rejected by the opener.
func TestDirectFallbackOnEINVAL(t *testing.T) {
	dir := t.TempDir()
	target := filepath.Join(dir, "fallback.dat")
	if err := os.WriteFile(target, make([]byte, 4096), 0o644); err != nil {
		t.Fatalf("WriteFile: %v", err)
	}

	// Inject an opener that fails the first open with EINVAL (simulating an
	// NFS/overlayfs that rejects O_DIRECT), then succeeds on the second.
	eng := localfile.NewWithOpener(
		func(name string, flag int, perm os.FileMode) (*os.File, error) {
			if flag&syscall.O_DIRECT != 0 {
				return nil, &os.PathError{Op: "open", Path: name, Err: syscall.EINVAL}
			}
			return os.OpenFile(name, flag, perm)
		},
		localfile.WithAllowDirect(true),
		localfile.WithDirectFallback(true),
	)
	ctx := context.Background()
	h, err := eng.Open(ctx, target, engine.ModeRead, engine.OpenFlagDirect)
	if err != nil {
		t.Fatalf("Open with fallback: %v", err)
	}
	defer eng.Close(ctx, h)

	lims := eng.Limitations()
	if len(lims) == 0 {
		t.Error("expected a limitation recorded for O_DIRECT fallback, got none")
	}

	// A buffered Read should still work correctly.
	buf := make([]byte, 10)
	if _, err := eng.Read(ctx, h, 0, 10, buf); !errors.Is(err, engine.ErrShortRead) && err != nil {
		t.Errorf("Read after fallback: %v", err)
	}
}

// TestAppendFlagFdCheck verifies via fcntl(F_GETFL) that O_APPEND is not set
// on the fd when the trace OPEN carries the append flag. The behavioral check
// (WriteAt succeeds) lives in localfile_test.go for cross-platform coverage.
func TestAppendFlagFdCheck(t *testing.T) {
	dir := t.TempDir()
	target := filepath.Join(dir, "append.dat")

	eng := localfile.New()
	ctx := context.Background()

	h, err := eng.Open(ctx, target, engine.ModeWrite, engine.OpenFlagCreate|engine.OpenFlagAppend)
	if err != nil {
		t.Fatalf("Open with append flag: %v", err)
	}
	defer eng.Close(ctx, h)

	fd, err := eng.FdForHandle(h)
	if err != nil {
		t.Fatalf("FdForHandle: %v", err)
	}
	flags, _, errno := syscall.Syscall(syscall.SYS_FCNTL, fd, syscall.F_GETFL, 0)
	if errno != 0 {
		t.Fatalf("fcntl(F_GETFL): %v", errno)
	}
	if flags&syscall.O_APPEND != 0 {
		t.Errorf("O_APPEND is set on the fd (%#x); the engine must not apply it", flags)
	}
}
