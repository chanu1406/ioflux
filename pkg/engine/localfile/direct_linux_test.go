//go:build linux

package localfile_test

import (
	"context"
	"os"
	"path/filepath"
	"syscall"
	"testing"

	"github.com/chanuollala/ioflux/pkg/engine"
	"github.com/chanuollala/ioflux/pkg/engine/localfile"
)

// TestDirectFlagPlatform verifies that opening with OpenFlagDirect +
// WithAllowDirect sets O_DIRECT on the file descriptor. Only meaningful on
// Linux where O_DIRECT is supported.
//
// O_DIRECT ALIGNMENT NOTE: O_DIRECT requires that buffer addresses, file
// offsets, and transfer lengths are all aligned to the block device's logical
// block size (typically 512 bytes; sometimes 4096 on newer hardware). The
// scheduler's growBuf helper allocates plain Go slices that are not guaranteed
// to be block-aligned, so actual Read/Write calls with O_DIRECT may return
// EINVAL on unaligned buffers. Current synthetic traces use record sizes >= 4
// KiB and sequential reads from offset 0, so alignment is incidental but not
// enforced. Captured traces with arbitrary offsets will need aligned-buffer
// allocation before direct I/O can be reliable.
func TestDirectFlagPlatform(t *testing.T) {
	dir := t.TempDir()
	target := filepath.Join(dir, "direct.dat")

	// O_DIRECT open succeeds only on a real file; create it with aligned content.
	aligned := make([]byte, 4096)
	if err := os.WriteFile(target, aligned, 0o644); err != nil {
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

	// fcntl(F_GETFL) to read open flags.
	flags, _, errno := syscall.Syscall(syscall.SYS_FCNTL, fd, syscall.F_GETFL, 0)
	if errno != 0 {
		t.Fatalf("fcntl(F_GETFL): %v", errno)
	}
	if flags&syscall.O_DIRECT == 0 {
		t.Errorf("O_DIRECT not set on fd %d; fcntl flags=%#x", fd, flags)
	}
}

// TestDirectFlagNotSetWhenDisabled verifies that WithAllowDirect(false) (the
// default) does NOT set O_DIRECT even when the trace flag requests it.
func TestDirectFlagNotSetWhenDisabled(t *testing.T) {
	dir := t.TempDir()
	target := filepath.Join(dir, "nodirect.dat")
	if err := os.WriteFile(target, make([]byte, 4096), 0o644); err != nil {
		t.Fatalf("WriteFile: %v", err)
	}

	eng := localfile.New() // allowDirect defaults to false
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
