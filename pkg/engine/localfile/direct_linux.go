//go:build linux

package localfile

import (
	"errors"
	"fmt"
	"io"
	"os"
	"syscall"
	"unsafe"

	"github.com/chanuollala/ioflux/pkg/engine"
)

// openDirectFlag is OR'd into the syscall open flags when O_DIRECT is
// requested and the engine was created with WithAllowDirect(true).
const openDirectFlag = syscall.O_DIRECT

// canDirect reports whether the running platform supports O_DIRECT.
const canDirect = true

// isDirectNotSupported reports whether err indicates that O_DIRECT is not
// supported by the filesystem (EINVAL at open time, e.g. some NFS/overlayfs).
func isDirectNotSupported(err error) bool {
	var pathErr *os.PathError
	if errors.As(err, &pathErr) {
		return errors.Is(pathErr.Err, syscall.EINVAL)
	}
	return errors.Is(err, syscall.EINVAL)
}

// detectAlign returns the block alignment required for O_DIRECT on f.
// override takes precedence if > 0; otherwise we use fstat Blksize, then
// fall back to 4096.
func detectAlign(f *os.File, override int64) int64 {
	if override > 0 {
		return override
	}
	var stat syscall.Stat_t
	if err := syscall.Fstat(int(f.Fd()), &stat); err == nil && stat.Blksize > 0 {
		return int64(stat.Blksize)
	}
	return 4096
}

// alignUp rounds x up to the nearest multiple of a (a must be a power of two).
func alignUp(x, a int64) int64 {
	return (x + a - 1) &^ (a - 1)
}

// alignDown rounds x down to the nearest multiple of a (a must be a power of two).
func alignDown(x, a int64) int64 {
	return x &^ (a - 1)
}

// makeAlignedBuf allocates size bytes with its start address aligned to align
// bytes. Go zeroes new allocations, so the returned slice is already zero-filled.
func makeAlignedBuf(size, align int64) []byte {
	if align <= 1 {
		return make([]byte, size)
	}
	raw := make([]byte, size+align-1)
	addr := uintptr(unsafe.Pointer(&raw[0]))
	offset := int64(alignUp(int64(addr), align) - int64(addr))
	return raw[offset : offset+size]
}

// alignedReadAt reads length bytes at off from f into buf using O_DIRECT-safe
// aligned staging. The caller's buf must be at least length bytes long.
// Returns engine.ErrShortRead when fewer bytes are available than requested.
func alignedReadAt(f *os.File, buf []byte, off, length, align int64) (int, error) {
	alignedOff := alignDown(off, align)
	alignedEnd := alignUp(off+length, align)
	alignedLen := alignedEnd - alignedOff

	staging := makeAlignedBuf(alignedLen, align)
	n, err := f.ReadAt(staging, alignedOff)
	if err != nil && !errors.Is(err, io.EOF) {
		return 0, fmt.Errorf("localfile: direct read at %d: %w", alignedOff, err)
	}

	// Extract the requested window from the staging buffer.
	windowStart := off - alignedOff
	available := int64(n) - windowStart
	if available <= 0 {
		return 0, engine.ErrShortRead
	}
	if available > length {
		available = length
	}
	copy(buf[:available], staging[windowStart:windowStart+available])
	if available < length {
		return int(available), engine.ErrShortRead
	}
	return int(available), nil
}

// alignedWriteAt writes data at off in f using O_DIRECT-safe read-modify-write
// through an aligned staging buffer. It preserves adjacent bytes and uses
// Ftruncate to prevent alignment-rounding from leaving the file larger than its
// intended logical size.
func alignedWriteAt(f *os.File, data []byte, off, align int64) (int, error) {
	if len(data) == 0 {
		return 0, nil
	}

	// Record original size so we know the correct post-write file size.
	var origStat syscall.Stat_t
	if err := syscall.Fstat(int(f.Fd()), &origStat); err != nil {
		return 0, fmt.Errorf("localfile: direct write fstat: %w", err)
	}
	origSize := origStat.Size

	alignedOff := alignDown(off, align)
	alignedEnd := alignUp(off+int64(len(data)), align)
	alignedLen := alignedEnd - alignedOff

	// Read the full aligned block window to preserve bytes we won't overwrite.
	// makeAlignedBuf zeroes the allocation, so unread tail bytes are already
	// zero-filled (handles the case where the write extends past current EOF).
	staging := makeAlignedBuf(alignedLen, align)
	n, err := f.ReadAt(staging, alignedOff)
	if err != nil && !errors.Is(err, io.EOF) {
		// A real error on the pre-read.
		return 0, fmt.Errorf("localfile: direct write pre-read at %d: %w", alignedOff, err)
	}
	_ = n // remaining bytes (past what was read) are already zero in staging

	// Patch the requested bytes.
	windowStart := off - alignedOff
	copy(staging[windowStart:windowStart+int64(len(data))], data)

	// Write the full aligned block back.
	if _, err := f.WriteAt(staging, alignedOff); err != nil {
		return 0, fmt.Errorf("localfile: direct write at %d: %w", alignedOff, err)
	}

	// Ftruncate to max(origSize, off+len(data)) so the alignment-rounding tail
	// does not leave the file larger than intended. Always truncate when the
	// aligned write window extends past the intended logical end.
	want := origSize
	if end := off + int64(len(data)); end > want {
		want = end
	}
	if alignedEnd > want {
		if err := syscall.Ftruncate(int(f.Fd()), want); err != nil {
			return 0, fmt.Errorf("localfile: direct write ftruncate to %d: %w", want, err)
		}
	}

	return len(data), nil
}
