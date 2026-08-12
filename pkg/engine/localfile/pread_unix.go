//go:build unix

package localfile

import (
	"errors"
	"io"
	"os"

	"golang.org/x/sys/unix"
)

// pread issues exactly one pread(2). It is a variable so tests can count how
// many syscalls one engine Read costs; production always uses unix.Pread.
var pread = func(fd uintptr, p []byte, off int64) (int, error) {
	return unix.Pread(int(fd), p, off)
}

// readAtOnce reads into buf at off with a single pread(2).
//
// os.File.ReadAt retries until buf is full, so a short source read would cost a
// second zero-byte read at an offset the trace never contained, replaying one
// recorded operation as two syscalls.
//
// A partial result is returned as-is; the caller converts it to
// engine.ErrShortRead. EINTR is retried, since it means no bytes moved.
func readAtOnce(f *os.File, buf []byte, off int64) (int, error) {
	rc, err := f.SyscallConn()
	if err != nil {
		return 0, err
	}
	var n int
	var readErr error
	if cerr := rc.Control(func(fd uintptr) {
		for {
			n, readErr = pread(fd, buf, off)
			if !errors.Is(readErr, unix.EINTR) {
				return
			}
		}
	}); cerr != nil {
		return 0, cerr
	}
	if readErr != nil {
		return 0, readErr
	}
	if n == 0 && len(buf) > 0 {
		return 0, io.EOF
	}
	return n, nil
}
