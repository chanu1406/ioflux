// Test-only exports: compiled only during `go test`.

package localfile

import "github.com/chanuollala/ioflux/pkg/engine"

// FdForHandle returns the underlying file descriptor for h. Only for use in
// tests that need to inspect fd-level properties (e.g. O_DIRECT via
// fcntl(F_GETFL)). Note: calling this puts the *os.File into blocking mode
// (Go runtime constraint), but that is harmless for pread/pwrite paths.
func (e *LocalFileEngine) FdForHandle(h engine.Handle) (uintptr, error) {
	f, err := e.lookupHandle(h)
	if err != nil {
		return 0, err
	}
	return f.Fd(), nil
}
