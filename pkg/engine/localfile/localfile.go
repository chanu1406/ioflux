// Package localfile provides LocalFileEngine, a local-filesystem storage engine.
//
// LocalFileEngine opens files via os.OpenFile and uses File.ReadAt /
// File.WriteAt so concurrent operations on different handles are safe without
// per-file locking. It is safe for concurrent use by multiple goroutines.
package localfile

import (
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sync"
	"sync/atomic"

	"github.com/chanuollala/ioflux/pkg/engine"
)

// openFile bundles a file with its O_DIRECT state so Read/Write can use
// aligned staging paths when direct I/O is active.
type openFile struct {
	f      *os.File
	direct bool
	align  int64
}

// openerFunc is the function used to open files. It defaults to os.OpenFile
// and can be overridden in tests to simulate filesystem behavior.
type openerFunc func(name string, flag int, perm os.FileMode) (*os.File, error)

// LocalFileEngine is a local-filesystem storage engine.
type LocalFileEngine struct {
	mu          sync.Mutex
	handles     map[engine.Handle]*openFile
	nextH       atomic.Int64
	allowDirect bool
	// directFallback: if true, fall back to buffered I/O when O_DIRECT is
	// unsupported by the filesystem rather than returning an error.
	directFallback      bool
	directAlignOverride int64 // 0 = auto-detect from filesystem
	opener              openerFunc

	limitMu     sync.Mutex
	limitations []string
}

// Option configures a LocalFileEngine.
type Option func(*LocalFileEngine)

// WithAllowDirect enables O_DIRECT when a trace OPEN carries the "direct"
// flag. Silently ignored on platforms where O_DIRECT is unavailable (non-Linux).
func WithAllowDirect(b bool) Option {
	return func(e *LocalFileEngine) { e.allowDirect = b }
}

// WithDirectFallback controls what happens when a filesystem rejects O_DIRECT
// (EINVAL at open time): if true, the engine falls back to buffered I/O and
// records the limitation; if false (the default), it returns an error.
func WithDirectFallback(b bool) Option {
	return func(e *LocalFileEngine) { e.directFallback = b }
}

// WithDirectAlign overrides the block-alignment size used for O_DIRECT I/O.
// 0 (the default) auto-detects from filesystem/device metadata.
func WithDirectAlign(n int64) Option {
	return func(e *LocalFileEngine) { e.directAlignOverride = n }
}

// New returns a new LocalFileEngine.
func New(opts ...Option) *LocalFileEngine {
	e := &LocalFileEngine{
		handles: make(map[engine.Handle]*openFile),
		opener:  os.OpenFile,
	}
	for _, opt := range opts {
		opt(e)
	}
	return e
}

// NewWithOpener returns a LocalFileEngine that uses openFn to open files.
// This is intended for tests that need to simulate filesystem behavior (e.g.,
// an NFS mount that rejects O_DIRECT with EINVAL).
func NewWithOpener(openFn openerFunc, opts ...Option) *LocalFileEngine {
	e := New(opts...)
	e.opener = openFn
	return e
}

// Limitations returns limitation strings accumulated during the run. A
// non-empty slice indicates that some requested engine behavior (e.g. O_DIRECT)
// was not honored; the caller should surface these in run metadata.
func (e *LocalFileEngine) Limitations() []string {
	e.limitMu.Lock()
	defer e.limitMu.Unlock()
	if len(e.limitations) == 0 {
		return nil
	}
	out := make([]string, len(e.limitations))
	copy(out, e.limitations)
	return out
}

func (e *LocalFileEngine) addLimitation(s string) {
	e.limitMu.Lock()
	e.limitations = append(e.limitations, s)
	e.limitMu.Unlock()
}

// Caps returns local-file capabilities: seekable, partial-write, durable; no
// object API.
func (e *LocalFileEngine) Caps() engine.Capabilities {
	return engine.Capabilities{
		Seekable:     true,
		PartialWrite: true,
		Durable:      true,
		ObjectAPI:    false,
		Multipart:    false,
		OSPageCache:  true,
	}
}

// Open opens target for the given mode and flags. target must be the full path
// to the file (absolute or relative to the process working directory).
//
// The "append" flag is intentionally not applied: replay uses offset-addressed
// WriteAt, and Go's WriteAt returns an error on a file opened with O_APPEND.
// The flag is preserved in the trace IR but treated as unmodeled by the engine.
func (e *LocalFileEngine) Open(_ context.Context, target string, mode engine.Mode, flags engine.OpenFlags) (engine.Handle, error) {
	oflags := modeToOsFlag(mode)
	if flags&engine.OpenFlagCreate != 0 {
		oflags |= os.O_CREATE
	}
	if flags&engine.OpenFlagTrunc != 0 {
		oflags |= os.O_TRUNC
	}
	// OpenFlagAppend is NOT applied: replay is offset-addressed via WriteAt,
	// which returns an error on O_APPEND-opened files. "append" is unmodeled.
	if flags&engine.OpenFlagSync != 0 {
		oflags |= os.O_SYNC
	}

	wantDirect := flags&engine.OpenFlagDirect != 0 && e.allowDirect && canDirect
	actualDirect := wantDirect
	if wantDirect {
		oflags |= openDirectFlag
		// The aligned-write path does a read-modify-write pre-read, which requires
		// read access on the fd. Upgrade O_WRONLY to O_RDWR so the pre-read
		// succeeds for write-only trace opens (e.g. checkpoint traces with
		// O_WRONLY|O_DIRECT). This does not affect replay semantics: all writes
		// are still offset-addressed, and no reads are issued against write targets.
		if mode == engine.ModeWrite {
			oflags = (oflags &^ (os.O_WRONLY | os.O_RDWR)) | os.O_RDWR
		}
	}

	if flags&engine.OpenFlagCreate != 0 {
		if dir := filepath.Dir(target); dir != "" && dir != "." {
			if err := os.MkdirAll(dir, 0o755); err != nil {
				return 0, fmt.Errorf("localfile: mkdir %s: %w", dir, err)
			}
		}
	}

	f, err := e.opener(target, oflags, 0o644)
	if err != nil {
		if wantDirect && isDirectNotSupported(err) {
			if !e.directFallback {
				return 0, fmt.Errorf("localfile: open %s with O_DIRECT: filesystem does not support direct I/O: %w", target, err)
			}
			// Fall back to buffered I/O and record the limitation.
			oflags &^= openDirectFlag
			f, err = e.opener(target, oflags, 0o644)
			if err != nil {
				return 0, fmt.Errorf("localfile: open %s: %w", target, err)
			}
			actualDirect = false
			e.addLimitation(fmt.Sprintf("O_DIRECT not supported by filesystem for %s; fell back to buffered I/O", target))
		} else {
			return 0, fmt.Errorf("localfile: open %s: %w", target, err)
		}
	}

	var align int64
	if actualDirect {
		align = detectAlign(f, e.directAlignOverride)
	}

	h := engine.Handle(e.nextH.Add(1))
	e.mu.Lock()
	e.handles[h] = &openFile{f: f, direct: actualDirect, align: align}
	e.mu.Unlock()
	return h, nil
}

// Read reads length bytes at off from h into buf using File.ReadAt.
// Returns engine.ErrShortRead when fewer bytes are available than requested.
// When the handle was opened with O_DIRECT, an aligned staging buffer is used.
func (e *LocalFileEngine) Read(_ context.Context, h engine.Handle, off, length int64, buf []byte) (int, error) {
	of, err := e.lookupHandle(h)
	if err != nil {
		return 0, err
	}
	if of.direct {
		return alignedReadAt(of.f, buf, off, length, of.align)
	}
	n, readErr := of.f.ReadAt(buf[:length], off)
	if errors.Is(readErr, io.EOF) {
		return n, engine.ErrShortRead
	}
	return n, readErr
}

// Write writes data at off in h using File.WriteAt.
// When the handle was opened with O_DIRECT, a read-modify-write through an
// aligned staging buffer is used to avoid corrupting adjacent bytes.
func (e *LocalFileEngine) Write(_ context.Context, h engine.Handle, off int64, data []byte) (int, error) {
	of, err := e.lookupHandle(h)
	if err != nil {
		return 0, err
	}
	if of.direct {
		return alignedWriteAt(of.f, data, off, of.align)
	}
	return of.f.WriteAt(data, off)
}

// Fsync flushes h to durable storage.
func (e *LocalFileEngine) Fsync(_ context.Context, h engine.Handle) error {
	of, err := e.lookupHandle(h)
	if err != nil {
		return err
	}
	return of.f.Sync()
}

// Close closes h and removes it from the handle table.
func (e *LocalFileEngine) Close(_ context.Context, h engine.Handle) error {
	e.mu.Lock()
	of, ok := e.handles[h]
	if !ok {
		e.mu.Unlock()
		return fmt.Errorf("localfile: close: unknown handle %d: %w", h, engine.ErrNotFound)
	}
	delete(e.handles, h)
	e.mu.Unlock()
	return of.f.Close()
}

// Stat returns size metadata for target.
func (e *LocalFileEngine) Stat(_ context.Context, target string) (engine.ObjectInfo, error) {
	info, err := os.Stat(target)
	if err != nil {
		if os.IsNotExist(err) {
			return engine.ObjectInfo{}, fmt.Errorf("localfile: stat %s: %w", target, engine.ErrNotFound)
		}
		return engine.ObjectInfo{}, fmt.Errorf("localfile: stat %s: %w", target, err)
	}
	return engine.ObjectInfo{Name: target, Size: info.Size()}, nil
}

// Put, Get, Head, and Delete are not supported by LocalFileEngine
// (Caps().ObjectAPI == false).

func (e *LocalFileEngine) Put(_ context.Context, _ string, _ io.Reader, _ int64) error {
	return engine.ErrUnsupported
}

func (e *LocalFileEngine) Get(_ context.Context, _ string, _, _ int64, _ []byte) (int, error) {
	return 0, engine.ErrUnsupported
}

func (e *LocalFileEngine) Head(_ context.Context, _ string) (engine.ObjectInfo, error) {
	return engine.ObjectInfo{}, engine.ErrUnsupported
}

func (e *LocalFileEngine) Delete(_ context.Context, _ string) error {
	return engine.ErrUnsupported
}

func (e *LocalFileEngine) lookupHandle(h engine.Handle) (*openFile, error) {
	e.mu.Lock()
	of, ok := e.handles[h]
	e.mu.Unlock()
	if !ok {
		return nil, fmt.Errorf("localfile: unknown handle %d: %w", h, engine.ErrNotFound)
	}
	return of, nil
}

func modeToOsFlag(mode engine.Mode) int {
	switch mode {
	case engine.ModeRead:
		return os.O_RDONLY
	case engine.ModeWrite:
		return os.O_WRONLY
	case engine.ModeReadWrite:
		return os.O_RDWR
	default:
		return os.O_RDONLY
	}
}
