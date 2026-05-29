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

// LocalFileEngine is a local-filesystem storage engine.
type LocalFileEngine struct {
	mu          sync.Mutex
	handles     map[engine.Handle]*os.File
	nextH       atomic.Int64
	allowDirect bool
}

// Option configures a LocalFileEngine.
type Option func(*LocalFileEngine)

// WithAllowDirect enables O_DIRECT when a trace OPEN carries the "direct"
// flag. Silently ignored on platforms where O_DIRECT is unavailable (non-Linux).
func WithAllowDirect(b bool) Option {
	return func(e *LocalFileEngine) { e.allowDirect = b }
}

// New returns a new LocalFileEngine.
func New(opts ...Option) *LocalFileEngine {
	e := &LocalFileEngine{
		handles: make(map[engine.Handle]*os.File),
	}
	for _, opt := range opts {
		opt(e)
	}
	return e
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
func (e *LocalFileEngine) Open(_ context.Context, target string, mode engine.Mode, flags engine.OpenFlags) (engine.Handle, error) {
	oflags := modeToOsFlag(mode)
	if flags&engine.OpenFlagCreate != 0 {
		oflags |= os.O_CREATE
	}
	if flags&engine.OpenFlagTrunc != 0 {
		oflags |= os.O_TRUNC
	}
	if flags&engine.OpenFlagAppend != 0 {
		oflags |= os.O_APPEND
	}
	if flags&engine.OpenFlagSync != 0 {
		oflags |= os.O_SYNC
	}
	if flags&engine.OpenFlagDirect != 0 && e.allowDirect && canDirect {
		oflags |= openDirectFlag
	}

	if flags&engine.OpenFlagCreate != 0 {
		if dir := filepath.Dir(target); dir != "" && dir != "." {
			if err := os.MkdirAll(dir, 0o755); err != nil {
				return 0, fmt.Errorf("localfile: mkdir %s: %w", dir, err)
			}
		}
	}

	f, err := os.OpenFile(target, oflags, 0o644)
	if err != nil {
		return 0, fmt.Errorf("localfile: open %s: %w", target, err)
	}

	h := engine.Handle(e.nextH.Add(1))
	e.mu.Lock()
	e.handles[h] = f
	e.mu.Unlock()
	return h, nil
}

// Read reads length bytes at off from h into buf using File.ReadAt.
// Returns engine.ErrShortRead when fewer bytes are available than requested.
func (e *LocalFileEngine) Read(_ context.Context, h engine.Handle, off, length int64, buf []byte) (int, error) {
	f, err := e.lookupHandle(h)
	if err != nil {
		return 0, err
	}
	n, readErr := f.ReadAt(buf[:length], off)
	if errors.Is(readErr, io.EOF) {
		return n, engine.ErrShortRead
	}
	return n, readErr
}

// Write writes data at off in h using File.WriteAt.
func (e *LocalFileEngine) Write(_ context.Context, h engine.Handle, off int64, data []byte) (int, error) {
	f, err := e.lookupHandle(h)
	if err != nil {
		return 0, err
	}
	return f.WriteAt(data, off)
}

// Fsync flushes h to durable storage.
func (e *LocalFileEngine) Fsync(_ context.Context, h engine.Handle) error {
	f, err := e.lookupHandle(h)
	if err != nil {
		return err
	}
	return f.Sync()
}

// Close closes h and removes it from the handle table.
func (e *LocalFileEngine) Close(_ context.Context, h engine.Handle) error {
	e.mu.Lock()
	f, ok := e.handles[h]
	if !ok {
		e.mu.Unlock()
		return fmt.Errorf("localfile: close: unknown handle %d: %w", h, engine.ErrNotFound)
	}
	delete(e.handles, h)
	e.mu.Unlock()
	return f.Close()
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

func (e *LocalFileEngine) lookupHandle(h engine.Handle) (*os.File, error) {
	e.mu.Lock()
	f, ok := e.handles[h]
	e.mu.Unlock()
	if !ok {
		return nil, fmt.Errorf("localfile: unknown handle %d: %w", h, engine.ErrNotFound)
	}
	return f, nil
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
