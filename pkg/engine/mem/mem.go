// Package mem provides MemEngine, an in-process, zero-I/O storage engine.
//
// MemEngine stores all objects as byte slices. New objects are created lazily
// on first Open or Stat using a configurable size function. It is the truth
// oracle for replay correctness tests: deterministic, fast, and noise-free.
//
// MemEngine calls runtime.Gosched() after every operation so that
// high-concurrency replay runs against it reproduce the goroutine-scheduling
// behavior of real blocking I/O. Without this, a tight loop of in-memory ops
// would never yield, and observed scheduler behavior would be unrepresentative.
package mem

import (
	"context"
	"fmt"
	"io"
	"runtime"
	"sync"
	"sync/atomic"

	"github.com/chanuollala/ioflux/pkg/engine"
)

// MemEngine is an in-process storage engine backed by byte slices.
// It is safe for concurrent use by multiple goroutines.
type MemEngine struct {
	mu      sync.Mutex
	objects map[string]*memObject
	handles map[engine.Handle]*openHandle
	nextH   atomic.Int64
	sizeOf  func(target string) int64
}

type memObject struct {
	mu   sync.RWMutex
	data []byte
}

type openHandle struct {
	target string
	mode   engine.Mode
	obj    *memObject
}

// Option configures a MemEngine.
type Option func(*MemEngine)

// WithFixedSize sets the byte size of every new object created by the engine.
// Use this when all targets are known to have the same size (e.g., uniform
// shard files). The default size is 64 MiB.
func WithFixedSize(size int64) Option {
	return func(e *MemEngine) {
		e.sizeOf = func(_ string) int64 { return size }
	}
}

// WithSizeFunc sets a per-target size function. The function is called once
// per new target (on first Open or Stat) and must return the byte size for
// that target.
func WithSizeFunc(f func(target string) int64) Option {
	return func(e *MemEngine) { e.sizeOf = f }
}

// TODO(M2): add WithInjectedDelay(d time.Duration) option so the coordinated-
// omission test can make the engine artificially slow.

// New returns a new MemEngine. Without options, new objects are 64 MiB.
func New(opts ...Option) *MemEngine {
	e := &MemEngine{
		objects: make(map[string]*memObject),
		handles: make(map[engine.Handle]*openHandle),
		sizeOf:  func(_ string) int64 { return 64 << 20 },
	}
	for _, opt := range opts {
		opt(e)
	}
	return e
}

func (e *MemEngine) Caps() engine.Capabilities {
	return engine.Capabilities{
		Seekable:     true,
		PartialWrite: true,
		Durable:      false,
		ObjectAPI:    false,
		Multipart:    false,
	}
}

// Open opens target for the given mode. If target does not yet exist, it is
// created with zeroed content sized by the engine's size function.
func (e *MemEngine) Open(_ context.Context, target string, mode engine.Mode, _ engine.OpenFlags) (engine.Handle, error) {
	defer runtime.Gosched()

	e.mu.Lock()
	obj, ok := e.objects[target]
	if !ok {
		obj = &memObject{data: make([]byte, e.sizeOf(target))}
		e.objects[target] = obj
	}
	h := engine.Handle(e.nextH.Add(1))
	e.handles[h] = &openHandle{target: target, mode: mode, obj: obj}
	e.mu.Unlock()

	return h, nil
}

// Read copies length bytes starting at off from h's object into buf.
// Returns ErrShortRead if fewer bytes are available than requested.
func (e *MemEngine) Read(_ context.Context, h engine.Handle, off, length int64, buf []byte) (int, error) {
	defer runtime.Gosched()

	if off < 0 {
		return 0, fmt.Errorf("mem: Read: offset %d must be non-negative", off)
	}
	if length < 0 {
		return 0, fmt.Errorf("mem: Read: length %d must be non-negative", length)
	}

	oh, err := e.lookupHandle(h)
	if err != nil {
		return 0, err
	}

	oh.obj.mu.RLock()
	defer oh.obj.mu.RUnlock()

	data := oh.obj.data
	if off >= int64(len(data)) {
		return 0, engine.ErrShortRead
	}
	end := off + length
	if end > int64(len(data)) {
		end = int64(len(data))
	}
	n := copy(buf, data[off:end])
	if int64(n) < length {
		return n, engine.ErrShortRead
	}
	return n, nil
}

// Write writes data into h's object starting at off. If the write extends
// beyond the current object size, the object is grown to fit.
func (e *MemEngine) Write(_ context.Context, h engine.Handle, off int64, data []byte) (int, error) {
	defer runtime.Gosched()

	if off < 0 {
		return 0, fmt.Errorf("mem: Write: offset %d must be non-negative", off)
	}

	oh, err := e.lookupHandle(h)
	if err != nil {
		return 0, err
	}

	oh.obj.mu.Lock()
	defer oh.obj.mu.Unlock()

	end := off + int64(len(data))
	if end > int64(len(oh.obj.data)) {
		grown := make([]byte, end)
		copy(grown, oh.obj.data)
		oh.obj.data = grown
	}
	copy(oh.obj.data[off:], data)
	return len(data), nil
}

// Fsync is not supported by MemEngine (Caps().Durable == false). Any trace
// with FSYNC ops must be rejected by the executor at PREPARE time via the
// Caps check, before Fsync is ever called.
func (e *MemEngine) Fsync(_ context.Context, _ engine.Handle) error {
	defer runtime.Gosched()
	return engine.ErrUnsupported
}

// Close releases the handle. The underlying object is retained in memory.
func (e *MemEngine) Close(_ context.Context, h engine.Handle) error {
	defer runtime.Gosched()

	e.mu.Lock()
	defer e.mu.Unlock()

	if _, ok := e.handles[h]; !ok {
		return fmt.Errorf("mem: Close: unknown handle %d: %w", h, engine.ErrNotFound)
	}
	delete(e.handles, h)
	return nil
}

// Stat returns metadata for target. If target does not exist, it is created
// (same lazy-creation semantics as Open).
func (e *MemEngine) Stat(_ context.Context, target string) (engine.ObjectInfo, error) {
	defer runtime.Gosched()

	e.mu.Lock()
	obj, ok := e.objects[target]
	if !ok {
		obj = &memObject{data: make([]byte, e.sizeOf(target))}
		e.objects[target] = obj
	}
	e.mu.Unlock()

	obj.mu.RLock()
	size := int64(len(obj.data))
	obj.mu.RUnlock()

	return engine.ObjectInfo{Name: target, Size: size}, nil
}

// Put, Get, Head, and Delete are not supported by MemEngine
// (Caps().ObjectAPI == false). Each calls runtime.Gosched() to satisfy the
// package contract ("every operation yields") even though they return
// immediately.

func (e *MemEngine) Put(_ context.Context, _ string, _ io.Reader, _ int64) error {
	defer runtime.Gosched()
	return engine.ErrUnsupported
}

func (e *MemEngine) Get(_ context.Context, _ string, _, _ int64, _ []byte) (int, error) {
	defer runtime.Gosched()
	return 0, engine.ErrUnsupported
}

func (e *MemEngine) Head(_ context.Context, _ string) (engine.ObjectInfo, error) {
	defer runtime.Gosched()
	return engine.ObjectInfo{}, engine.ErrUnsupported
}

func (e *MemEngine) Delete(_ context.Context, _ string) error {
	defer runtime.Gosched()
	return engine.ErrUnsupported
}

// lookupHandle returns the openHandle for h, holding the engine lock only
// long enough to read the map. Safe to call without the lock held.
func (e *MemEngine) lookupHandle(h engine.Handle) (*openHandle, error) {
	e.mu.Lock()
	oh, ok := e.handles[h]
	e.mu.Unlock()
	if !ok {
		return nil, fmt.Errorf("mem: unknown handle %d: %w", h, engine.ErrNotFound)
	}
	return oh, nil
}
