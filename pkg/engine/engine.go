// Package engine defines the storage-backend abstraction and the types
// shared by all engine implementations.
//
// Every engine implements Engine. The replay executor depends only on this
// interface; it knows nothing about the underlying storage system.
package engine

import (
	"context"
	"errors"
	"io"
)

// Sentinel errors returned by Engine implementations.
var (
	// ErrUnsupported is returned when an operation is not supported by the
	// engine (e.g., Put on a local-file engine, Fsync on S3).
	ErrUnsupported = errors.New("engine: operation not supported by this backend")

	// ErrNotFound is returned when the target or handle does not exist.
	ErrNotFound = errors.New("engine: target not found")

	// ErrShortRead is returned by Read when fewer bytes are available than
	// requested (e.g., read starting at or near EOF). The return value n
	// holds the number of bytes actually read.
	ErrShortRead = errors.New("engine: short read")
)

// Handle is an opaque reference to an open file, returned by Open and passed
// to Read, Write, Fsync, and Close. Engines define what Handle values mean
// internally; callers must treat them as opaque identifiers.
type Handle int64

// Mode is the file-open mode. Values match the trace format's "mode" field so
// the replay executor can pass them through without conversion.
type Mode string

const (
	ModeRead      Mode = "r"
	ModeWrite     Mode = "w"
	ModeReadWrite Mode = "rw"
)

// OpenFlags is a bitmask of optional behaviors for Engine.Open. Engines
// honor only the flags relevant to their backend; unrecognized flags are
// silently ignored.
type OpenFlags uint32

const (
	OpenFlagNone   OpenFlags = 0
	OpenFlagDirect OpenFlags = 1 << 0 // bypass page cache (O_DIRECT)
	OpenFlagSeq    OpenFlags = 1 << 1 // sequential access hint (FADV_SEQUENTIAL)
	OpenFlagRand   OpenFlags = 1 << 2 // random access hint (FADV_RANDOM)
	OpenFlagSync   OpenFlags = 1 << 3 // O_SYNC — synchronous writes
	OpenFlagAppend OpenFlags = 1 << 4 // O_APPEND
	OpenFlagCreate OpenFlags = 1 << 5 // O_CREAT
	OpenFlagTrunc  OpenFlags = 1 << 6 // O_TRUNC
)

// Capabilities describes what a backend supports. The replay executor calls
// Caps() before the run starts and rejects traces that require unsupported
// operations, rather than letting them fail silently mid-run.
type Capabilities struct {
	Seekable     bool // pread/pwrite at arbitrary offsets
	PartialWrite bool // writes at non-append, non-zero offsets
	Durable      bool // Fsync is meaningful; if false, Fsync returns ErrUnsupported
	ObjectAPI    bool // Put/Get/Head/Delete
	Multipart    bool // multipart/chunked object writes (S3 multipart)
	OSPageCache  bool // backend reads/writes go through the host OS page cache
	// (true for local-FS engines, false for in-process or remote-object engines)
}

// ObjectInfo is returned by Stat and Head.
type ObjectInfo struct {
	Name string
	Size int64
}

// Engine is the storage-backend abstraction. Implementations must be safe for
// concurrent use by multiple goroutines. Operations not supported by the
// backend return ErrUnsupported; the caller must check Caps() before calling
// them in production code.
//
// ctx cancellation is meaningful for engines with network I/O (S3, AIStore);
// in-process engines (Mem) may ignore it.
type Engine interface {
	Caps() Capabilities

	// File operations.
	Open(ctx context.Context, target string, mode Mode, flags OpenFlags) (Handle, error)
	Read(ctx context.Context, h Handle, off, length int64, buf []byte) (int, error)
	Write(ctx context.Context, h Handle, off int64, data []byte) (int, error)
	Fsync(ctx context.Context, h Handle) error
	Close(ctx context.Context, h Handle) error
	Stat(ctx context.Context, target string) (ObjectInfo, error)

	// Object-store operations. Return ErrUnsupported if !Caps().ObjectAPI.
	Put(ctx context.Context, key string, r io.Reader, size int64) error
	Get(ctx context.Context, key string, off, length int64, buf []byte) (int, error)
	Head(ctx context.Context, key string) (ObjectInfo, error)
	Delete(ctx context.Context, key string) error
}
