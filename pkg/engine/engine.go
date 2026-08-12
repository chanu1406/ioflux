// Package engine defines the storage-backend abstraction and the types shared
// by all engine implementations. The replay executor depends only on Engine.
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

	// ErrShortWrite marks a Write that accepted fewer bytes than requested.
	// The return value n holds the number of bytes actually written.
	ErrShortWrite = errors.New("engine: short write")

	// ErrOutsideRoot is returned when a target resolves outside the engine's
	// configured containment root.
	ErrOutsideRoot = errors.New("engine: target resolves outside the configured root")
)

// Handle is an opaque reference to an open file, returned by Open and passed to
// Read, Write, Fsync, and Close.
type Handle int64

// Mode is the file-open mode. Values match the trace format's "mode" field.
type Mode string

const (
	ModeRead      Mode = "r"
	ModeWrite     Mode = "w"
	ModeReadWrite Mode = "rw"
)

// OpenFlags is a bitmask of optional behaviors for Engine.Open. Engines honor
// only the flags relevant to their backend.
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

// Capabilities describes what a backend supports. The replay executor checks it
// before the run starts and rejects traces needing unsupported operations.
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

// TargetChecker is implemented by engines that confine targets to a configured
// root. Callers that touch targets outside the Engine interface — dataset
// preparation, cache controls — must consult it for containment to hold.
//
// CheckTarget returns nil when target is allowed, and an error wrapping
// ErrOutsideRoot when it is not.
type TargetChecker interface {
	CheckTarget(target string) error
}

// Limiter is implemented by engines that record what their configuration does
// not guarantee: a requested behavior that could not be honored, or a safety
// property the run lacked. Callers surface these in run metadata.
type Limiter interface {
	Limitations() []string
}

// Shutdowner is implemented by engines holding process-level OS resources
// beyond file handles, such as the local engine's containment root. A caller
// building engines repeatedly must shut the previous one down.
//
// Shutdown does not close outstanding file handles; Close those individually.
type Shutdowner interface {
	Shutdown() error
}

// Engine is the storage-backend abstraction. Implementations must be safe for
// concurrent use. Unsupported operations return ErrUnsupported; callers check
// Caps() first. ctx cancellation is meaningful only for network-backed engines.
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
