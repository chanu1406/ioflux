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
	"io/fs"
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

	// rootSpec is the containment root as configured (WithRoot); root is the
	// opened directory every target must resolve within, and rootPath its
	// cleaned absolute form, used to translate target names and to phrase
	// errors. rootErr poisons the engine when a configured root could not be
	// opened, so containment fails closed instead of silently lapsing.
	// root is immutable after New, so reads need no lock; closeOnce guards the
	// single release of its directory handle.
	rootSpec  string
	root      *os.Root
	rootPath  string
	rootErr   error
	closeOnce sync.Once

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

// WithRoot confines the engine to root: every target passed to Open or Stat
// must resolve inside it, so a trace target name can neither read nor overwrite
// data elsewhere on the host. An empty root (the default) applies no
// containment and is recorded as a limitation. The root is created if it does
// not exist.
//
// Enforcement is done by the operating system, not by string comparison: the
// engine holds the root open (os.Root) and every file operation is performed
// relative to that directory handle, so neither a ".." component nor a symlink
// can escape it. Target names are translated to root-relative form first, which
// is where a plainly out-of-root name is rejected with a specific message.
//
// Two consequences worth knowing:
//   - An *absolute* symlink inside the root is rejected even when it points
//     back inside the root, because an absolute link cannot be validated
//     against a root. Relative symlinks that stay inside the root work.
//   - A root confines path resolution, not the filesystem: bind mounts, /proc
//     special files, and device nodes reachable inside the root remain
//     reachable. This is recorded in Limitations.
func WithRoot(root string) Option {
	return func(e *LocalFileEngine) { e.rootSpec = root }
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
	e.addLimitation(posixFormLimitation)
	e.initRoot()
	return e
}

// posixFormLimitation records the syscall-form substitution this engine makes
// on every run. It is not conditional on the trace: reads and writes always go
// through ReadAt/WriteAt (pread/pwrite), and a confined engine resolves targets
// relative to a directory handle, so the syscalls the kernel sees differ in form
// from a source workload that used sequential read/write and path-based stat --
// even when every offset and length matches exactly. A result that claims
// syscall-level equivalence needs to say which syscalls.
const posixFormLimitation = "replay is positional: READ/WRITE are issued as " +
	"pread/pwrite at explicit offsets and never move a file cursor, so a source " +
	"workload's sequential read/write calls and its lseek calls are not reproduced in " +
	"form (offsets and lengths are); STAT is issued against the target path, not as " +
	"fstat on an open descriptor"

// initRoot opens a configured containment root and records what the run may and
// may not conclude about target safety. Both the confined and the unconfined
// case produce a limitation note, so a saved report never leaves the question
// unanswered.
func (e *LocalFileEngine) initRoot() {
	if e.rootSpec == "" {
		e.addLimitation("no target root configured: replay and dataset preparation were not " +
			"confined, so any path a rewritten trace target resolves to could be read or " +
			"overwritten (set --target-root to confine them)")
		return
	}
	abs, err := filepath.Abs(e.rootSpec)
	if err != nil {
		// Fail closed: every subsequent target resolution returns this error.
		e.rootErr = fmt.Errorf("localfile: cannot resolve target root %q: %w", e.rootSpec, err)
		return
	}
	abs = filepath.Clean(abs)
	// Create the root so pointing --target-root at a fresh scratch directory
	// works the same way materialization creates the directories beneath it.
	if err := os.MkdirAll(abs, 0o755); err != nil {
		e.rootErr = fmt.Errorf("localfile: cannot create target root %s: %w", abs, err)
		return
	}
	root, err := os.OpenRoot(abs)
	if err != nil {
		e.rootErr = fmt.Errorf("localfile: cannot open target root %s: %w", abs, err)
		return
	}
	e.root, e.rootPath = root, abs
	e.addLimitation(fmt.Sprintf("target root %s is enforced by the OS: neither \"..\" nor a "+
		"symlink can escape it, but a bind mount, /proc file, or device node reachable "+
		"inside the root is still reachable", abs))
}

// Shutdown releases the containment root's directory handle, implementing
// engine.Shutdowner. Open file handles are unaffected; Close them individually.
// It is safe to call on an unconfined engine and safe to call more than once.
// Operations attempted afterwards fail rather than escaping, since a closed
// root refuses every request.
func (e *LocalFileEngine) Shutdown() error {
	var err error
	e.closeOnce.Do(func() {
		if e.root != nil {
			err = e.root.Close()
		}
	})
	return err
}

// rootRelative translates a trace target name into the root-relative form the
// os.Root methods take. It is path translation, not the security boundary: a
// plainly out-of-root name is rejected here so the error names the resolved
// path, but the guarantee that nothing escapes comes from os.Root itself, which
// also resolves symlinks the filesystem would follow.
func (e *LocalFileEngine) rootRelative(target string) (string, error) {
	abs, err := filepath.Abs(target)
	if err != nil {
		return "", fmt.Errorf("localfile: resolve target %q: %w", target, err)
	}
	// Rel + IsLocal rather than a string prefix: "/rootX" must not count as
	// being inside "/root", and ".." segments must not walk out of it.
	rel, err := filepath.Rel(e.rootPath, abs)
	if err != nil || !filepath.IsLocal(rel) {
		return "", fmt.Errorf("localfile: target %q resolves to %s, outside root %s: %w",
			target, abs, e.rootPath, engine.ErrOutsideRoot)
	}
	return rel, nil
}

// openerFor returns the function that opens target with a given set of OS
// flags, plus the path to name in messages. When a containment root is
// configured every open goes through the root's directory handle, so the
// returned opener cannot reach outside it; otherwise the engine's plain opener
// is used and working-directory relative semantics are preserved. createDirs
// requests the parent directories, created inside the root when confined.
//
// A custom opener installed by NewWithOpener applies only to the unconfined
// path; it is a test seam for simulating filesystem behavior, and a confined
// engine must not bypass its root to honor it.
func (e *LocalFileEngine) openerFor(target string, createDirs bool) (func(int) (*os.File, error), string, error) {
	if e.rootErr != nil {
		return nil, target, e.rootErr
	}
	if e.root == nil {
		if createDirs {
			if dir := filepath.Dir(target); dir != "" && dir != "." {
				if err := os.MkdirAll(dir, 0o755); err != nil {
					return nil, target, fmt.Errorf("localfile: mkdir %s: %w", dir, err)
				}
			}
		}
		return func(oflags int) (*os.File, error) {
			return e.opener(target, oflags, 0o644)
		}, target, nil
	}

	rel, err := e.rootRelative(target)
	if err != nil {
		return nil, target, err
	}
	if createDirs {
		if dir := filepath.Dir(rel); dir != "" && dir != "." {
			if err := e.root.MkdirAll(dir, 0o755); err != nil {
				return nil, target, e.escapeMessage(target, fmt.Errorf("localfile: mkdir %s in root %s: %w", dir, e.rootPath, err))
			}
		}
	}
	display := filepath.Join(e.rootPath, rel)
	return func(oflags int) (*os.File, error) {
		f, err := e.root.OpenFile(rel, oflags, 0o644)
		if err != nil {
			return nil, e.escapeMessage(target, err)
		}
		return f, nil
	}, display, nil
}

// escapeMessage converts an os.Root failure into an engine error. os.Root's
// escape sentinel is unexported, so an escape is identified by the *os.PathError
// it carries. The classification only decides the wording: the operation has
// already been refused either way, so containment never depends on it.
func (e *LocalFileEngine) escapeMessage(target string, err error) error {
	var pe *os.PathError
	if errors.As(err, &pe) && pe.Err != nil && pe.Err.Error() == "path escapes from parent" {
		return fmt.Errorf("localfile: target %q escapes root %s (a symlink or path component "+
			"leaves the root; absolute symlinks are rejected even when they point back inside): %w",
			target, e.rootPath, engine.ErrOutsideRoot)
	}
	return err
}

// CheckTarget reports whether target is inside the configured containment root,
// implementing engine.TargetChecker. It lets a caller reject a whole target
// table before any I/O, including I/O this engine never sees — cache controls
// open the file themselves to call fadvise, so without this gate a symlinked
// target would be resolved by the OS outside the root even though every engine
// call is confined.
//
// A target that does not exist yet is allowed: preparation creates it, and the
// root enforces containment at that point.
func (e *LocalFileEngine) CheckTarget(target string) error {
	if e.rootErr != nil {
		return e.rootErr
	}
	if e.root == nil {
		return nil
	}
	if _, _, err := e.statTarget(target); err != nil {
		if errors.Is(err, fs.ErrNotExist) {
			return nil
		}
		if errors.Is(err, engine.ErrOutsideRoot) {
			return err
		}
		// Fail closed: a target that cannot be confirmed inside the root is not
		// treated as if it were.
		return fmt.Errorf("localfile: target %q could not be verified inside root %s: %w",
			target, e.rootPath, err)
	}
	return nil
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
// to the file (absolute or relative to the process working directory). When the
// engine is confined by WithRoot, a target that resolves outside the root is
// rejected with engine.ErrOutsideRoot before any directory is created or any
// file is opened, created, or truncated.
//
// The "append" flag is intentionally not applied: replay uses offset-addressed
// WriteAt, and Go's WriteAt returns an error on a file opened with O_APPEND.
// The flag is preserved in the trace IR but treated as unmodeled by the engine.
func (e *LocalFileEngine) Open(_ context.Context, target string, mode engine.Mode, flags engine.OpenFlags) (engine.Handle, error) {
	open, path, err := e.openerFor(target, flags&engine.OpenFlagCreate != 0)
	if err != nil {
		return 0, err
	}

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

	f, err := open(oflags)
	if err != nil {
		if wantDirect && isDirectNotSupported(err) {
			if !e.directFallback {
				return 0, fmt.Errorf("localfile: open %s with O_DIRECT: filesystem does not support direct I/O: %w", path, err)
			}
			// Fall back to buffered I/O and record the limitation.
			oflags &^= openDirectFlag
			f, err = open(oflags)
			if err != nil {
				return 0, fmt.Errorf("localfile: open %s: %w", path, err)
			}
			actualDirect = false
			e.addLimitation(fmt.Sprintf("O_DIRECT not supported by filesystem for %s; fell back to buffered I/O", path))
		} else {
			return 0, fmt.Errorf("localfile: open %s: %w", path, err)
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

// Stat returns size metadata for target. When the engine is confined by
// WithRoot, a target that resolves outside the root is rejected with
// engine.ErrOutsideRoot rather than probing the host filesystem.
func (e *LocalFileEngine) Stat(_ context.Context, target string) (engine.ObjectInfo, error) {
	info, path, err := e.statTarget(target)
	if err != nil {
		if errors.Is(err, fs.ErrNotExist) {
			return engine.ObjectInfo{}, fmt.Errorf("localfile: stat %s: %w", path, engine.ErrNotFound)
		}
		return engine.ObjectInfo{}, fmt.Errorf("localfile: stat %s: %w", path, err)
	}
	return engine.ObjectInfo{Name: target, Size: info.Size()}, nil
}

// statTarget stats target through the containment root when one is configured,
// so a symlink that leaves the root is refused rather than followed. It returns
// the path to name in messages alongside the result.
func (e *LocalFileEngine) statTarget(target string) (fs.FileInfo, string, error) {
	if e.rootErr != nil {
		return nil, target, e.rootErr
	}
	if e.root == nil {
		info, err := os.Stat(target)
		return info, target, err
	}
	rel, err := e.rootRelative(target)
	if err != nil {
		return nil, target, err
	}
	display := filepath.Join(e.rootPath, rel)
	info, err := e.root.Stat(rel)
	if err != nil {
		return nil, display, e.escapeMessage(target, err)
	}
	return info, display, nil
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
