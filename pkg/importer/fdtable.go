package importer

// fdKey identifies an open file descriptor within a stream. The same fd number
// in different streams (processes/threads) is a distinct descriptor.
type fdKey struct {
	stream int64
	fd     int
}

// Entry holds the per-descriptor state an importer needs to translate handle
// ops. HasHandle distinguishes a real file handle from a path-only directory
// entry kept solely for openat/fstat path resolution; it must be checked
// explicitly rather than inferred from Handle (0 is a valid handle id).
type Entry struct {
	HasHandle  bool
	Handle     int64
	TargetID   int
	Cursor     int64
	Path       string
	AppendMode bool
}

// FDTable maps (stream, fd) to descriptor state during import. It allocates a
// fresh handle id per Open, so repeated opens of the same target receive
// distinct handles as the trace handle invariant requires.
type FDTable struct {
	entries    map[fdKey]*Entry
	nextHandle int64
}

// NewFDTable returns an empty FDTable.
func NewFDTable() *FDTable {
	return &FDTable{entries: make(map[fdKey]*Entry)}
}

// Open records a real file handle on (stream, fd) and returns its fresh handle
// id. Any prior entry on the same (stream, fd) is replaced (the kernel reuses
// fd numbers after close).
func (t *FDTable) Open(stream int64, fd, targetID int, path string, appendMode bool) int64 {
	h := t.nextHandle
	t.nextHandle++
	t.entries[fdKey{stream, fd}] = &Entry{
		HasHandle:  true,
		Handle:     h,
		TargetID:   targetID,
		Path:       path,
		AppendMode: appendMode,
	}
	return h
}

// OpenDir records a directory descriptor for path resolution only: no handle is
// allocated and no OPEN op should be emitted for it.
func (t *FDTable) OpenDir(stream int64, fd int, path string) {
	t.entries[fdKey{stream, fd}] = &Entry{Path: path}
}

// Resolve returns the entry for (stream, fd), if tracked.
func (t *FDTable) Resolve(stream int64, fd int) (*Entry, bool) {
	e, ok := t.entries[fdKey{stream, fd}]
	return e, ok
}

// PathFor returns the recorded path for (stream, fd), used to resolve relative
// openat dirfds and fstat targets.
func (t *FDTable) PathFor(stream int64, fd int) (string, bool) {
	e, ok := t.entries[fdKey{stream, fd}]
	if !ok {
		return "", false
	}
	return e.Path, true
}

// Advance moves the cursor of (stream, fd) forward by n bytes (no-op if
// untracked).
func (t *FDTable) Advance(stream int64, fd int, n int64) {
	if e, ok := t.entries[fdKey{stream, fd}]; ok {
		e.Cursor += n
	}
}

// SetCursor sets the cursor of (stream, fd) to pos (no-op if untracked). Used
// to apply an lseek result.
func (t *FDTable) SetCursor(stream int64, fd int, pos int64) {
	if e, ok := t.entries[fdKey{stream, fd}]; ok {
		e.Cursor = pos
	}
}

// Close removes (stream, fd) and reports the handle it held. hadHandle is false
// for a path-only directory entry or an untracked fd, signalling the caller to
// consume the close without emitting a CLOSE op.
func (t *FDTable) Close(stream int64, fd int) (handle int64, hadHandle bool) {
	k := fdKey{stream, fd}
	e, ok := t.entries[k]
	if !ok {
		return 0, false
	}
	delete(t.entries, k)
	if !e.HasHandle {
		return 0, false
	}
	return e.Handle, true
}
