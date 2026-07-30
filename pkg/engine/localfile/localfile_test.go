package localfile_test

import (
	"bytes"
	"context"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/chanuollala/ioflux/pkg/engine"
	"github.com/chanuollala/ioflux/pkg/engine/localfile"
)

func TestLocalFileCaps(t *testing.T) {
	eng := localfile.New()
	caps := eng.Caps()
	if !caps.Seekable {
		t.Error("Seekable must be true")
	}
	if !caps.PartialWrite {
		t.Error("PartialWrite must be true")
	}
	if !caps.Durable {
		t.Error("Durable must be true")
	}
	if caps.ObjectAPI {
		t.Error("ObjectAPI must be false")
	}
	if caps.Multipart {
		t.Error("Multipart must be false")
	}
}

func TestLocalFileRoundTrip(t *testing.T) {
	dir := t.TempDir()
	target := filepath.Join(dir, "test.dat")
	ctx := context.Background()

	eng := localfile.New()

	// Write via ReadWrite + Create.
	h, err := eng.Open(ctx, target, engine.ModeReadWrite, engine.OpenFlagCreate)
	if err != nil {
		t.Fatalf("Open rw: %v", err)
	}
	payload := []byte("hello ioflux local file engine")
	n, err := eng.Write(ctx, h, 0, payload)
	if err != nil {
		t.Fatalf("Write: %v", err)
	}
	if n != len(payload) {
		t.Fatalf("Write: wrote %d, want %d", n, len(payload))
	}
	if err := eng.Close(ctx, h); err != nil {
		t.Fatalf("Close after write: %v", err)
	}

	// Read back.
	h2, err := eng.Open(ctx, target, engine.ModeRead, engine.OpenFlagNone)
	if err != nil {
		t.Fatalf("Open r: %v", err)
	}
	buf := make([]byte, len(payload))
	n2, err := eng.Read(ctx, h2, 0, int64(len(payload)), buf)
	if err != nil {
		t.Fatalf("Read: %v", err)
	}
	if n2 != len(payload) {
		t.Fatalf("Read: got %d bytes, want %d", n2, len(payload))
	}
	if string(buf) != string(payload) {
		t.Fatalf("Read: got %q, want %q", buf, payload)
	}
	if err := eng.Close(ctx, h2); err != nil {
		t.Fatalf("Close after read: %v", err)
	}

	// Stat.
	info, err := eng.Stat(ctx, target)
	if err != nil {
		t.Fatalf("Stat: %v", err)
	}
	if info.Size != int64(len(payload)) {
		t.Fatalf("Stat.Size=%d, want %d", info.Size, len(payload))
	}
	if info.Name != target {
		t.Fatalf("Stat.Name=%q, want %q", info.Name, target)
	}
}

// TestLocalFilePartialReadWrite verifies offset reads and writes.
func TestLocalFilePartialReadWrite(t *testing.T) {
	dir := t.TempDir()
	target := filepath.Join(dir, "partial.dat")
	ctx := context.Background()
	eng := localfile.New()

	// Create a 1 KiB file.
	h, err := eng.Open(ctx, target, engine.ModeReadWrite, engine.OpenFlagCreate|engine.OpenFlagTrunc)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	full := make([]byte, 1024)
	for i := range full {
		full[i] = byte(i)
	}
	if _, err := eng.Write(ctx, h, 0, full); err != nil {
		t.Fatalf("Write full: %v", err)
	}
	if err := eng.Close(ctx, h); err != nil {
		t.Fatalf("Close: %v", err)
	}

	// Overwrite bytes [256:512] with 0xFF.
	h2, err := eng.Open(ctx, target, engine.ModeReadWrite, engine.OpenFlagNone)
	if err != nil {
		t.Fatalf("Open rw: %v", err)
	}
	patch := make([]byte, 256)
	for i := range patch {
		patch[i] = 0xFF
	}
	if _, err := eng.Write(ctx, h2, 256, patch); err != nil {
		t.Fatalf("Write partial: %v", err)
	}

	// Read back the patched region.
	got := make([]byte, 256)
	if _, err := eng.Read(ctx, h2, 256, 256, got); err != nil {
		t.Fatalf("Read patched: %v", err)
	}
	for i, b := range got {
		if b != 0xFF {
			t.Fatalf("Read patched[%d]=%#x, want 0xFF", i, b)
		}
	}

	// Unpatched region should be unchanged.
	orig := make([]byte, 256)
	if _, err := eng.Read(ctx, h2, 0, 256, orig); err != nil {
		t.Fatalf("Read unpatched: %v", err)
	}
	for i, b := range orig {
		if b != byte(i) {
			t.Fatalf("Read unpatched[%d]=%#x, want %#x", i, b, byte(i))
		}
	}
	if err := eng.Close(ctx, h2); err != nil {
		t.Fatalf("Close h2: %v", err)
	}
}

func TestLocalFileFsync(t *testing.T) {
	dir := t.TempDir()
	target := filepath.Join(dir, "fsync.dat")
	ctx := context.Background()
	eng := localfile.New()

	h, err := eng.Open(ctx, target, engine.ModeWrite, engine.OpenFlagCreate)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	if _, err := eng.Write(ctx, h, 0, []byte("fsync test")); err != nil {
		t.Fatalf("Write: %v", err)
	}
	if err := eng.Fsync(ctx, h); err != nil {
		t.Fatalf("Fsync: %v", err)
	}
	if err := eng.Close(ctx, h); err != nil {
		t.Fatalf("Close: %v", err)
	}
}

func TestLocalFileRejectsBadHandle(t *testing.T) {
	eng := localfile.New()
	ctx := context.Background()
	bad := engine.Handle(9999)

	if _, err := eng.Read(ctx, bad, 0, 10, make([]byte, 10)); !errors.Is(err, engine.ErrNotFound) {
		t.Errorf("Read bad handle: got %v, want ErrNotFound", err)
	}
	if _, err := eng.Write(ctx, bad, 0, []byte("x")); !errors.Is(err, engine.ErrNotFound) {
		t.Errorf("Write bad handle: got %v, want ErrNotFound", err)
	}
	if err := eng.Fsync(ctx, bad); !errors.Is(err, engine.ErrNotFound) {
		t.Errorf("Fsync bad handle: got %v, want ErrNotFound", err)
	}
	if err := eng.Close(ctx, bad); !errors.Is(err, engine.ErrNotFound) {
		t.Errorf("Close bad handle: got %v, want ErrNotFound", err)
	}
}

func TestLocalFileStatNotFound(t *testing.T) {
	eng := localfile.New()
	ctx := context.Background()

	_, err := eng.Stat(ctx, "/nonexistent-ioflux-target-that-does-not-exist")
	if !errors.Is(err, engine.ErrNotFound) {
		t.Fatalf("Stat missing: got %v, want ErrNotFound", err)
	}
}

func TestLocalFileObjectOpsUnsupported(t *testing.T) {
	eng := localfile.New()
	ctx := context.Background()

	if err := eng.Put(ctx, "k", nil, 0); !errors.Is(err, engine.ErrUnsupported) {
		t.Errorf("Put: got %v, want ErrUnsupported", err)
	}
	if _, err := eng.Get(ctx, "k", 0, 0, nil); !errors.Is(err, engine.ErrUnsupported) {
		t.Errorf("Get: got %v, want ErrUnsupported", err)
	}
	if _, err := eng.Head(ctx, "k"); !errors.Is(err, engine.ErrUnsupported) {
		t.Errorf("Head: got %v, want ErrUnsupported", err)
	}
	if err := eng.Delete(ctx, "k"); !errors.Is(err, engine.ErrUnsupported) {
		t.Errorf("Delete: got %v, want ErrUnsupported", err)
	}
}

// TestLocalFileShortReadAtEOF verifies that reading past EOF returns ErrShortRead.
func TestLocalFileShortReadAtEOF(t *testing.T) {
	dir := t.TempDir()
	target := filepath.Join(dir, "small.dat")
	ctx := context.Background()

	if err := os.WriteFile(target, []byte("hi"), 0o644); err != nil {
		t.Fatal(err)
	}
	eng := localfile.New()
	h, err := eng.Open(ctx, target, engine.ModeRead, engine.OpenFlagNone)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer eng.Close(ctx, h)

	buf := make([]byte, 100)
	n, err := eng.Read(ctx, h, 0, 100, buf)
	if !errors.Is(err, engine.ErrShortRead) {
		t.Fatalf("Read past EOF: got (%d, %v), want ErrShortRead", n, err)
	}
	if n != 2 {
		t.Fatalf("Read: n=%d, want 2", n)
	}
}

// TestAppendFlagNotApplied verifies that the engine does NOT apply O_APPEND
// when a trace OPEN carries the append flag. Replay is offset-addressed via
// WriteAt, which returns an error on an O_APPEND-opened file.
func TestAppendFlagNotApplied(t *testing.T) {
	dir := t.TempDir()
	target := filepath.Join(dir, "append_test.dat")
	ctx := context.Background()
	eng := localfile.New()

	h, err := eng.Open(ctx, target, engine.ModeWrite, engine.OpenFlagCreate|engine.OpenFlagAppend)
	if err != nil {
		t.Fatalf("Open with append flag: %v", err)
	}
	defer eng.Close(ctx, h)

	// WriteAt must succeed; if O_APPEND were set, Go's WriteAt returns an error.
	if _, err := eng.Write(ctx, h, 0, []byte("hello")); err != nil {
		t.Errorf("WriteAt on append-flagged open failed: %v (O_APPEND must not be applied)", err)
	}
}

// TestLocalFileConcurrentReads verifies that multiple goroutines can read the
// same file concurrently without data races.
func TestLocalFileConcurrentReads(t *testing.T) {
	dir := t.TempDir()
	target := filepath.Join(dir, "concurrent.dat")
	const fileSize = 64 * 1024
	data := make([]byte, fileSize)
	for i := range data {
		data[i] = byte(i)
	}
	if err := os.WriteFile(target, data, 0o644); err != nil {
		t.Fatal(err)
	}

	ctx := context.Background()
	eng := localfile.New()

	const goroutines = 16
	errs := make(chan error, goroutines)
	for i := 0; i < goroutines; i++ {
		go func(off int64) {
			h, err := eng.Open(ctx, target, engine.ModeRead, engine.OpenFlagNone)
			if err != nil {
				errs <- err
				return
			}
			defer eng.Close(ctx, h)
			buf := make([]byte, 1024)
			_, err = eng.Read(ctx, h, off%int64(fileSize-1024), 1024, buf)
			if err != nil && !errors.Is(err, engine.ErrShortRead) {
				errs <- err
				return
			}
			errs <- nil
		}(int64(i * 1024))
	}
	for i := 0; i < goroutines; i++ {
		if err := <-errs; err != nil {
			t.Errorf("concurrent read error: %v", err)
		}
	}
}

// --- Target-root containment ---
//
// A trace's target names are untrusted input (imported from a capture, or
// hand-edited), and dataset preparation opens write targets with CREATE|TRUNC.
// An engine confined by WithRoot must therefore refuse an escaping target
// before it can read or destroy anything outside the root.

// TestOpenRejectsTargetOutsideRoot verifies that a confined engine refuses both
// a "../" traversal and a bare absolute path, and — the part that actually
// matters — leaves the file it would have escaped to untouched.
func TestOpenRejectsTargetOutsideRoot(t *testing.T) {
	base := t.TempDir()
	root := filepath.Join(base, "scratch")
	if err := os.MkdirAll(root, 0o755); err != nil {
		t.Fatal(err)
	}
	outside := filepath.Join(base, "precious.dat")
	original := []byte("real data that must survive")
	if err := os.WriteFile(outside, original, 0o644); err != nil {
		t.Fatal(err)
	}

	ctx := context.Background()
	eng := localfile.New(localfile.WithRoot(root))

	for _, target := range []string{
		filepath.Join(root, "..", "precious.dat"), // traversal out of the root
		outside, // absolute path outside the root
	} {
		h, err := eng.Open(ctx, target, engine.ModeWrite, engine.OpenFlagCreate|engine.OpenFlagTrunc)
		if !errors.Is(err, engine.ErrOutsideRoot) {
			eng.Close(ctx, h)
			t.Fatalf("Open(%q) err=%v, want engine.ErrOutsideRoot", target, err)
		}
	}

	got, err := os.ReadFile(outside)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(got, original) {
		t.Fatalf("file outside the root was modified: got %q, want %q", got, original)
	}
}

// TestStatRejectsTargetOutsideRoot verifies containment covers the read-only
// metadata path too, so assume-existing preparation cannot probe the host
// filesystem outside the root.
func TestStatRejectsTargetOutsideRoot(t *testing.T) {
	base := t.TempDir()
	root := filepath.Join(base, "scratch")
	if err := os.MkdirAll(root, 0o755); err != nil {
		t.Fatal(err)
	}
	outside := filepath.Join(base, "other.dat")
	if err := os.WriteFile(outside, []byte("x"), 0o644); err != nil {
		t.Fatal(err)
	}

	eng := localfile.New(localfile.WithRoot(root))
	if _, err := eng.Stat(context.Background(), outside); !errors.Is(err, engine.ErrOutsideRoot) {
		t.Fatalf("Stat(%q) err=%v, want engine.ErrOutsideRoot", outside, err)
	}
}

// TestRootDoesNotMatchSiblingPrefix guards against the string-prefix version of
// the containment check: "<root>X" shares a textual prefix with "<root>" but is
// a different directory.
func TestRootDoesNotMatchSiblingPrefix(t *testing.T) {
	base := t.TempDir()
	root := filepath.Join(base, "root")
	sibling := filepath.Join(base, "rootX")
	for _, dir := range []string{root, sibling} {
		if err := os.MkdirAll(dir, 0o755); err != nil {
			t.Fatal(err)
		}
	}

	eng := localfile.New(localfile.WithRoot(root))
	target := filepath.Join(sibling, "f.dat")
	h, err := eng.Open(context.Background(), target, engine.ModeWrite, engine.OpenFlagCreate)
	if !errors.Is(err, engine.ErrOutsideRoot) {
		eng.Close(context.Background(), h)
		t.Fatalf("Open(%q) err=%v, want engine.ErrOutsideRoot", target, err)
	}
}

// TestOpenAllowsTargetInsideRoot verifies containment does not break the normal
// replay path, including creating a nested directory under the root.
func TestOpenAllowsTargetInsideRoot(t *testing.T) {
	root := t.TempDir()
	ctx := context.Background()
	eng := localfile.New(localfile.WithRoot(root))

	target := filepath.Join(root, "nested", "dir", "shard.dat")
	h, err := eng.Open(ctx, target, engine.ModeWrite, engine.OpenFlagCreate|engine.OpenFlagTrunc)
	if err != nil {
		t.Fatalf("Open inside root: %v", err)
	}
	payload := []byte("payload")
	if _, err := eng.Write(ctx, h, 0, payload); err != nil {
		t.Fatalf("Write: %v", err)
	}
	if err := eng.Close(ctx, h); err != nil {
		t.Fatalf("Close: %v", err)
	}

	info, err := eng.Stat(ctx, target)
	if err != nil {
		t.Fatalf("Stat inside root: %v", err)
	}
	if info.Size != int64(len(payload)) {
		t.Fatalf("Stat size=%d, want %d", info.Size, len(payload))
	}
}

// TestRelativeTargetResolvedAgainstWorkingDir verifies that a relative target
// keeps its working-directory meaning under containment: it is made absolute
// against the process CWD (not reinterpreted as root-relative) and then checked,
// so an existing target map is not silently redirected.
func TestRelativeTargetResolvedAgainstWorkingDir(t *testing.T) {
	wd, err := os.Getwd()
	if err != nil {
		t.Fatal(err)
	}
	// A root that cannot contain anything reachable from the CWD: every
	// relative name must be rejected rather than joined onto the root.
	eng := localfile.New(localfile.WithRoot(t.TempDir()))
	h, openErr := eng.Open(context.Background(), "relative-target.dat",
		engine.ModeWrite, engine.OpenFlagCreate)
	if !errors.Is(openErr, engine.ErrOutsideRoot) {
		eng.Close(context.Background(), h)
		t.Fatalf("Open relative target err=%v, want engine.ErrOutsideRoot", openErr)
	}
	if _, err := os.Stat(filepath.Join(wd, "relative-target.dat")); !os.IsNotExist(err) {
		t.Fatalf("relative target was created in the working directory: %v", err)
	}
}

// TestLimitationsRecordContainmentState verifies that both the confined and the
// unconfined engine state their containment status, so a saved report never
// leaves target safety unanswered.
func TestLimitationsRecordContainmentState(t *testing.T) {
	unconfined := localfile.New().Limitations()
	if !anyContains(unconfined, "no target root configured") {
		t.Fatalf("unconfined limitations = %v, want a note that no root was configured", unconfined)
	}

	confined := localfile.New(localfile.WithRoot(t.TempDir())).Limitations()
	if !anyContains(confined, "enforced by the OS") {
		t.Fatalf("confined limitations = %v, want the OS-enforcement statement", confined)
	}
	if !anyContains(confined, "bind mount") {
		t.Fatalf("confined limitations = %v, want the residual bind-mount/device caveat", confined)
	}
	if anyContains(confined, "no target root configured") {
		t.Fatalf("confined engine must not report itself unconfined: %v", confined)
	}
}

func anyContains(list []string, sub string) bool {
	for _, s := range list {
		if strings.Contains(s, sub) {
			return true
		}
	}
	return false
}

// --- Symlink containment ---
//
// The containment root is enforced by the OS through an open directory handle,
// so resolution cannot leave the root even when the filesystem itself points
// out of it. These are the cases a string-comparison check silently allows.

// TestSymlinkInsideRootCannotEscape is the regression test for the hole that
// motivated OS-level enforcement: a symlink placed inside the root pointing at
// a file outside it. A lexical check sees only an in-root path and allows the
// open, letting preparation truncate real data.
func TestSymlinkInsideRootCannotEscape(t *testing.T) {
	base := t.TempDir()
	root := filepath.Join(base, "scratch")
	if err := os.MkdirAll(root, 0o755); err != nil {
		t.Fatal(err)
	}
	outside := filepath.Join(base, "precious.dat")
	original := []byte("real data that must survive")
	if err := os.WriteFile(outside, original, 0o644); err != nil {
		t.Fatal(err)
	}
	// Textually inside the root; actually a door out of it.
	link := filepath.Join(root, "looks-innocent.dat")
	if err := os.Symlink(outside, link); err != nil {
		t.Fatal(err)
	}

	ctx := context.Background()
	eng := localfile.New(localfile.WithRoot(root))
	defer eng.Shutdown()

	h, err := eng.Open(ctx, link, engine.ModeWrite, engine.OpenFlagCreate|engine.OpenFlagTrunc)
	if !errors.Is(err, engine.ErrOutsideRoot) {
		eng.Close(ctx, h)
		t.Fatalf("Open through escaping symlink err=%v, want engine.ErrOutsideRoot", err)
	}
	if _, err := eng.Stat(ctx, link); !errors.Is(err, engine.ErrOutsideRoot) {
		t.Fatalf("Stat through escaping symlink err=%v, want engine.ErrOutsideRoot", err)
	}
	if err := eng.CheckTarget(link); !errors.Is(err, engine.ErrOutsideRoot) {
		t.Fatalf("CheckTarget on escaping symlink err=%v, want engine.ErrOutsideRoot", err)
	}

	got, err := os.ReadFile(outside)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(got, original) {
		t.Fatalf("symlink escape modified the file outside the root: got %q, want %q", got, original)
	}
}

// TestRelativeSymlinkInsideRootIsAllowed verifies containment refuses escapes
// without refusing ordinary datasets: a relative symlink that stays inside the
// root still resolves, including one that traverses "..".
func TestRelativeSymlinkInsideRootIsAllowed(t *testing.T) {
	root := t.TempDir()
	if err := os.MkdirAll(filepath.Join(root, "sub"), 0o755); err != nil {
		t.Fatal(err)
	}
	payload := []byte("INSIDE")
	if err := os.WriteFile(filepath.Join(root, "real.dat"), payload, 0o644); err != nil {
		t.Fatal(err)
	}
	if err := os.Symlink("real.dat", filepath.Join(root, "flat.dat")); err != nil {
		t.Fatal(err)
	}
	if err := os.Symlink("../real.dat", filepath.Join(root, "sub", "updir.dat")); err != nil {
		t.Fatal(err)
	}

	ctx := context.Background()
	eng := localfile.New(localfile.WithRoot(root))
	defer eng.Shutdown()

	for _, name := range []string{"flat.dat", filepath.Join("sub", "updir.dat")} {
		target := filepath.Join(root, name)
		if err := eng.CheckTarget(target); err != nil {
			t.Fatalf("CheckTarget(%s) on an in-root relative symlink: %v", name, err)
		}
		info, err := eng.Stat(ctx, target)
		if err != nil {
			t.Fatalf("Stat(%s) on an in-root relative symlink: %v", name, err)
		}
		if info.Size != int64(len(payload)) {
			t.Fatalf("Stat(%s).Size=%d, want %d", name, info.Size, len(payload))
		}
	}
}

// TestAbsoluteSymlinkRejectedEvenInsideRoot pins a real behavior change: an
// absolute symlink is refused even when it points back inside the root, because
// an absolute link cannot be resolved relative to the root. Documented so an
// operator who hits it knows the dataset, not IOFlux, needs to change.
func TestAbsoluteSymlinkRejectedEvenInsideRoot(t *testing.T) {
	root := t.TempDir()
	if err := os.WriteFile(filepath.Join(root, "real.dat"), []byte("INSIDE"), 0o644); err != nil {
		t.Fatal(err)
	}
	link := filepath.Join(root, "abs.dat")
	if err := os.Symlink(filepath.Join(root, "real.dat"), link); err != nil {
		t.Fatal(err)
	}

	eng := localfile.New(localfile.WithRoot(root))
	defer eng.Shutdown()

	err := eng.CheckTarget(link)
	if !errors.Is(err, engine.ErrOutsideRoot) {
		t.Fatalf("CheckTarget on an absolute in-root symlink err=%v, want engine.ErrOutsideRoot", err)
	}
	if !strings.Contains(err.Error(), "absolute symlinks are rejected") {
		t.Fatalf("error should explain the absolute-symlink rule, got %q", err)
	}
}

// TestRootCreatedWhenMissing verifies pointing --target-root at a directory that
// does not exist yet still works, matching how materialization creates the
// directories beneath it.
func TestRootCreatedWhenMissing(t *testing.T) {
	root := filepath.Join(t.TempDir(), "not", "yet", "there")
	ctx := context.Background()
	eng := localfile.New(localfile.WithRoot(root))
	defer eng.Shutdown()

	target := filepath.Join(root, "shard.dat")
	h, err := eng.Open(ctx, target, engine.ModeWrite, engine.OpenFlagCreate|engine.OpenFlagTrunc)
	if err != nil {
		t.Fatalf("Open inside a freshly created root: %v", err)
	}
	if _, err := eng.Write(ctx, h, 0, []byte("x")); err != nil {
		t.Fatalf("Write: %v", err)
	}
	if err := eng.Close(ctx, h); err != nil {
		t.Fatalf("Close: %v", err)
	}
}

// TestShutdownReleasesRootAndFailsClosed verifies the root handle is released
// and that a shut-down engine refuses work rather than falling back to
// unconfined access.
func TestShutdownReleasesRootAndFailsClosed(t *testing.T) {
	root := t.TempDir()
	ctx := context.Background()
	eng := localfile.New(localfile.WithRoot(root))

	if err := eng.Shutdown(); err != nil {
		t.Fatalf("Shutdown: %v", err)
	}
	if err := eng.Shutdown(); err != nil {
		t.Fatalf("second Shutdown must be a no-op, got %v", err)
	}
	if _, err := eng.Open(ctx, filepath.Join(root, "x.dat"), engine.ModeWrite, engine.OpenFlagCreate); err == nil {
		t.Fatal("Open after Shutdown succeeded; a released root must refuse work")
	}
}

// TestShutdownOnUnconfinedEngineIsNoop verifies an engine with no root is
// unaffected by Shutdown.
func TestShutdownOnUnconfinedEngineIsNoop(t *testing.T) {
	eng := localfile.New()
	if err := eng.Shutdown(); err != nil {
		t.Fatalf("Shutdown on unconfined engine: %v", err)
	}
	dir := t.TempDir()
	h, err := eng.Open(context.Background(), filepath.Join(dir, "x.dat"), engine.ModeWrite, engine.OpenFlagCreate)
	if err != nil {
		t.Fatalf("unconfined engine must keep working after Shutdown: %v", err)
	}
	eng.Close(context.Background(), h)
}

// TestLimitations_DeclaresPositionalReplayForm pins the disclosure that this
// engine substitutes the syscall form on every run: offsets and lengths are
// reproduced exactly, but pread/pwrite replace the source's sequential
// read/write and path-based stat replaces fstat. The qual-01 reconciliation
// confirmed the substitution against a straced replay, so a result claiming
// syscall-level equivalence must carry the qualifier.
func TestLimitations_DeclaresPositionalReplayForm(t *testing.T) {
	// Unconditional: it does not depend on the trace, the root, or O_DIRECT.
	for _, tc := range []struct {
		name string
		eng  *localfile.LocalFileEngine
	}{
		{"unconfined", localfile.New()},
		{"confined", localfile.New(localfile.WithRoot(t.TempDir()))},
	} {
		t.Run(tc.name, func(t *testing.T) {
			defer tc.eng.Shutdown()
			var found string
			for _, l := range tc.eng.Limitations() {
				if strings.Contains(l, "replay is positional") {
					found = l
				}
			}
			if found == "" {
				t.Fatalf("Limitations() does not declare positional replay: %v", tc.eng.Limitations())
			}
			for _, want := range []string{"pread/pwrite", "lseek", "fstat"} {
				if !strings.Contains(found, want) {
					t.Errorf("limitation does not mention %q: %q", want, found)
				}
			}
		})
	}
}
