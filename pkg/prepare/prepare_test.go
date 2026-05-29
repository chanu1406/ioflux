package prepare_test

import (
	"context"
	"io"
	"os"
	"path/filepath"
	"runtime"
	"testing"

	"github.com/chanuollala/ioflux/pkg/engine"
	"github.com/chanuollala/ioflux/pkg/engine/localfile"
	"github.com/chanuollala/ioflux/pkg/prepare"
	"github.com/chanuollala/ioflux/pkg/trace"
)

// --- assume-existing ---

func TestAssumeExisting_RejectsMissing(t *testing.T) {
	dir := t.TempDir()
	eng := localfile.New()
	tgts := []trace.TargetInfo{{ID: 0, Name: filepath.Join(dir, "missing.tar"), Kind: trace.TargetFile, Size: 1024}}

	prep, err := prepare.For(prepare.ModeAssumeExisting, "")
	if err != nil {
		t.Fatal(err)
	}
	_, err = prep.Prepare(context.Background(), tgts, nil, nil, eng)
	if err == nil {
		t.Fatal("Prepare should fail for missing target")
	}
}

func TestAssumeExisting_RejectsSizeMismatchWhenKnown(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "file.tar")
	if err := os.WriteFile(path, make([]byte, 512), 0o644); err != nil {
		t.Fatal(err)
	}
	eng := localfile.New()
	tgts := []trace.TargetInfo{{ID: 0, Name: path, Kind: trace.TargetFile, Size: 1024}} // size mismatch

	prep, err := prepare.For(prepare.ModeAssumeExisting, "")
	if err != nil {
		t.Fatal(err)
	}
	_, err = prep.Prepare(context.Background(), tgts, nil, nil, eng)
	if err == nil {
		t.Fatal("Prepare should fail on size mismatch")
	}
}

func TestAssumeExisting_SkipsSizeCheckWhenUnknown(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "file.tar")
	if err := os.WriteFile(path, make([]byte, 512), 0o644); err != nil {
		t.Fatal(err)
	}
	eng := localfile.New()
	tgts := []trace.TargetInfo{{ID: 0, Name: path, Kind: trace.TargetFile, Size: 0}} // unknown size

	prep, err := prepare.For(prepare.ModeAssumeExisting, "")
	if err != nil {
		t.Fatal(err)
	}
	stats, err := prep.Prepare(context.Background(), tgts, nil, nil, eng)
	if err != nil {
		t.Fatalf("Prepare: %v", err)
	}
	if stats.SkippedSizeUnknown != 1 {
		t.Errorf("SkippedSizeUnknown=%d, want 1", stats.SkippedSizeUnknown)
	}
	if stats.Verified != 0 {
		t.Errorf("Verified=%d, want 0 (size was unknown)", stats.Verified)
	}
}

func TestAssumeExisting_VerifiesCorrectSize(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "file.tar")
	const size = 4096
	if err := os.WriteFile(path, make([]byte, size), 0o644); err != nil {
		t.Fatal(err)
	}
	eng := localfile.New()
	tgts := []trace.TargetInfo{{ID: 0, Name: path, Kind: trace.TargetFile, Size: size}}

	prep, err := prepare.For(prepare.ModeAssumeExisting, "")
	if err != nil {
		t.Fatal(err)
	}
	stats, err := prep.Prepare(context.Background(), tgts, nil, nil, eng)
	if err != nil {
		t.Fatalf("Prepare: %v", err)
	}
	if stats.Verified != 1 {
		t.Errorf("Verified=%d, want 1", stats.Verified)
	}
}

func TestAssumeExisting_ObjectEngineUsesHead(t *testing.T) {
	const size = 4096
	eng := &objectStub{sizes: map[string]int64{"shard.tar": size}}
	tgts := []trace.TargetInfo{{ID: 0, Name: "shard.tar", Kind: trace.TargetObject, Size: size}}

	prep, err := prepare.For(prepare.ModeAssumeExisting, "")
	if err != nil {
		t.Fatal(err)
	}
	stats, err := prep.Prepare(context.Background(), tgts, nil, nil, eng)
	if err != nil {
		t.Fatalf("Prepare: %v", err)
	}
	if stats.Verified != 1 {
		t.Errorf("Verified=%d, want 1", stats.Verified)
	}
	if eng.statCalls != 0 {
		t.Errorf("Stat called %d times, want 0 (should use Head for object engines)", eng.statCalls)
	}
	if eng.headCalls != 1 {
		t.Errorf("Head called %d times, want 1", eng.headCalls)
	}
}

// objectStub is a minimal engine with ObjectAPI=true for testing assume-existing.
// Stat always errors to prove the code path takes Head instead.
type objectStub struct {
	sizes     map[string]int64
	headCalls int
	statCalls int
}

func (e *objectStub) Caps() engine.Capabilities {
	return engine.Capabilities{ObjectAPI: true}
}
func (e *objectStub) Open(_ context.Context, _ string, _ engine.Mode, _ engine.OpenFlags) (engine.Handle, error) {
	return 0, engine.ErrUnsupported
}
func (e *objectStub) Read(_ context.Context, _ engine.Handle, _, _ int64, _ []byte) (int, error) {
	return 0, engine.ErrUnsupported
}
func (e *objectStub) Write(_ context.Context, _ engine.Handle, _ int64, _ []byte) (int, error) {
	return 0, engine.ErrUnsupported
}
func (e *objectStub) Fsync(_ context.Context, _ engine.Handle) error { return engine.ErrUnsupported }
func (e *objectStub) Close(_ context.Context, _ engine.Handle) error { return engine.ErrUnsupported }
func (e *objectStub) Stat(_ context.Context, _ string) (engine.ObjectInfo, error) {
	e.statCalls++
	return engine.ObjectInfo{}, engine.ErrUnsupported
}
func (e *objectStub) Put(_ context.Context, _ string, _ io.Reader, _ int64) error {
	return engine.ErrUnsupported
}
func (e *objectStub) Get(_ context.Context, _ string, _, _ int64, _ []byte) (int, error) {
	return 0, engine.ErrUnsupported
}
func (e *objectStub) Head(_ context.Context, key string) (engine.ObjectInfo, error) {
	e.headCalls++
	sz, ok := e.sizes[key]
	if !ok {
		return engine.ObjectInfo{}, engine.ErrNotFound
	}
	return engine.ObjectInfo{Name: key, Size: sz}, nil
}
func (e *objectStub) Delete(_ context.Context, _ string) error { return engine.ErrUnsupported }

// --- materialize-synthetic ---

func TestMaterializeSynthetic_PosixCreatesAllTargets(t *testing.T) {
	dir := t.TempDir()
	paths := []string{
		filepath.Join(dir, "shard_0.tar"),
		filepath.Join(dir, "shard_1.tar"),
	}
	tgts := []trace.TargetInfo{
		{ID: 0, Name: paths[0], Kind: trace.TargetFile, Size: 16 * 1024},
		{ID: 1, Name: paths[1], Kind: trace.TargetFile, Size: 8 * 1024},
	}

	prep, err := prepare.For(prepare.ModeMaterializeSynthetic, "")
	if err != nil {
		t.Fatal(err)
	}
	stats, err := prep.Prepare(context.Background(), tgts, nil, nil, localfile.New())
	if err != nil {
		t.Fatalf("Prepare: %v", err)
	}
	if stats.Created != 2 {
		t.Errorf("Created=%d, want 2", stats.Created)
	}
	if !stats.TouchedSameData {
		t.Error("TouchedSameData should be true after materialization")
	}

	for i, p := range paths {
		fi, err := os.Stat(p)
		if err != nil {
			t.Fatalf("stat %s: %v", p, err)
		}
		if fi.Size() != tgts[i].Size {
			t.Errorf("%s: size=%d, want %d", p, fi.Size(), tgts[i].Size)
		}
	}
}

func TestMaterializeSynthetic_DerivesSizeFromOps(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "target.tar")
	tgts := []trace.TargetInfo{{ID: 0, Name: path, Kind: trace.TargetFile, Size: 0}} // unknown size

	// Two READs: [0, 4MiB) and [8MiB, 12MiB) → required size = 12MiB.
	h := int64(1)
	tgt0 := 0
	ops := []trace.Op{
		{T: 0, OpID: trace.Ptr(int64(0)), S: 0, Op: trace.OpOpen, Tgt: &tgt0, H: &h, Mode: trace.ModeRead},
		{T: 1, OpID: trace.Ptr(int64(1)), S: 0, Op: trace.OpRead, H: &h, Off: trace.Ptr(int64(0)), Len: trace.Ptr(int64(4 << 20))},
		{T: 2, OpID: trace.Ptr(int64(2)), S: 0, Op: trace.OpRead, H: &h, Off: trace.Ptr(int64(8 << 20)), Len: trace.Ptr(int64(4 << 20))},
		{T: 3, OpID: trace.Ptr(int64(3)), S: 0, Op: trace.OpClose, H: &h},
	}

	prep, err := prepare.For(prepare.ModeMaterializeSynthetic, "")
	if err != nil {
		t.Fatal(err)
	}
	stats, err := prep.Prepare(context.Background(), tgts, nil, ops, localfile.New())
	if err != nil {
		t.Fatalf("Prepare: %v", err)
	}
	if stats.DerivedSizeFromOps != 1 {
		t.Errorf("DerivedSizeFromOps=%d, want 1", stats.DerivedSizeFromOps)
	}

	fi, err := os.Stat(path)
	if err != nil {
		t.Fatal(err)
	}
	const wantSize = 12 << 20
	if fi.Size() != wantSize {
		t.Errorf("file size=%d, want %d (12MiB)", fi.Size(), wantSize)
	}
}

func TestMaterializeSynthetic_RejectsUntouchedTarget(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "unused.tar")
	tgts := []trace.TargetInfo{{ID: 0, Name: path, Kind: trace.TargetFile, Size: 0}}
	// No ops reference this target.

	prep, err := prepare.For(prepare.ModeMaterializeSynthetic, "")
	if err != nil {
		t.Fatal(err)
	}
	_, err = prep.Prepare(context.Background(), tgts, nil, nil, localfile.New())
	if err == nil {
		t.Fatal("Prepare should error for target with unknown size and no ops")
	}
}

func TestMaterializeSynthetic_StreamsInChunks(t *testing.T) {
	const targetSize = 256 << 20 // 256 MiB
	tgts := []trace.TargetInfo{{ID: 0, Name: "big.tar", Kind: trace.TargetFile, Size: targetSize}}

	eng := &discardEngine{}
	prep, err := prepare.For(prepare.ModeMaterializeSynthetic, "")
	if err != nil {
		t.Fatal(err)
	}

	runtime.GC()
	var memBefore runtime.MemStats
	runtime.ReadMemStats(&memBefore)

	if _, err := prep.Prepare(context.Background(), tgts, nil, nil, eng); err != nil {
		t.Fatalf("Prepare: %v", err)
	}

	runtime.GC()
	var memAfter runtime.MemStats
	runtime.ReadMemStats(&memAfter)

	// Cumulative bytes allocated during Prepare should be well under 16MiB;
	// if the implementation naively allocates the full 256MiB it would fail.
	const maxDelta = 16 << 20
	allocated := memAfter.TotalAlloc - memBefore.TotalAlloc
	if allocated > maxDelta {
		t.Errorf("TotalAlloc delta=%d bytes (%.1f MiB) > 16MiB limit; prepare may be pre-allocating the full object",
			allocated, float64(allocated)/(1<<20))
	}
}

// --- materialize-from-source ---

func TestMaterializeFromSource_CopiesContent(t *testing.T) {
	srcDir := t.TempDir()
	dstDir := t.TempDir()

	// Write source files.
	const (
		file0Content = "hello from source 0"
		file1Content = "hello from source 1 (longer)"
	)
	if err := os.WriteFile(filepath.Join(srcDir, "a.dat"), []byte(file0Content), 0o644); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(srcDir, "b.dat"), []byte(file1Content), 0o644); err != nil {
		t.Fatal(err)
	}

	tgts := []trace.TargetInfo{
		{ID: 0, Name: filepath.Join(dstDir, "a.dat"), Kind: trace.TargetFile},
		{ID: 1, Name: filepath.Join(dstDir, "b.dat"), Kind: trace.TargetFile},
	}

	// materialize-from-source joins root + tgt.Name; since tgt.Name is already
	// absolute, use filepath.Base for the names instead.
	tgtsRel := []trace.TargetInfo{
		{ID: 0, Name: "a.dat", Kind: trace.TargetFile},
		{ID: 1, Name: "b.dat", Kind: trace.TargetFile},
	}

	eng := localfile.New()
	prep, err := prepare.For(prepare.ModeMaterializeFromSource, srcDir)
	if err != nil {
		t.Fatal(err)
	}

	// Temporarily chdir to dstDir so localfile opens relative names there.
	orig, err := os.Getwd()
	if err != nil {
		t.Fatal(err)
	}
	if err := os.Chdir(dstDir); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { os.Chdir(orig) })

	stats, err := prep.Prepare(context.Background(), tgtsRel, nil, nil, eng)
	if err != nil {
		t.Fatalf("Prepare: %v", err)
	}
	if stats.Copied != 2 {
		t.Errorf("Copied=%d, want 2", stats.Copied)
	}
	if !stats.TouchedSameData {
		t.Error("TouchedSameData should be true")
	}

	// Verify content in dstDir.
	got0, err := os.ReadFile(filepath.Join(dstDir, "a.dat"))
	if err != nil {
		t.Fatal(err)
	}
	if string(got0) != file0Content {
		t.Errorf("a.dat content=%q, want %q", got0, file0Content)
	}
	got1, err := os.ReadFile(filepath.Join(dstDir, "b.dat"))
	if err != nil {
		t.Fatal(err)
	}
	if string(got1) != file1Content {
		t.Errorf("b.dat content=%q, want %q", got1, file1Content)
	}
	_ = tgts // suppress unused warning
}

func TestMaterializeFromSource_RejectsMissingSource(t *testing.T) {
	tgts := []trace.TargetInfo{{ID: 0, Name: "nonexistent.tar", Kind: trace.TargetFile}}
	prep, err := prepare.For(prepare.ModeMaterializeFromSource, "/nonexistent-source-root")
	if err != nil {
		t.Fatal(err)
	}
	_, err = prep.Prepare(context.Background(), tgts, nil, nil, localfile.New())
	if err == nil {
		t.Fatal("Prepare should fail for missing source file")
	}
}

func TestFor_UnknownMode(t *testing.T) {
	_, err := prepare.For("unknown-mode", "")
	if err == nil {
		t.Fatal("For should error on unknown mode")
	}
}

func TestFor_MaterializeFromSourceRequiresRoot(t *testing.T) {
	_, err := prepare.For(prepare.ModeMaterializeFromSource, "")
	if err == nil {
		t.Fatal("For should error when sourceRoot is empty for materialize-from-source")
	}
}

// discardEngine accepts writes but does not store data. Used to test that
// materialize-synthetic doesn't allocate the full object size in memory.
type discardEngine struct{}

func (e *discardEngine) Caps() engine.Capabilities {
	return engine.Capabilities{Seekable: true, PartialWrite: true}
}
func (e *discardEngine) Open(_ context.Context, _ string, _ engine.Mode, _ engine.OpenFlags) (engine.Handle, error) {
	return 1, nil
}
func (e *discardEngine) Read(_ context.Context, _ engine.Handle, _, _ int64, _ []byte) (int, error) {
	return 0, engine.ErrShortRead
}
func (e *discardEngine) Write(_ context.Context, _ engine.Handle, _ int64, data []byte) (int, error) {
	return len(data), nil
}
func (e *discardEngine) Fsync(_ context.Context, _ engine.Handle) error { return nil }
func (e *discardEngine) Close(_ context.Context, _ engine.Handle) error { return nil }
func (e *discardEngine) Stat(_ context.Context, name string) (engine.ObjectInfo, error) {
	return engine.ObjectInfo{Name: name, Size: 0}, nil
}
func (e *discardEngine) Put(_ context.Context, _ string, r io.Reader, _ int64) error {
	_, err := io.Copy(io.Discard, r)
	return err
}
func (e *discardEngine) Get(_ context.Context, _ string, _, _ int64, _ []byte) (int, error) {
	return 0, engine.ErrUnsupported
}
func (e *discardEngine) Head(_ context.Context, _ string) (engine.ObjectInfo, error) {
	return engine.ObjectInfo{}, engine.ErrUnsupported
}
func (e *discardEngine) Delete(_ context.Context, _ string) error { return engine.ErrUnsupported }
