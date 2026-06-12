package cache_test

import (
	"context"
	"io"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"testing"

	"github.com/chanuollala/ioflux/pkg/cache"
	"github.com/chanuollala/ioflux/pkg/engine"
	"github.com/chanuollala/ioflux/pkg/engine/localfile"
	"github.com/chanuollala/ioflux/pkg/engine/mem"
	"github.com/chanuollala/ioflux/pkg/trace"
)

func TestColdRecordsLimitationsForMissingFile(t *testing.T) {
	tgts := []trace.TargetInfo{{ID: 0, Name: "/nonexistent/path/file.tar", Kind: trace.TargetFile, Size: 1024}}
	res := cache.Apply(context.Background(), cache.ModeCold, localfile.New(), tgts)
	// Missing file → limitation recorded; run should not crash.
	if len(res.Limitations) == 0 {
		t.Error("expected at least one limitation for missing file, got none")
	}
}

func TestColdRecordsLimitationsOnNonLinux(t *testing.T) {
	// On non-Linux this always records a platform limitation.
	// On Linux with an existing file it should succeed (action) or fail with a
	// limitation — either way the result must be non-empty.
	tgts := []trace.TargetInfo{{ID: 0, Name: "/nonexistent.tar", Kind: trace.TargetFile}}
	res := cache.Apply(context.Background(), cache.ModeCold, localfile.New(), tgts)
	if len(res.Actions)+len(res.Limitations) == 0 {
		t.Error("cold result has neither actions nor limitations")
	}
}

func TestColdSkipsPOSIXForNonPageCacheEngine(t *testing.T) {
	// MemEngine has OSPageCache=false; cold should record a limitation
	// instead of running posix_fadvise on a path the engine never opened.
	tgts := []trace.TargetInfo{{ID: 0, Name: "shard_0.tar", Kind: trace.TargetFile}}
	res := cache.Apply(context.Background(), cache.ModeCold, mem.New(), tgts)
	if len(res.Limitations) == 0 {
		t.Error("expected limitation when engine lacks OS page cache")
	}
}

func TestColdLinuxRecordsSyncBeforeFadvise(t *testing.T) {
	if runtime.GOOS != "linux" {
		t.Skip("Linux-only fadvise behavior")
	}
	dir := t.TempDir()
	path := filepath.Join(dir, "target.dat")
	if err := os.WriteFile(path, []byte("data"), 0o644); err != nil {
		t.Fatalf("WriteFile: %v", err)
	}
	res := cache.Apply(context.Background(), cache.ModeCold, localfile.New(), []trace.TargetInfo{
		{ID: 0, Name: path, Kind: trace.TargetFile, Size: 4},
	})
	if len(res.Actions) < 2 {
		t.Fatalf("Actions=%v, want sync and fadvise actions", res.Actions)
	}
	if !strings.Contains(res.Actions[0], "synced") {
		t.Fatalf("first action=%q, want sync before fadvise; actions=%v", res.Actions[0], res.Actions)
	}
	if !strings.Contains(res.Actions[1], "fadvised DONTNEED") {
		t.Fatalf("second action=%q, want fadvise; actions=%v", res.Actions[1], res.Actions)
	}
}

func TestWarmModePrimesAllTargets(t *testing.T) {
	const targetSize = 4 * 1024
	eng := mem.New(mem.WithFixedSize(targetSize))
	tgts := []trace.TargetInfo{
		{ID: 0, Name: "shard_0.tar", Kind: trace.TargetFile, Size: targetSize},
		{ID: 1, Name: "shard_1.tar", Kind: trace.TargetFile, Size: targetSize},
	}

	res := cache.Apply(context.Background(), cache.ModeWarm, eng, tgts)
	if len(res.Actions) != 2 {
		t.Errorf("Actions=%d, want 2 (one per target)", len(res.Actions))
	}
	if len(res.Limitations) != 0 {
		t.Errorf("Limitations=%v, want none for MemEngine warm priming", res.Limitations)
	}
}

func TestWarmModeUsesObjectAPIForObjectEngines(t *testing.T) {
	const size = 8 * 1024
	eng := &warmObjectStub{sizes: map[string]int64{"a.tar": size, "b.tar": size}}
	tgts := []trace.TargetInfo{
		{ID: 0, Name: "a.tar", Kind: trace.TargetObject, Size: size},
		{ID: 1, Name: "b.tar", Kind: trace.TargetObject, Size: size},
	}

	res := cache.Apply(context.Background(), cache.ModeWarm, eng, tgts)
	if res.Primed != 2 {
		t.Errorf("Primed=%d, want 2", res.Primed)
	}
	if len(res.Limitations) != 0 {
		t.Errorf("Limitations=%v, want none", res.Limitations)
	}
	if eng.openCalls != 0 {
		t.Errorf("Open called %d times, want 0 (should use Head+Get for object engines)", eng.openCalls)
	}
	if eng.headCalls != 2 || eng.getCalls < 2 {
		t.Errorf("Head=%d, Get=%d; want Head=2 and Get>=2", eng.headCalls, eng.getCalls)
	}
}

func TestWarmFailedPrimingZeroPrimed(t *testing.T) {
	eng := &warmObjectStub{sizes: map[string]int64{}} // Head returns ErrNotFound
	tgts := []trace.TargetInfo{{ID: 0, Name: "missing.tar", Kind: trace.TargetObject, Size: 1024}}

	res := cache.Apply(context.Background(), cache.ModeWarm, eng, tgts)
	if res.Primed != 0 {
		t.Errorf("Primed=%d, want 0 when priming failed", res.Primed)
	}
	if len(res.Limitations) == 0 {
		t.Error("expected limitation for failed warm priming")
	}
}

func TestWarmModeRejectsPartialObjectRead(t *testing.T) {
	// Head reports 1 MiB, but Get returns only the first 4 KiB with ErrShortRead.
	// primeObject must treat this as a limitation, not a successful prime.
	eng := &shortReadObjectStub{declaredSize: 1 << 20, returnable: 4096}
	tgts := []trace.TargetInfo{{ID: 0, Name: "truncated.tar", Kind: trace.TargetObject, Size: 1 << 20}}

	res := cache.Apply(context.Background(), cache.ModeWarm, eng, tgts)
	if res.Primed != 0 {
		t.Errorf("Primed=%d, want 0 (short read should not count as primed)", res.Primed)
	}
	if len(res.Limitations) == 0 {
		t.Error("expected a limitation for short object prime")
	}
}

// shortReadObjectStub reports declaredSize via Head but only ever returns
// returnable bytes from the first Get call (with ErrShortRead).
type shortReadObjectStub struct {
	declaredSize int64
	returnable   int64
}

func (e *shortReadObjectStub) Caps() engine.Capabilities {
	return engine.Capabilities{ObjectAPI: true}
}
func (e *shortReadObjectStub) Open(_ context.Context, _ string, _ engine.Mode, _ engine.OpenFlags) (engine.Handle, error) {
	return 0, engine.ErrUnsupported
}
func (e *shortReadObjectStub) Read(_ context.Context, _ engine.Handle, _, _ int64, _ []byte) (int, error) {
	return 0, engine.ErrUnsupported
}
func (e *shortReadObjectStub) Write(_ context.Context, _ engine.Handle, _ int64, _ []byte) (int, error) {
	return 0, engine.ErrUnsupported
}
func (e *shortReadObjectStub) Fsync(_ context.Context, _ engine.Handle) error {
	return engine.ErrUnsupported
}
func (e *shortReadObjectStub) Close(_ context.Context, _ engine.Handle) error {
	return engine.ErrUnsupported
}
func (e *shortReadObjectStub) Stat(_ context.Context, _ string) (engine.ObjectInfo, error) {
	return engine.ObjectInfo{}, engine.ErrUnsupported
}
func (e *shortReadObjectStub) Put(_ context.Context, _ string, _ io.Reader, _ int64) error {
	return engine.ErrUnsupported
}
func (e *shortReadObjectStub) Get(_ context.Context, _ string, _, _ int64, _ []byte) (int, error) {
	return int(e.returnable), engine.ErrShortRead
}
func (e *shortReadObjectStub) Head(_ context.Context, key string) (engine.ObjectInfo, error) {
	return engine.ObjectInfo{Name: key, Size: e.declaredSize}, nil
}
func (e *shortReadObjectStub) Delete(_ context.Context, _ string) error {
	return engine.ErrUnsupported
}

// warmObjectStub is a minimal ObjectAPI engine for warm-cache tests.
type warmObjectStub struct {
	sizes     map[string]int64
	openCalls int
	headCalls int
	getCalls  int
}

func (e *warmObjectStub) Caps() engine.Capabilities {
	return engine.Capabilities{ObjectAPI: true}
}
func (e *warmObjectStub) Open(_ context.Context, _ string, _ engine.Mode, _ engine.OpenFlags) (engine.Handle, error) {
	e.openCalls++
	return 0, engine.ErrUnsupported
}
func (e *warmObjectStub) Read(_ context.Context, _ engine.Handle, _, _ int64, _ []byte) (int, error) {
	return 0, engine.ErrUnsupported
}
func (e *warmObjectStub) Write(_ context.Context, _ engine.Handle, _ int64, _ []byte) (int, error) {
	return 0, engine.ErrUnsupported
}
func (e *warmObjectStub) Fsync(_ context.Context, _ engine.Handle) error {
	return engine.ErrUnsupported
}
func (e *warmObjectStub) Close(_ context.Context, _ engine.Handle) error {
	return engine.ErrUnsupported
}
func (e *warmObjectStub) Stat(_ context.Context, _ string) (engine.ObjectInfo, error) {
	return engine.ObjectInfo{}, engine.ErrUnsupported
}
func (e *warmObjectStub) Put(_ context.Context, _ string, _ io.Reader, _ int64) error {
	return engine.ErrUnsupported
}
func (e *warmObjectStub) Get(_ context.Context, key string, off, length int64, _ []byte) (int, error) {
	e.getCalls++
	sz, ok := e.sizes[key]
	if !ok {
		return 0, engine.ErrNotFound
	}
	remaining := sz - off
	if remaining <= 0 {
		return 0, nil
	}
	if remaining < length {
		return int(remaining), engine.ErrShortRead
	}
	return int(length), nil
}
func (e *warmObjectStub) Head(_ context.Context, key string) (engine.ObjectInfo, error) {
	e.headCalls++
	sz, ok := e.sizes[key]
	if !ok {
		return engine.ObjectInfo{}, engine.ErrNotFound
	}
	return engine.ObjectInfo{Name: key, Size: sz}, nil
}
func (e *warmObjectStub) Delete(_ context.Context, _ string) error { return engine.ErrUnsupported }

func TestColdObjectTargetRecordsNote(t *testing.T) {
	tgts := []trace.TargetInfo{{ID: 0, Name: "imagenet/shard_0.tar", Kind: trace.TargetObject}}
	res := cache.Apply(context.Background(), cache.ModeCold, mem.New(), tgts)
	// Object target → note about S3 cold handling.
	if len(res.Limitations) == 0 {
		t.Error("expected a limitation/note for object target cold mode")
	}
}

func TestUnknownModeRecordsLimitation(t *testing.T) {
	res := cache.Apply(context.Background(), "blazing-hot", mem.New(), nil)
	if len(res.Limitations) == 0 {
		t.Error("unknown mode should record a limitation")
	}
}
