package importer_test

import (
	"testing"

	"github.com/chanuollala/ioflux/pkg/importer"
)

func TestFDTable_OpenAllocatesDistinctHandles(t *testing.T) {
	tbl := importer.NewFDTable()
	h0 := tbl.Open(0, 3, 0, "/a", false)
	h1 := tbl.Open(0, 4, 1, "/b", false)
	if h0 == h1 {
		t.Fatalf("handles not distinct: %d == %d", h0, h1)
	}
	// Re-opening the same path on a new fd still gets a fresh handle.
	h2 := tbl.Open(0, 5, 0, "/a", false)
	if h2 == h0 {
		t.Fatalf("repeated open of /a reused handle %d", h2)
	}
}

func TestFDTable_ResolveCursorAdvanceSeek(t *testing.T) {
	tbl := importer.NewFDTable()
	tbl.Open(0, 3, 7, "/a", false)

	e, ok := tbl.Resolve(0, 3)
	if !ok {
		t.Fatal("Resolve: fd 3 not found")
	}
	if e.TargetID != 7 || e.Cursor != 0 || !e.HasHandle {
		t.Fatalf("entry = %+v, want targetID=7 cursor=0 hasHandle=true", e)
	}

	tbl.Advance(0, 3, 100)
	if e, _ := tbl.Resolve(0, 3); e.Cursor != 100 {
		t.Errorf("cursor after Advance = %d, want 100", e.Cursor)
	}
	tbl.SetCursor(0, 3, 50)
	if e, _ := tbl.Resolve(0, 3); e.Cursor != 50 {
		t.Errorf("cursor after SetCursor = %d, want 50", e.Cursor)
	}
}

func TestFDTable_CloseRealHandle(t *testing.T) {
	tbl := importer.NewFDTable()
	want := tbl.Open(0, 3, 0, "/a", false)

	h, hadHandle := tbl.Close(0, 3)
	if !hadHandle {
		t.Fatal("Close: hadHandle = false, want true for a real file handle")
	}
	if h != want {
		t.Errorf("Close returned handle %d, want %d", h, want)
	}
	if _, ok := tbl.Resolve(0, 3); ok {
		t.Error("fd 3 still resolvable after Close")
	}
}

func TestFDTable_DirEntryHasNoHandleAndCloseConsumed(t *testing.T) {
	tbl := importer.NewFDTable()
	tbl.OpenDir(0, 5, "/data/dir")

	if p, ok := tbl.PathFor(0, 5); !ok || p != "/data/dir" {
		t.Fatalf("PathFor = (%q,%v), want (/data/dir,true)", p, ok)
	}
	e, ok := tbl.Resolve(0, 5)
	if !ok || e.HasHandle {
		t.Fatalf("dir entry = (%+v,%v), want hasHandle=false", e, ok)
	}
	if _, hadHandle := tbl.Close(0, 5); hadHandle {
		t.Error("Close of dir entry reported hadHandle=true; want consumed silently")
	}
}

func TestFDTable_UnresolvedFd(t *testing.T) {
	tbl := importer.NewFDTable()
	if _, ok := tbl.Resolve(0, 99); ok {
		t.Error("Resolve of unknown fd returned ok=true")
	}
	if _, hadHandle := tbl.Close(0, 99); hadHandle {
		t.Error("Close of unknown fd returned hadHandle=true")
	}
}

func TestFDTable_StreamIsolation(t *testing.T) {
	tbl := importer.NewFDTable()
	h0 := tbl.Open(0, 3, 0, "/a", false)
	h1 := tbl.Open(1, 3, 1, "/b", false) // same fd number, different stream

	if h0 == h1 {
		t.Fatalf("same fd in different streams shared a handle: %d", h0)
	}
	e0, _ := tbl.Resolve(0, 3)
	e1, _ := tbl.Resolve(1, 3)
	if e0.TargetID != 0 || e1.TargetID != 1 {
		t.Errorf("stream isolation broken: e0.target=%d e1.target=%d", e0.TargetID, e1.TargetID)
	}
}
