package replay_test

import (
	"context"
	"errors"
	"testing"

	"github.com/chanuollala/ioflux/pkg/replay"
	"github.com/chanuollala/ioflux/pkg/trace"
)

// TestMaterialize_HonorsContextCancellation proves the PREPARE-cancellation fix:
// dataset preparation runs under the caller's context, so a cancelled PREPARE
// phase (or a dropped coordinator connection) stops materialization instead of
// letting a large copy run to completion.
func TestMaterialize_HonorsContextCancellation(t *testing.T) {
	buf := smallTrace(t, 1, 4)
	r, err := trace.NewReader(buf)
	if err != nil {
		t.Fatalf("NewReader: %v", err)
	}
	eng := memEngineForTrace(r.Header())

	exec, err := replay.Prepare(replay.Plan{
		Engine:      eng,
		EngineName:  "mem",
		Mode:        "asap",
		PrepareMode: "materialize-synthetic",
	}, r)
	if err != nil {
		t.Fatalf("Prepare: %v", err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := exec.Materialize(ctx); !errors.Is(err, context.Canceled) {
		t.Fatalf("Materialize with cancelled ctx err=%v, want context.Canceled", err)
	}
}

// TestMaterialize_Idempotent confirms Materialize runs prep once: a second call
// is a no-op returning the same stats, so the Session (PREPARE) and a later Run
// cannot double-materialize.
func TestMaterialize_Idempotent(t *testing.T) {
	buf := smallTrace(t, 1, 4)
	r, err := trace.NewReader(buf)
	if err != nil {
		t.Fatalf("NewReader: %v", err)
	}
	eng := memEngineForTrace(r.Header())

	exec, err := replay.Prepare(replay.Plan{
		Engine:      eng,
		EngineName:  "mem",
		Mode:        "asap",
		PrepareMode: "materialize-synthetic",
	}, r)
	if err != nil {
		t.Fatalf("Prepare: %v", err)
	}

	first, err := exec.Materialize(context.Background())
	if err != nil {
		t.Fatalf("Materialize (1): %v", err)
	}
	if first.Created != 4 {
		t.Fatalf("first Created=%d, want 4 (one per shard)", first.Created)
	}
	second, err := exec.Materialize(context.Background())
	if err != nil {
		t.Fatalf("Materialize (2): %v", err)
	}
	if second.Created != first.Created {
		t.Errorf("second Created=%d, want %d (idempotent, no re-create)", second.Created, first.Created)
	}
}
