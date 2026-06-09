package cluster

import (
	"reflect"
	"testing"
	"time"
)

func TestPartitionStreams(t *testing.T) {
	tests := []struct {
		name    string
		ids     []int64
		workers int
		want    [][]int64
	}{
		{
			name:    "even split",
			ids:     []int64{0, 1, 2, 3},
			workers: 2,
			want:    [][]int64{{0, 2}, {1, 3}},
		},
		{
			name:    "more workers than streams leaves idle",
			ids:     []int64{0},
			workers: 3,
			want:    [][]int64{{0}, nil, nil},
		},
		{
			name:    "non-dense ids (importer pids)",
			ids:     []int64{2001, 2002, 2003},
			workers: 2,
			want:    [][]int64{{2001, 2003}, {2002}},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := partitionStreams(tt.ids, tt.workers)
			if !reflect.DeepEqual(got, tt.want) {
				t.Errorf("partitionStreams(%v, %d) = %v, want %v", tt.ids, tt.workers, got, tt.want)
			}
			// Disjoint union must equal the input set.
			seen := map[int64]int{}
			for _, set := range got {
				for _, id := range set {
					seen[id]++
				}
			}
			for _, id := range tt.ids {
				if seen[id] != 1 {
					t.Errorf("stream %d assigned %d times, want exactly 1", id, seen[id])
				}
			}
		})
	}
}

// TestServerLease verifies the worker lease: an active run is owned (overlapping
// acquire rejected), a live run keeps its lease via refresh, an abandoned run is
// taken over once the lease expires, and release frees the worker immediately.
func TestServerLease(t *testing.T) {
	s := NewServer()
	s.runLease = 40 * time.Millisecond

	if !s.acquire() {
		t.Fatal("first acquire failed")
	}
	if s.acquire() {
		t.Fatal("acquire during an active lease should fail")
	}

	// Refreshing keeps the run owned past one lease window.
	time.Sleep(30 * time.Millisecond)
	s.refreshLease()
	time.Sleep(30 * time.Millisecond)
	if s.acquire() {
		t.Fatal("acquire should fail while the lease is being refreshed")
	}

	// Stop refreshing: the abandoned run's lease expires and a new run takes over.
	time.Sleep(50 * time.Millisecond)
	if !s.acquire() {
		t.Fatal("acquire after lease expiry should succeed (takeover)")
	}

	// Release frees the worker immediately.
	s.release()
	if !s.acquire() {
		t.Fatal("acquire after release should succeed")
	}
}

func TestSkewNS(t *testing.T) {
	if got := skewNS([]int64{42}); got != 0 {
		t.Errorf("single worker skew=%d, want 0", got)
	}
	got := skewNS([]int64{
		(5 * time.Millisecond).Nanoseconds(),
		0,
		(2 * time.Millisecond).Nanoseconds(),
	})
	if want := (5 * time.Millisecond).Nanoseconds(); got != want {
		t.Errorf("skew=%d, want %d", got, want)
	}
}
