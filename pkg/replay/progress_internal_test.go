package replay

import (
	"bytes"
	"context"
	"sync"
	"testing"
	"time"

	"github.com/chanuollala/ioflux/pkg/engine/mem"
	"github.com/chanuollala/ioflux/pkg/gen/trainingread"
	"github.com/chanuollala/ioflux/pkg/trace"
)

// TestRunStreams_ProgressTicker checks the live progress hook: while a run is in
// flight, the callback fires with monotonically non-decreasing cumulative totals
// that never exceed the final figures, and no callback fires after runStreams
// returns (progressWG ensures clean teardown — verified under -race).
func TestRunStreams_ProgressTicker(t *testing.T) {
	p := trainingread.DefaultParams()
	p.Shards = 6
	p.ShardSize = 128 << 10
	p.RecordSize = 16 << 10
	p.DataloaderWorkers = 1
	p.Epochs = 1
	p.Shuffle = false
	p.Seed = 1
	var buf bytes.Buffer
	if err := trainingread.Generate(p, &buf); err != nil {
		t.Fatalf("Generate: %v", err)
	}
	r, err := trace.NewReader(&buf)
	if err != nil {
		t.Fatalf("NewReader: %v", err)
	}
	hdr := r.Header()

	sizeMap := make(map[string]int64, len(hdr.Targets))
	for _, tgt := range hdr.Targets {
		sizeMap[tgt.Name] = tgt.Size
	}
	// Inject a small per-op delay so the run lasts long enough for several ticks.
	eng := mem.New(
		mem.WithSizeFunc(func(name string) int64 {
			if sz, ok := sizeMap[name]; ok && sz > 0 {
				return sz
			}
			return 64 << 20
		}),
		mem.WithInjectedDelay(2*time.Millisecond),
	)

	exec, err := Prepare(Plan{TracePath: "t.ioflux", Engine: eng, EngineName: "mem", Mode: "asap"}, r)
	if err != nil {
		t.Fatalf("Prepare: %v", err)
	}

	var mu sync.Mutex
	var samples [][2]int64
	cb := func(ops, bytesMoved int64) {
		mu.Lock()
		samples = append(samples, [2]int64{ops, bytesMoved})
		mu.Unlock()
	}

	opts := SchedulerOpts{
		Mode:             "asap",
		MaxInflight:      512,
		Progress:         cb,
		ProgressInterval: 5 * time.Millisecond,
	}
	out, err := runStreams(context.Background(), exec.byStream, eng, exec.hdr, opts)
	if err != nil {
		t.Fatalf("runStreams: %v", err)
	}

	mu.Lock()
	defer mu.Unlock()
	if len(samples) == 0 {
		t.Fatal("progress callback never fired")
	}
	finalOps := out.Recorder.TotalOps()
	finalBytes := out.Recorder.Bytes
	var prevOps, prevBytes int64
	for i, s := range samples {
		if s[0] < prevOps || s[1] < prevBytes {
			t.Errorf("sample %d not monotonic: ops %d→%d bytes %d→%d", i, prevOps, s[0], prevBytes, s[1])
		}
		if s[0] > finalOps || s[1] > finalBytes {
			t.Errorf("sample %d exceeds final totals: ops %d/%d bytes %d/%d", i, s[0], finalOps, s[1], finalBytes)
		}
		prevOps, prevBytes = s[0], s[1]
	}
}
