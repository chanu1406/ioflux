// Package trainingread generates synthetic training-read traces.
//
// The generated trace models a sharded WebDataset-style read workload: N shard
// files partitioned across W DataLoader worker streams, each reading one shard
// at a time (OPEN → READ* → CLOSE) for E epochs. All streams are strictly
// sequential and do not use group tags.
//
// Each op advances the owning stream's logical clock by opDeltaNS. Streams
// start at t=0 and are merge-sorted before writing so timestamps remain
// globally non-decreasing.
package trainingread

import (
	"fmt"
	"io"
	"math/rand/v2"
	"slices"

	"github.com/chanuollala/ioflux/pkg/trace"
)

// Params configures the training-read generator. Use DefaultParams for a valid
// starting point.
type Params struct {
	Shards            int
	ShardSize         int64  // bytes per shard
	RecordSize        int64  // mean record size in bytes (lognormal centre)
	RecordSizeDist    string // only "lognormal" is supported
	Epochs            int
	DataloaderWorkers int
	PrefetchDepth     int    // accepted for CLI compatibility
	Shuffle           bool   // shuffle shard order each epoch
	ReadWithinShard   string // "sequential" | "random"
	Seed              int64

	// CreatedUTC, if non-empty, is written to the trace header as created_utc.
	// Leave empty for deterministic (byte-identical) output across runs.
	// The CLI sets this to time.Now().UTC().Format(time.RFC3339).
	CreatedUTC string
}

// DefaultParams returns the training-read defaults.
func DefaultParams() Params {
	return Params{
		Shards:            1024,
		ShardSize:         64 << 20,  // 64 MiB
		RecordSize:        512 << 10, // 512 KiB
		RecordSizeDist:    "lognormal",
		Epochs:            1,
		DataloaderWorkers: 8,
		PrefetchDepth:     2,
		Shuffle:           true,
		ReadWithinShard:   "sequential",
		Seed:              42,
	}
}

// opDeltaNS is the per-op logical clock advance.
const opDeltaNS int64 = 1_000_000 // 1 ms

// streamOp holds an op alongside the metadata needed to merge-sort streams
// before writing. localIdx is the op's position within its stream; it is used
// as a tiebreaker so ops from the same stream and same t appear in generation
// order.
type streamOp struct {
	streamID int64
	localIdx int64
	op       trace.Op
}

// Generate writes a synthetic training-read trace to w. It is deterministic:
// identical Params (including Seed) with an empty CreatedUTC produce
// byte-identical output across runs.
func Generate(p Params, w io.Writer) error {
	if err := ValidateParams(p); err != nil {
		return err
	}

	rng := rand.New(rand.NewPCG(uint64(p.Seed), 0))
	sampler := newLognormalSampler(rng, p.RecordSize)

	targets := buildTargets(p)

	perStream := make([][]streamOp, p.DataloaderWorkers)
	streamT := make([]int64, p.DataloaderWorkers) // per-stream logical clock (ns)
	var nextHandle int64 = 1
	var totalBytes int64

	for range p.Epochs {
		shardOrder := makeShardOrder(p.Shards, p.Shuffle, rng)

		for pos, shardIdx := range shardOrder {
			wi := pos % p.DataloaderWorkers
			t := &streamT[wi]
			s := &perStream[wi]

			h := nextHandle
			nextHandle++

			// OPEN
			openFlags := openFlagsFor(p.ReadWithinShard)
			appendOp(s, trace.Op{
				T: *t, S: int64(wi), Op: trace.OpOpen,
				Tgt: trace.Ptr(shardIdx), H: trace.Ptr(h),
				Mode: trace.ModeRead, Flags: openFlags,
			})
			*t += opDeltaNS

			// READ ops — consume exactly ShardSize bytes per shard.
			reads, bytes := buildReads(p, sampler, rng, h, int64(wi), *t)
			for _, r := range reads {
				appendOp(s, r.op)
			}
			totalBytes += bytes
			*t += opDeltaNS * int64(len(reads))

			// CLOSE
			appendOp(s, trace.Op{
				T: *t, S: int64(wi), Op: trace.OpClose,
				H: trace.Ptr(h),
			})
			*t += opDeltaNS
		}
	}

	// Merge all streams, sorted by (t, streamID, localIdx). This ensures the
	// written file satisfies the global non-decreasing timestamp invariant even
	// though streams run concurrently from t=0.
	all := mergeAndSort(perStream)

	// Assign sequential global op_ids. After merge-sort, ops from the same
	// stream appear in their original order (stream t is strictly increasing),
	// so per-stream op_ids are automatically strictly increasing.
	for i := range all {
		id := int64(i)
		all[i].op.OpID = &id
	}

	var durationNS int64
	if len(all) > 0 {
		durationNS = all[len(all)-1].op.T
	}

	hdr := trace.Header{
		Version:       trace.TraceFormatVersion,
		Kind:          trace.TraceSynthetic,
		Profile:       "training-read",
		GeneratedBy:   "ioflux-gen 0.1.0",
		CreatedUTC:    p.CreatedUTC,
		TimeUnit:      trace.TimeUnitNanoseconds,
		CaptureMethod: trace.CaptureSynthetic,
		Targets:       targets,
		Summary: trace.Summary{
			NumOps:     int64(len(all)),
			NumStreams: p.DataloaderWorkers,
			NumGroups:  0,
			TotalBytes: totalBytes,
			DurationNS: durationNS,
		},
		Notes: buildNotes(p),
	}

	tw := trace.NewWriter(w)
	if err := tw.WriteHeader(hdr); err != nil {
		return err
	}
	for _, sop := range all {
		if err := tw.WriteOp(sop.op); err != nil {
			return err
		}
	}
	return nil
}

func buildTargets(p Params) []trace.TargetInfo {
	targets := make([]trace.TargetInfo, p.Shards)
	for i := range targets {
		targets[i] = trace.TargetInfo{
			ID:   i,
			Name: fmt.Sprintf("shard_%04d.tar", i),
			Kind: trace.TargetFile,
			Size: p.ShardSize,
		}
	}
	return targets
}

func openFlagsFor(readWithinShard string) []string {
	if readWithinShard == "random" {
		return []string{"rand"}
	}
	return []string{"seq"}
}

func makeShardOrder(shards int, shuffle bool, rng *rand.Rand) []int {
	order := make([]int, shards)
	for i := range order {
		order[i] = i
	}
	if shuffle {
		rng.Shuffle(len(order), func(i, j int) { order[i], order[j] = order[j], order[i] })
	}
	return order
}

// appendOp appends op to s, setting localIdx to the current stream length.
func appendOp(s *[]streamOp, op trace.Op) {
	*s = append(*s, streamOp{
		streamID: op.S,
		localIdx: int64(len(*s)),
		op:       op,
	})
}

// buildReads generates the READ ops for one shard. The logical clock starts at
// startT and advances by opDeltaNS per read. The reads collectively consume
// exactly ShardSize bytes. Returns the ops and total bytes.
func buildReads(p Params, sampler *lognormalSampler, rng *rand.Rand, h, streamID, startT int64) ([]streamOp, int64) {
	type readSpec struct{ off, size int64 }
	var specs []readSpec
	remaining := p.ShardSize
	off := int64(0)
	for remaining > 0 {
		size := sampler.Sample()
		if size > remaining {
			size = remaining
		}
		specs = append(specs, readSpec{off, size})
		off += size
		remaining -= size
	}
	if p.ReadWithinShard == "random" {
		rng.Shuffle(len(specs), func(i, j int) { specs[i], specs[j] = specs[j], specs[i] })
	}

	ops := make([]streamOp, len(specs))
	t := startT
	var total int64
	for i, sp := range specs {
		o, sz := sp.off, sp.size
		ops[i] = streamOp{
			streamID: streamID,
			op: trace.Op{
				T: t, S: streamID, Op: trace.OpRead,
				H: trace.Ptr(h), Off: trace.Ptr(o), Len: trace.Ptr(sz),
			},
		}
		total += sz
		t += opDeltaNS
	}
	return ops, total
}

func mergeAndSort(perStream [][]streamOp) []streamOp {
	total := 0
	for _, s := range perStream {
		total += len(s)
	}
	all := make([]streamOp, 0, total)
	for _, s := range perStream {
		all = append(all, s...)
	}
	slices.SortStableFunc(all, func(a, b streamOp) int {
		if a.op.T != b.op.T {
			if a.op.T < b.op.T {
				return -1
			}
			return 1
		}
		if a.streamID != b.streamID {
			if a.streamID < b.streamID {
				return -1
			}
			return 1
		}
		if a.localIdx < b.localIdx {
			return -1
		}
		if a.localIdx > b.localIdx {
			return 1
		}
		return 0
	})
	return all
}

func buildNotes(p Params) string {
	return fmt.Sprintf(
		"%d shards × %d MiB, %d dataloader-workers, %d epoch(s), shuffle=%v, within-shard=%s",
		p.Shards, p.ShardSize>>20, p.DataloaderWorkers, p.Epochs, p.Shuffle, p.ReadWithinShard,
	)
}

// ValidateParams checks that p is internally consistent before generation.
// Generate calls this automatically; CLI callers may call it earlier to avoid
// truncating an existing output file on invalid parameters.
func ValidateParams(p Params) error {
	switch {
	case p.Shards <= 0:
		return fmt.Errorf("gen: shards must be > 0, got %d", p.Shards)
	case p.ShardSize <= 0:
		return fmt.Errorf("gen: shard-size must be > 0, got %d", p.ShardSize)
	case p.RecordSize <= 0:
		return fmt.Errorf("gen: record-size must be > 0, got %d", p.RecordSize)
	case p.RecordSize > p.ShardSize:
		return fmt.Errorf("gen: record-size %d exceeds shard-size %d", p.RecordSize, p.ShardSize)
	case p.RecordSizeDist != "lognormal":
		return fmt.Errorf("gen: record-size-dist %q not supported (only lognormal is supported)", p.RecordSizeDist)
	case p.Epochs <= 0:
		return fmt.Errorf("gen: epochs must be > 0, got %d", p.Epochs)
	case p.DataloaderWorkers <= 0:
		return fmt.Errorf("gen: dataloader-workers must be > 0, got %d", p.DataloaderWorkers)
	case p.ReadWithinShard != "sequential" && p.ReadWithinShard != "random":
		return fmt.Errorf("gen: read-within-shard must be sequential or random, got %q", p.ReadWithinShard)
	}
	return nil
}
