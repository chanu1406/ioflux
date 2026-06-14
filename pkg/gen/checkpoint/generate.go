// Package checkpoint generates synthetic checkpoint-write traces.
//
// The generated trace models a sharded model checkpoint written by a multi-rank
// training job: the model is split into one shard per writer rank, and each rank
// writes its shard as a single file (OPEN(create,trunc) → WRITE* → [FSYNC] →
// CLOSE). A run may emit several checkpoints separated by an interval, modelling
// periodic checkpointing.
//
// Each writer rank is a strictly-sequential stream (at most one op in flight),
// so replay never adds parallelism. All ops within one rank's checkpoint share
// the checkpoint's arrival time: a checkpoint is an I/O burst issued as fast as
// the backend allows, not a paced stream. Successive checkpoints are separated
// by the checkpoint interval; with a zero interval all checkpoints collapse into
// one dense burst at t=0.
//
// fsync policy controls durability:
//   - per-file: every shard is fsync'd before close.
//   - final:    only the last checkpoint's shards are fsync'd.
//   - none:     no fsync is issued.
package checkpoint

import (
	"fmt"
	"io"
	"math"
	"slices"

	"github.com/chanuollala/ioflux/pkg/trace"
)

// Fsync policy values.
const (
	FsyncPerFile = "per-file"
	FsyncFinal   = "final"
	FsyncNone    = "none"
)

// Params configures the checkpoint-write generator. Use DefaultParams for a
// valid starting point.
type Params struct {
	ModelSize             int64   // total bytes written per checkpoint
	WriterRanks           int     // concurrent writer streams (= shards per checkpoint)
	WriteBlock            int64   // size of each WRITE call
	NumCheckpoints        int     // number of checkpoint bursts
	CheckpointIntervalSec float64 // seconds between bursts (0 = single dense burst)
	Fsync                 string  // per-file | final | none

	// CreatedUTC, if non-empty, is written to the header as created_utc. Leave
	// empty for deterministic (byte-identical) output across runs. The CLI sets
	// this to time.Now().UTC().Format(time.RFC3339).
	CreatedUTC string
}

// DefaultParams returns the checkpoint-write defaults.
func DefaultParams() Params {
	return Params{
		ModelSize:             16 << 30, // 16 GiB
		WriterRanks:           8,
		WriteBlock:            4 << 20, // 4 MiB
		NumCheckpoints:        1,
		CheckpointIntervalSec: 0,
		Fsync:                 FsyncPerFile,
	}
}

// streamOp holds an op alongside the metadata needed to merge-sort streams
// before writing. localIdx is the op's position within its stream; it is used as
// a tiebreaker so ops from the same stream and same t keep generation order.
type streamOp struct {
	streamID int64
	localIdx int64
	op       trace.Op
}

// Generate writes a synthetic checkpoint-write trace to w. It is deterministic:
// identical Params with an empty CreatedUTC produce byte-identical output across
// runs.
func Generate(p Params, w io.Writer) error {
	if err := ValidateParams(p); err != nil {
		return err
	}

	targets := buildTargets(p)

	perStream := make([][]streamOp, p.WriterRanks)
	streamT := make([]int64, p.WriterRanks) // per-stream logical clock (ns)
	var nextHandle int64 = 1
	var totalBytes int64

	intervalNS := int64(p.CheckpointIntervalSec * 1e9)

	for c := 0; c < p.NumCheckpoints; c++ {
		burstNS := int64(c) * intervalNS
		isFinal := c == p.NumCheckpoints-1

		for r := 0; r < p.WriterRanks; r++ {
			// A checkpoint is a burst: every op in this rank's checkpoint is due at
			// the burst start. max() keeps the per-stream clock non-decreasing when a
			// prior burst overran the interval (back-to-back bursts).
			t := burstNS
			if streamT[r] > t {
				t = streamT[r]
			}
			streamT[r] = t

			tgtID := c*p.WriterRanks + r
			size := targets[tgtID].Size
			h := nextHandle
			nextHandle++
			s := &perStream[r]

			// OPEN — create+trunc: the run itself creates the shard file.
			appendOp(s, trace.Op{
				T: t, S: int64(r), Op: trace.OpOpen,
				Tgt: trace.Ptr(tgtID), H: trace.Ptr(h),
				Mode: trace.ModeWrite, Flags: []string{"create", "trunc"},
			})

			// WRITE ops covering the whole shard in WriteBlock-sized calls.
			var off int64
			for off < size {
				n := p.WriteBlock
				if rem := size - off; rem < n {
					n = rem
				}
				appendOp(s, trace.Op{
					T: t, S: int64(r), Op: trace.OpWrite,
					H: trace.Ptr(h), Off: trace.Ptr(off), Len: trace.Ptr(n),
				})
				off += n
				totalBytes += n
			}

			// FSYNC per policy.
			if p.Fsync == FsyncPerFile || (p.Fsync == FsyncFinal && isFinal) {
				appendOp(s, trace.Op{
					T: t, S: int64(r), Op: trace.OpFsync, H: trace.Ptr(h),
				})
			}

			// CLOSE
			appendOp(s, trace.Op{
				T: t, S: int64(r), Op: trace.OpClose, H: trace.Ptr(h),
			})
		}
	}

	// Merge all streams, sorted by (t, streamID, localIdx), so the written file
	// is globally non-decreasing in t while each stream keeps its own order.
	all := mergeAndSort(perStream)

	// Assign sequential global op_ids in file order. Ops from the same stream stay
	// in their original order, so per-stream op_ids are strictly increasing.
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
		Profile:       "checkpoint-write",
		GeneratedBy:   "ioflux-gen 0.1.0",
		CreatedUTC:    p.CreatedUTC,
		TimeUnit:      trace.TimeUnitNanoseconds,
		CaptureMethod: trace.CaptureSynthetic,
		Targets:       targets,
		Summary: trace.Summary{
			NumOps:     int64(len(all)),
			NumStreams: p.WriterRanks,
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

// buildTargets builds one shard file per (checkpoint, rank). The model is split
// evenly across ranks; the remainder folds into the last rank so the per-
// checkpoint bytes sum to exactly ModelSize.
func buildTargets(p Params) []trace.TargetInfo {
	base := p.ModelSize / int64(p.WriterRanks)
	rem := p.ModelSize % int64(p.WriterRanks)
	targets := make([]trace.TargetInfo, 0, p.NumCheckpoints*p.WriterRanks)
	for c := 0; c < p.NumCheckpoints; c++ {
		for r := 0; r < p.WriterRanks; r++ {
			size := base
			if r == p.WriterRanks-1 {
				size += rem
			}
			targets = append(targets, trace.TargetInfo{
				ID:   c*p.WriterRanks + r,
				Name: fmt.Sprintf("checkpoint_%04d/shard_%04d.pt", c, r),
				Kind: trace.TargetFile,
				Size: size,
			})
		}
	}
	return targets
}

// appendOp appends op to s, setting localIdx to the current stream length.
func appendOp(s *[]streamOp, op trace.Op) {
	*s = append(*s, streamOp{
		streamID: op.S,
		localIdx: int64(len(*s)),
		op:       op,
	})
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
		"%d checkpoint(s) × %d writer-ranks, %d MiB model, %d MiB write-block, interval %gs, fsync=%s",
		p.NumCheckpoints, p.WriterRanks, p.ModelSize>>20, p.WriteBlock>>20,
		p.CheckpointIntervalSec, p.Fsync,
	)
}

// ValidateParams checks that p is internally consistent before generation.
// Generate calls this automatically; CLI callers may call it earlier to avoid
// truncating an existing output file on invalid parameters.
func ValidateParams(p Params) error {
	switch {
	case p.ModelSize <= 0:
		return fmt.Errorf("gen: model-size must be > 0, got %d", p.ModelSize)
	case p.WriterRanks <= 0:
		return fmt.Errorf("gen: writer-ranks must be > 0, got %d", p.WriterRanks)
	case int64(p.WriterRanks) > p.ModelSize:
		return fmt.Errorf("gen: writer-ranks %d exceeds model-size %d bytes (each rank needs at least 1 byte)", p.WriterRanks, p.ModelSize)
	case p.WriteBlock <= 0:
		return fmt.Errorf("gen: write-block must be > 0, got %d", p.WriteBlock)
	case p.NumCheckpoints <= 0:
		return fmt.Errorf("gen: num-checkpoints must be > 0, got %d", p.NumCheckpoints)
	case math.IsNaN(p.CheckpointIntervalSec) || math.IsInf(p.CheckpointIntervalSec, 0):
		return fmt.Errorf("gen: checkpoint-interval must be a finite number, got %v", p.CheckpointIntervalSec)
	case p.CheckpointIntervalSec < 0:
		return fmt.Errorf("gen: checkpoint-interval must be >= 0, got %v", p.CheckpointIntervalSec)
	case p.Fsync != FsyncPerFile && p.Fsync != FsyncFinal && p.Fsync != FsyncNone:
		return fmt.Errorf("gen: fsync must be %s, %s, or %s, got %q", FsyncPerFile, FsyncFinal, FsyncNone, p.Fsync)
	}
	return nil
}
