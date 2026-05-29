// Package prepare implements the three dataset preparation modes for IOFlux
// replay runs. Preparation runs before the replay executor starts, so its I/O
// is never credited to results.
package prepare

import (
	"context"
	"fmt"
	"io"
	"os"
	"path/filepath"

	"github.com/chanuollala/ioflux/pkg/engine"
	"github.com/chanuollala/ioflux/pkg/trace"
)

// Mode names a dataset preparation strategy.
type Mode string

const (
	// ModeAssumeExisting verifies that targets already exist on the backend and
	// checks sizes where known. Fails fast on a missing or size-mismatched target.
	ModeAssumeExisting Mode = "assume-existing"

	// ModeMaterializeSynthetic creates byte-equivalent dummy objects of the sizes
	// recorded in the trace's target table (or derived from READ/WRITE op extents).
	// Default for synthetic traces.
	ModeMaterializeSynthetic Mode = "materialize-synthetic"

	// ModeMaterializeFromSource copies real objects from a local source root into
	// the replay backend before the run.
	ModeMaterializeFromSource Mode = "materialize-from-source"
)

// Stats summarizes what the preparer did.
type Stats struct {
	Verified           int  // assume-existing: targets verified present+correct
	Created            int  // materialize-synthetic: objects created
	Copied             int  // materialize-from-source: objects copied
	SkippedSizeUnknown int  // assume-existing: target present but Size==0 (not verified)
	DerivedSizeFromOps int  // materialize-synthetic: size was derived from op extents
	TouchedSameData    bool // true when prep I/O overlaps with replay read targets
}

// Preparer performs dataset preparation before replay starts. targets is the
// rewritten target slice the engine will see; originalTargets is the
// pre-rewrite slice from the trace header (originalTargets == targets when no
// target-map was applied). materialize-from-source uses originalTargets to
// locate source files; other modes ignore it.
type Preparer interface {
	Prepare(ctx context.Context, targets, originalTargets []trace.TargetInfo, ops []trace.Op, eng engine.Engine) (Stats, error)
}

// For returns a Preparer for mode. sourceRoot is required only for
// ModeMaterializeFromSource (the local FS path to copy from).
func For(mode Mode, sourceRoot string) (Preparer, error) {
	switch mode {
	case ModeAssumeExisting:
		return &assumeExisting{}, nil
	case ModeMaterializeSynthetic:
		return &materializeSynthetic{}, nil
	case ModeMaterializeFromSource:
		if sourceRoot == "" {
			return nil, fmt.Errorf("prepare: %s requires --source-root", mode)
		}
		return &materializeFromSource{root: sourceRoot}, nil
	default:
		return nil, fmt.Errorf("prepare: unknown mode %q (want assume-existing|materialize-synthetic|materialize-from-source)", mode)
	}
}

const prepareChunkSize = 1 << 20 // 1 MiB reused buffer

// --- assume-existing ---

type assumeExisting struct{}

func (a *assumeExisting) Prepare(ctx context.Context, targets, _ []trace.TargetInfo, _ []trace.Op, eng engine.Engine) (Stats, error) {
	var stats Stats
	useHead := eng.Caps().ObjectAPI
	for _, tgt := range targets {
		var info engine.ObjectInfo
		var err error
		if useHead {
			info, err = eng.Head(ctx, tgt.Name)
		} else {
			info, err = eng.Stat(ctx, tgt.Name)
		}
		if err != nil {
			return stats, fmt.Errorf("prepare: assume-existing: target %q: %w", tgt.Name, err)
		}
		if tgt.Size == 0 {
			// Size unknown in trace — can't verify, skip.
			stats.SkippedSizeUnknown++
		} else if info.Size != tgt.Size {
			return stats, fmt.Errorf("prepare: assume-existing: target %q: backend size %d does not match expected %d", tgt.Name, info.Size, tgt.Size)
		} else {
			stats.Verified++
		}
	}
	return stats, nil
}

// --- materialize-synthetic ---

type materializeSynthetic struct{}

func (m *materializeSynthetic) Prepare(ctx context.Context, targets, _ []trace.TargetInfo, ops []trace.Op, eng engine.Engine) (Stats, error) {
	sizes := computeRequiredSizes(targets, ops)
	var stats Stats
	buf := make([]byte, prepareChunkSize) // allocated once; reused across all targets

	for _, tgt := range targets {
		size, ok := sizes[tgt.Name]
		if !ok || size == 0 {
			return stats, fmt.Errorf("prepare: materialize-synthetic: target %q: size unknown and no READ/WRITE ops found; re-run with --prepare assume-existing if target is already provisioned", tgt.Name)
		}
		if err := writeTarget(ctx, eng, tgt.Name, size, buf); err != nil {
			return stats, fmt.Errorf("prepare: materialize-synthetic: %w", err)
		}
		stats.Created++
		if tgt.Size == 0 {
			stats.DerivedSizeFromOps++
		}
	}
	stats.TouchedSameData = true
	return stats, nil
}

// computeRequiredSizes returns the minimum required byte size per target.
// It uses TargetInfo.Size when > 0; otherwise it scans ops for max(off+len).
func computeRequiredSizes(targets []trace.TargetInfo, ops []trace.Op) map[string]int64 {
	sizes := make(map[string]int64, len(targets))
	for _, tgt := range targets {
		if tgt.Size > 0 {
			sizes[tgt.Name] = tgt.Size
		}
	}

	// Map handle → target index from OPEN ops so we can look up targets for
	// READ/WRITE ops (which carry h, not tgt).
	handleToIdx := make(map[int64]int)
	for _, op := range ops {
		if op.Op == trace.OpOpen && op.H != nil && op.Tgt != nil {
			handleToIdx[*op.H] = *op.Tgt
		}
	}

	// For targets with Size==0, derive from max(off+len) of READ/WRITE ops.
	for _, op := range ops {
		if (op.Op != trace.OpRead && op.Op != trace.OpWrite) || op.Off == nil || op.Len == nil || op.H == nil {
			continue
		}
		idx, ok := handleToIdx[*op.H]
		if !ok || idx >= len(targets) {
			continue
		}
		tgt := targets[idx]
		if tgt.Size > 0 {
			continue // authoritative size already covers this target
		}
		end := *op.Off + *op.Len
		if cur := sizes[tgt.Name]; end > cur {
			sizes[tgt.Name] = end
		}
	}
	return sizes
}

// writeTarget creates or replaces target with size zero bytes, streamed in
// prepareChunkSize chunks from buf.
func writeTarget(ctx context.Context, eng engine.Engine, name string, size int64, buf []byte) error {
	if eng.Caps().ObjectAPI {
		return eng.Put(ctx, name, &zeroReadSeeker{size: size}, size)
	}
	return writePOSIX(ctx, eng, name, size, buf)
}

func writePOSIX(ctx context.Context, eng engine.Engine, name string, size int64, buf []byte) error {
	h, err := eng.Open(ctx, name, engine.ModeWrite, engine.OpenFlagCreate|engine.OpenFlagTrunc)
	if err != nil {
		return fmt.Errorf("writeTarget %q: open: %w", name, err)
	}
	var off int64
	for off < size {
		n := int64(len(buf))
		if remaining := size - off; remaining < n {
			n = remaining
		}
		written, writeErr := eng.Write(ctx, h, off, buf[:n])
		off += int64(written)
		if writeErr != nil {
			_ = eng.Close(ctx, h)
			return fmt.Errorf("writeTarget %q: write at %d: %w", name, off-int64(written), writeErr)
		}
	}
	return eng.Close(ctx, h)
}

// zeroReadSeeker is a seekable source of zero bytes used for Put-based materialization.
type zeroReadSeeker struct {
	off, size int64
}

func (z *zeroReadSeeker) Read(p []byte) (int, error) {
	if z.off >= z.size {
		return 0, io.EOF
	}
	n := int64(len(p))
	if remain := z.size - z.off; remain < n {
		n = remain
	}
	for i := int64(0); i < n; i++ {
		p[i] = 0
	}
	z.off += n
	return int(n), nil
}

func (z *zeroReadSeeker) Seek(offset int64, whence int) (int64, error) {
	switch whence {
	case io.SeekStart:
		z.off = offset
	case io.SeekCurrent:
		z.off += offset
	case io.SeekEnd:
		z.off = z.size + offset
	}
	if z.off < 0 {
		return 0, fmt.Errorf("seek before start")
	}
	return z.off, nil
}

// --- materialize-from-source ---

type materializeFromSource struct{ root string }

func (m *materializeFromSource) Prepare(ctx context.Context, targets, originalTargets []trace.TargetInfo, _ []trace.Op, eng engine.Engine) (Stats, error) {
	var stats Stats
	buf := make([]byte, prepareChunkSize)
	if len(originalTargets) == 0 {
		originalTargets = targets
	}
	if len(originalTargets) != len(targets) {
		return stats, fmt.Errorf("prepare: materialize-from-source: originalTargets length %d != targets length %d", len(originalTargets), len(targets))
	}

	for i, tgt := range targets {
		srcPath := filepath.Join(m.root, originalTargets[i].Name)
		if err := copyTarget(ctx, eng, tgt.Name, srcPath, buf); err != nil {
			return stats, fmt.Errorf("prepare: materialize-from-source: %w", err)
		}
		stats.Copied++
	}
	stats.TouchedSameData = true
	return stats, nil
}

func copyTarget(ctx context.Context, eng engine.Engine, name, srcPath string, buf []byte) error {
	src, err := os.Open(srcPath)
	if err != nil {
		return fmt.Errorf("open source %q: %w", srcPath, err)
	}
	defer src.Close()

	if eng.Caps().ObjectAPI {
		fi, err := src.Stat()
		if err != nil {
			return fmt.Errorf("stat source %q: %w", srcPath, err)
		}
		return eng.Put(ctx, name, io.NewSectionReader(src, 0, fi.Size()), fi.Size())
	}

	h, err := eng.Open(ctx, name, engine.ModeWrite, engine.OpenFlagCreate|engine.OpenFlagTrunc)
	if err != nil {
		return fmt.Errorf("open dest %q: %w", name, err)
	}
	var off int64
	for {
		n, readErr := src.Read(buf)
		if n > 0 {
			written, writeErr := eng.Write(ctx, h, off, buf[:n])
			off += int64(written)
			if writeErr != nil {
				_ = eng.Close(ctx, h)
				return fmt.Errorf("write dest %q at %d: %w", name, off-int64(written), writeErr)
			}
		}
		if readErr == io.EOF {
			break
		}
		if readErr != nil {
			_ = eng.Close(ctx, h)
			return fmt.Errorf("read source %q: %w", srcPath, readErr)
		}
	}
	return eng.Close(ctx, h)
}
