// Package cache implements cache-state controls applied before the RUN phase.
//
// cold: attempts to evict OS page cache for file targets via posix_fadvise
// DONTNEED (Linux) and logs a limitation on other platforms.
// warm: reads every target once through the engine to prime the OS cache.
//
// Cache controls are advisory — they record what was done (and what couldn't
// be done) in the Result so the report is never silently misleading.
package cache

import (
	"context"
	"fmt"

	"github.com/chanuollala/ioflux/pkg/engine"
	"github.com/chanuollala/ioflux/pkg/trace"
)

// Mode is the cache-state strategy.
type Mode string

const (
	ModeCold Mode = "cold"
	ModeWarm Mode = "warm"
)

// Result records what the cache-state control did (and what it could not do).
// Primed counts targets the warm path successfully read end-to-end; callers use
// this to decide whether to claim "replay data was pre-touched" honestly.
type Result struct {
	Actions     []string
	Limitations []string
	Primed      int
}

// Apply applies mode before the replay RUN phase. It returns a Result
// describing what was done, which the caller stores in run metadata.
// Apply is best-effort: it never returns an error; obstacles are recorded in
// Result.Limitations so the run continues with an honest report.
func Apply(ctx context.Context, mode Mode, eng engine.Engine, targets []trace.TargetInfo) Result {
	switch mode {
	case ModeCold:
		return applyCold(eng, targets)
	case ModeWarm:
		return applyWarm(ctx, eng, targets)
	default:
		return Result{Limitations: []string{fmt.Sprintf("cache: unknown mode %q", mode)}}
	}
}

// applyCold attempts page-cache eviction for file targets. Platform-specific
// logic is in cache_linux.go / cache_other.go.
func applyCold(eng engine.Engine, targets []trace.TargetInfo) Result {
	var fileTargets []trace.TargetInfo
	for _, tgt := range targets {
		if tgt.Kind == trace.TargetFile {
			fileTargets = append(fileTargets, tgt)
		}
	}

	var res Result
	if len(fileTargets) > 0 {
		if !eng.Caps().OSPageCache {
			res.Limitations = append(res.Limitations,
				"cold: engine does not use OS page cache; POSIX fadvise skipped")
		} else {
			acts, lims := applyPOSIXCold(fileTargets)
			res.Actions = acts
			res.Limitations = lims
			res.Limitations = append(res.Limitations,
				"cold: device, array, controller, and filesystem metadata caches are outside POSIX fadvise control")
		}
	}

	// Note object targets (S3): cold for S3 means disabling HTTP keep-alives.
	// S3Engine handles this via its DisableHTTPKeepAlive config; we record a
	// note here so the operator knows what was and wasn't done.
	for _, tgt := range targets {
		if tgt.Kind == trace.TargetObject {
			res.Limitations = append(res.Limitations, "S3 cold (disable HTTP keep-alives) is configured on S3Engine; no action needed here")
			break
		}
	}

	return res
}

// applyWarm reads each target once through the engine to prime the OS page
// cache (or S3 CDN layer). Object-API engines use Head+Get; file engines use
// Open+Read. Per-target outcomes are recorded in Actions/Limitations, and the
// successful count is exposed via Primed.
func applyWarm(ctx context.Context, eng engine.Engine, targets []trace.TargetInfo) Result {
	var res Result
	const chunkSize = 1 << 20
	buf := make([]byte, chunkSize)
	useObject := eng.Caps().ObjectAPI

	for _, tgt := range targets {
		var err error
		if useObject {
			err = primeObject(ctx, eng, tgt.Name, buf)
		} else {
			err = primeFile(ctx, eng, tgt.Name, buf)
		}
		if err != nil {
			res.Limitations = append(res.Limitations, fmt.Sprintf("warm: prime %q: %v", tgt.Name, err))
		} else {
			res.Actions = append(res.Actions, fmt.Sprintf("warm: primed %q", tgt.Name))
			res.Primed++
		}
	}
	return res
}

// primeFile reads all bytes of name through file APIs, discarding the data.
func primeFile(ctx context.Context, eng engine.Engine, name string, buf []byte) error {
	h, err := eng.Open(ctx, name, engine.ModeRead, engine.OpenFlagSeq)
	if err != nil {
		return err
	}
	var off int64
	for {
		n, readErr := eng.Read(ctx, h, off, int64(len(buf)), buf)
		off += int64(n)
		if readErr == engine.ErrShortRead || n == 0 {
			break
		}
		if readErr != nil {
			_ = eng.Close(ctx, h)
			return readErr
		}
	}
	return eng.Close(ctx, h)
}

// primeObject reads key end-to-end via Head+Get, discarding the data. Returns
// an error if fewer than info.Size bytes were actually read, so that callers
// don't credit partial priming as success.
func primeObject(ctx context.Context, eng engine.Engine, key string, buf []byte) error {
	info, err := eng.Head(ctx, key)
	if err != nil {
		return err
	}
	var off int64
	for off < info.Size {
		n := int64(len(buf))
		if remaining := info.Size - off; remaining < n {
			n = remaining
		}
		got, getErr := eng.Get(ctx, key, off, n, buf[:n])
		off += int64(got)
		if getErr != nil && getErr != engine.ErrShortRead {
			return getErr
		}
		if got == 0 || getErr == engine.ErrShortRead {
			break
		}
	}
	if off < info.Size {
		return fmt.Errorf("short prime: read %d of %d bytes", off, info.Size)
	}
	return nil
}
