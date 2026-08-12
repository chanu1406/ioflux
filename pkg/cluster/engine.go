// Package cluster contains reusable coordinator/worker building blocks.
package cluster

import (
	"fmt"

	"github.com/chanuollala/ioflux/pkg/engine"
	"github.com/chanuollala/ioflux/pkg/engine/localfile"
	"github.com/chanuollala/ioflux/pkg/engine/mem"
	s3engine "github.com/chanuollala/ioflux/pkg/engine/s3"
)

// EngineSpec is the serializable storage-engine configuration shared by the
// CLI and future workers.
type EngineSpec struct {
	Name      string `json:"name"`
	CacheMode string `json:"cache_mode,omitempty"`

	AllowDirect    bool  `json:"allow_direct,omitempty"`
	DirectFallback bool  `json:"direct_fallback,omitempty"`
	DirectAlign    int64 `json:"direct_align,omitempty"`

	// Root confines the engine to one region of its namespace: a directory for
	// local, a key prefix for s3. Empty applies no containment, and BuildEngine
	// rejects a root on mem, which has nothing persistent to confine.
	//
	// Root is not carried over the gRPC wire, so a coordinator cannot impose one
	// on a remote worker — the host owning the data decides, via
	// `ioflux worker --target-root`. `ioflux run` rejects --target-root with
	// --hosts so a coordinator-side root can never silently fail to apply.
	Root string `json:"root,omitempty"`

	S3          s3engine.Config  `json:"s3,omitempty"`
	TargetSizes map[string]int64 `json:"target_sizes,omitempty"`
}

// BuildEngine constructs an engine from spec and returns its bucket when the
// engine uses an object-store bucket namespace.
func BuildEngine(spec EngineSpec) (engine.Engine, string, error) {
	// Fail closed: a containment root the chosen engine cannot enforce must
	// stop the run, never be silently dropped. The mem engine has no persistent
	// namespace to confine, so accepting a root there would imply a guarantee
	// that means nothing.
	if spec.Root != "" && spec.Name != "local" && spec.Name != "s3" {
		return nil, "", fmt.Errorf("target root is only supported by the local and s3 engines, not %q", spec.Name)
	}
	switch spec.Name {
	case "mem":
		sizeMap := make(map[string]int64, len(spec.TargetSizes))
		for target, size := range spec.TargetSizes {
			sizeMap[target] = size
		}
		return mem.New(mem.WithSizeFunc(func(target string) int64 {
			if sz, ok := sizeMap[target]; ok && sz > 0 {
				return sz
			}
			return 64 << 20
		})), "", nil
	case "local":
		return localfile.New(
			localfile.WithAllowDirect(spec.AllowDirect),
			localfile.WithDirectFallback(spec.DirectFallback),
			localfile.WithDirectAlign(spec.DirectAlign),
			localfile.WithRoot(spec.Root),
		), "", nil
	case "s3":
		cfg := spec.S3
		cfg.DisableHTTPKeepAlive = spec.CacheMode == "cold"
		// For an object store the containment root is a key prefix within the
		// configured bucket.
		cfg.KeyPrefix = spec.Root
		eng, err := s3engine.New(cfg)
		if err != nil {
			return nil, "", err
		}
		return eng, cfg.Bucket, nil
	default:
		return nil, "", fmt.Errorf("unsupported engine %q (currently supported: mem, local, s3)", spec.Name)
	}
}
