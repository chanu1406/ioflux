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

	// Root confines the engine to one region of its namespace, so a trace cannot
	// read or overwrite data elsewhere: a directory for the local engine, a key
	// prefix within the bucket for s3. Empty applies no containment. The mem
	// engine has nothing persistent to confine, so BuildEngine rejects a root
	// there rather than ignoring it.
	//
	// Root is deliberately NOT carried over the gRPC wire (clusterpb.EngineSpec
	// has no matching field, and regenerating it needs a protoc toolchain this
	// repo does not vendor). A coordinator therefore cannot impose a root on a
	// remote worker — which is the correct trust boundary anyway: the host that
	// owns the data decides what a plan may touch, via
	// `ioflux worker --target-root`. `ioflux run` rejects --target-root together
	// with --hosts so a coordinator-side root can never silently fail to apply.
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
