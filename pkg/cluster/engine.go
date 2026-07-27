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

	// Root confines the local engine to a directory: every target must resolve
	// inside it, so a trace cannot read or overwrite data elsewhere on the host.
	// Empty applies no containment. Only the local engine can honor it;
	// BuildEngine rejects it for any other engine rather than ignoring it.
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
	// stop the run, never be silently dropped.
	if spec.Root != "" && spec.Name != "local" {
		return nil, "", fmt.Errorf("target root is only supported by the local engine, not %q", spec.Name)
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
		eng, err := s3engine.New(cfg)
		if err != nil {
			return nil, "", err
		}
		return eng, cfg.Bucket, nil
	default:
		return nil, "", fmt.Errorf("unsupported engine %q (currently supported: mem, local, s3)", spec.Name)
	}
}
