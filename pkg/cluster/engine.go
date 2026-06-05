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

	S3          s3engine.Config  `json:"s3,omitempty"`
	TargetSizes map[string]int64 `json:"target_sizes,omitempty"`
}

// BuildEngine constructs an engine from spec and returns its bucket when the
// engine uses an object-store bucket namespace.
func BuildEngine(spec EngineSpec) (engine.Engine, string, error) {
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
