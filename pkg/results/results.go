// Package results defines the Results struct written to results.json at the end
// of a replay run.
package results

import (
	"encoding/json"
	"io"
	"time"

	"github.com/chanuollala/ioflux/pkg/fidelity"
	"github.com/chanuollala/ioflux/pkg/metrics"
	"github.com/chanuollala/ioflux/pkg/trace"
)

// PlanInfo records the replay configuration echoed into results.json.
type PlanInfo struct {
	TracePath                 string  `json:"trace_path"`
	Engine                    string  `json:"engine"`
	Mode                      string  `json:"mode"`
	MaxInflight               int     `json:"max_inflight"`
	SpeedupFactor             float64 `json:"speedup_factor,omitempty"`
	TraceKind                 string  `json:"trace_kind"`
	NumStreams                int     `json:"num_streams"`
	NumOps                    int64   `json:"num_ops"`
	TotalBytes                int64   `json:"total_bytes"`
	PrepareMode               string  `json:"prepare_mode,omitempty"`
	PrepareTouchedSameData    bool    `json:"prepare_touched_same_data,omitempty"`
	PrepareVerified           int     `json:"prepare_verified,omitempty"`
	PrepareCreated            int     `json:"prepare_created,omitempty"`
	PrepareCopied             int     `json:"prepare_copied,omitempty"`
	PrepareSkippedSizeUnknown int     `json:"prepare_skipped_size_unknown,omitempty"`
	PrepareDerivedSizeFromOps int     `json:"prepare_derived_size_from_ops,omitempty"`
}

// PerOpStats holds latency percentiles and counters for one op type.
type PerOpStats struct {
	OpType string  `json:"op_type"`
	Count  int64   `json:"count"`
	P50NS  int64   `json:"p50_ns"`
	P90NS  int64   `json:"p90_ns"`
	P99NS  int64   `json:"p99_ns"`
	P999NS int64   `json:"p999_ns"`
	MaxNS  int64   `json:"max_ns"`
	MeanNS float64 `json:"mean_ns"`
}

// DriftStats summarizes schedule drift (actualIssue − intendedArrival) for a
// run. Zero values indicate the field was not measured (e.g., asap mode).
type DriftStats struct {
	P99NS  int64   `json:"p99_ns"`
	P999NS int64   `json:"p999_ns"`
	MaxNS  int64   `json:"max_ns"`
	MeanNS float64 `json:"mean_ns"`
}

// RunEnv records the environment state applied before the RUN phase.
type RunEnv struct {
	CacheMode        string   `json:"cache_mode,omitempty"`
	CacheActions     []string `json:"cache_actions,omitempty"`
	CacheLimitations []string `json:"cache_limitations,omitempty"`
}

// CPU records per-process CPU time consumed by the run. Reported alongside
// throughput so a CPU-bound result is not mistaken for a storage-bound one.
type CPU struct {
	UserNS int64 `json:"user_ns"`
	SysNS  int64 `json:"sys_ns"`
	WallNS int64 `json:"wall_ns"`
}

// Results is the full output of a replay run written to results.json.
type Results struct {
	GeneratedAt      string       `json:"generated_at"`
	Plan             PlanInfo     `json:"plan"`
	RunEnv           RunEnv       `json:"run_env"`
	DurationNS       int64        `json:"duration_ns"`
	OpsCompleted     int64        `json:"ops_completed"`
	BytesMoved       int64        `json:"bytes_moved"`
	Errors           int64        `json:"errors"`
	PerOpStats       []PerOpStats `json:"per_op_stats"`
	BacklogEvents    int64        `json:"backlog_events"`
	BacklogBlockedNS int64        `json:"backlog_blocked_ns"`
	// MaxInflightDepth is the peak concurrent in-flight op count.
	MaxInflightDepth int64                   `json:"max_backlog_depth"`
	ScheduleDrift    DriftStats              `json:"schedule_drift"`
	CPU              CPU                     `json:"cpu"`
	Fidelity         fidelity.FidelityReport `json:"fidelity"`
}

// Build constructs a Results from a merged Recorder, a plan, run environment
// metadata, and the measured run duration. Caller may set Results.CPU after.
func Build(plan PlanInfo, runEnv RunEnv, rec *metrics.Recorder, durationNS int64) *Results {
	kinds := rec.OpKinds()
	stats := make([]PerOpStats, 0, len(kinds))
	for _, k := range kinds {
		h := rec.Histogram(k)
		if h == nil {
			continue
		}
		stats = append(stats, PerOpStats{
			OpType: string(k),
			Count:  rec.Count(k),
			P50NS:  h.Percentile(50),
			P90NS:  h.Percentile(90),
			P99NS:  h.Percentile(99),
			P999NS: h.Percentile(99.9),
			MaxNS:  h.Max(),
			MeanNS: h.Mean(),
		})
	}
	r := &Results{
		GeneratedAt:      time.Now().UTC().Format(time.RFC3339),
		Plan:             plan,
		RunEnv:           runEnv,
		DurationNS:       durationNS,
		OpsCompleted:     rec.TotalOps(),
		BytesMoved:       rec.Bytes,
		Errors:           rec.Errors,
		PerOpStats:       stats,
		BacklogEvents:    rec.BacklogEvents,
		BacklogBlockedNS: rec.BacklogBlockedNS,
		MaxInflightDepth: rec.MaxInflightDepth,
	}
	if dh := rec.DriftHist; dh != nil {
		r.ScheduleDrift = DriftStats{
			P99NS:  dh.Percentile(99),
			P999NS: dh.Percentile(99.9),
			MaxNS:  dh.Max(),
			MeanNS: dh.Mean(),
		}
	}
	return r
}

// WriteJSON writes r as indented JSON to w.
func WriteJSON(w io.Writer, r *Results) error {
	enc := json.NewEncoder(w)
	enc.SetIndent("", "  ")
	return enc.Encode(r)
}

// PerOpMap returns a map from op type string to PerOpStats for quick lookup.
func (r *Results) PerOpMap() map[string]PerOpStats {
	m := make(map[string]PerOpStats, len(r.PerOpStats))
	for _, s := range r.PerOpStats {
		m[s.OpType] = s
	}
	return m
}

// AllOpKinds enumerates all op kinds present in a trace's op list. Used by
// callers that need to know which per-op-type histograms should be non-empty.
func AllOpKinds(ops []trace.Op) map[trace.OpKind]struct{} {
	m := make(map[trace.OpKind]struct{})
	for _, op := range ops {
		m[op.Op] = struct{}{}
	}
	return m
}
