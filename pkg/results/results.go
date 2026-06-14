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
	Profile                   string  `json:"profile,omitempty"`
	NumStreams                int     `json:"num_streams"`
	NumOps                    int64   `json:"num_ops"`
	TotalBytes                int64   `json:"total_bytes"`
	PrepareMode               string  `json:"prepare_mode,omitempty"`
	PrepareScope              string  `json:"prepare_scope,omitempty"`
	FillMode                  string  `json:"fill_mode,omitempty"`
	FillSeed                  int64   `json:"fill_seed,omitempty"`
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

// ProgressPoint records cumulative progress at a point in the run.
type ProgressPoint struct {
	ElapsedNS  int64 `json:"elapsed_ns"`
	Ops        int64 `json:"ops"`
	Bytes      int64 `json:"bytes"`
	OpsDelta   int64 `json:"ops_delta,omitempty"`
	BytesDelta int64 `json:"bytes_delta,omitempty"`
}

// RunEnv records the environment state applied before the RUN phase.
type RunEnv struct {
	CacheMode         string   `json:"cache_mode,omitempty"`
	CacheActions      []string `json:"cache_actions,omitempty"`
	CacheLimitations  []string `json:"cache_limitations,omitempty"`
	EngineLimitations []string `json:"engine_limitations,omitempty"`
}

// CPU records per-process CPU time consumed by the run. Reported alongside
// throughput so a CPU-bound result is not mistaken for a storage-bound one.
type CPU struct {
	UserNS int64 `json:"user_ns"`
	SysNS  int64 `json:"sys_ns"`
	WallNS int64 `json:"wall_ns"`
}

// HostResult records one worker's contribution to a distributed run. It is
// populated only for multi-host runs; single-node runs omit the hosts array.
type HostResult struct {
	Hostname     string `json:"hostname"`
	OpsCompleted int64  `json:"ops_completed"`
	BytesMoved   int64  `json:"bytes_moved"`
	CPU          CPU    `json:"cpu"`
	// FirstDoneNS/LastDoneNS are this worker's earliest/latest stream completion
	// times, relative to its run start.
	FirstDoneNS int64 `json:"first_done_ns"`
	LastDoneNS  int64 `json:"last_done_ns"`
}

// StragglerWindow quantifies completion skew across workers in a distributed
// run. FirstDoneNS is the earliest worker completion (throughput up to it
// excludes the straggler tail); LastDoneNS is the latest. SkewNS is the gap.
type StragglerWindow struct {
	FirstDoneNS        int64   `json:"first_done_ns"`
	LastDoneNS         int64   `json:"last_done_ns"`
	SkewNS             int64   `json:"skew_ns"`
	FirstDoneOpsPerSec float64 `json:"first_done_ops_per_sec"`
	LastDoneOpsPerSec  float64 `json:"last_done_ops_per_sec"`
	FirstDoneGiBPerSec float64 `json:"first_done_gib_per_sec"`
	LastDoneGiBPerSec  float64 `json:"last_done_gib_per_sec"`
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
	ShortReads       int64        `json:"short_reads,omitempty"`
	PerOpStats       []PerOpStats `json:"per_op_stats"`
	ServiceTimeStats []PerOpStats `json:"service_time_stats,omitempty"`
	BacklogEvents    int64        `json:"backlog_events"`
	BacklogBlockedNS int64        `json:"backlog_blocked_ns"`
	// MaxInflightDepth is the peak concurrent in-flight op count.
	MaxInflightDepth int64                   `json:"max_backlog_depth"`
	ScheduleDrift    DriftStats              `json:"schedule_drift"`
	CPU              CPU                     `json:"cpu"`
	Fidelity         fidelity.FidelityReport `json:"fidelity"`
	// HistogramSnapshot is the merged recorder in lossless form, so saved runs
	// can be re-merged or re-queried at arbitrary percentiles. (omitempty has
	// no effect on a struct field; the snapshot is always present.)
	HistogramSnapshot metrics.RecorderSnapshot `json:"histogram_snapshot"`
	TimeSeries        []ProgressPoint          `json:"time_series,omitempty"`

	// Hosts, Straggler, and GoDeliverySkewNS are populated only for distributed
	// (multi-host) runs; single-node runs omit them so their output is unchanged.
	Hosts            []HostResult     `json:"hosts,omitempty"`
	Straggler        *StragglerWindow `json:"straggler,omitempty"`
	GoDeliverySkewNS int64            `json:"go_delivery_skew_ns,omitempty"`
}

// Build constructs a Results from a merged Recorder, a plan, run environment
// metadata, and the measured run duration. Caller may set Results.CPU after.
func Build(plan PlanInfo, runEnv RunEnv, rec *metrics.Recorder, durationNS int64) *Results {
	kinds := rec.OpKinds()
	stats := make([]PerOpStats, 0, len(kinds))
	serviceStats := make([]PerOpStats, 0, len(kinds))
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
		if sh := rec.ServiceHistogram(k); sh != nil {
			serviceStats = append(serviceStats, PerOpStats{
				OpType: string(k),
				Count:  rec.Count(k),
				P50NS:  sh.Percentile(50),
				P90NS:  sh.Percentile(90),
				P99NS:  sh.Percentile(99),
				P999NS: sh.Percentile(99.9),
				MaxNS:  sh.Max(),
				MeanNS: sh.Mean(),
			})
		}
	}
	r := &Results{
		GeneratedAt:       time.Now().UTC().Format(time.RFC3339),
		Plan:              plan,
		RunEnv:            runEnv,
		DurationNS:        durationNS,
		OpsCompleted:      rec.TotalOps(),
		BytesMoved:        rec.Bytes,
		Errors:            rec.Errors,
		ShortReads:        rec.ShortReads,
		PerOpStats:        stats,
		ServiceTimeStats:  serviceStats,
		BacklogEvents:     rec.BacklogEvents,
		BacklogBlockedNS:  rec.BacklogBlockedNS,
		MaxInflightDepth:  rec.MaxInflightDepth,
		HistogramSnapshot: rec.Export(),
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
