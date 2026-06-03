// Package fidelity builds the replay-fidelity report that answers "how
// faithfully did the replay reproduce the trace?" Every run emits this report
// so users are never left interpreting throughput numbers without knowing
// whether the replay stayed on schedule.
//
// Terminology used throughout:
//   - Schedule drift: actualIssueTime − intendedArrivalTime (> 0 means late)
//   - Completion lag: completionTime − intendedArrivalTime (always ≥ drift)
//   - Backlog:        an op that was ready to issue but blocked on the
//     worker-level max-inflight semaphore
package fidelity

import (
	"fmt"
	"sort"

	"github.com/chanuollala/ioflux/pkg/metrics"
	"github.com/chanuollala/ioflux/pkg/trace"
)

// LowFidelityDriftFraction is the p99 schedule-drift threshold as a fraction
// of the mean inter-arrival period. Exposed as a var so tests can override.
var LowFidelityDriftFraction = 0.10

// LowFidelityDriftFallbackNS is used when mean inter-arrival is undefined
// (0 or unknown). 10 ms is a reasonable human-perceptible I/O latency floor.
var LowFidelityDriftFallbackNS = int64(10_000_000) // 10 ms

// LowFidelityBacklogFraction is the maximum fraction of ops allowed to have
// been backlog-blocked before the run is flagged low-fidelity.
var LowFidelityBacklogFraction = 0.05

// PercentileSummary holds latency-style percentiles for a histogram.
type PercentileSummary struct {
	P50NS  int64   `json:"p50_ns"`
	P99NS  int64   `json:"p99_ns"`
	P999NS int64   `json:"p999_ns"`
	MaxNS  int64   `json:"max_ns"`
	MeanNS float64 `json:"mean_ns"`
}

// BacklogSummary summarises the worker-level max-inflight cap's impact.
type BacklogSummary struct {
	TotalEvents           int64   `json:"total_events"`
	TotalBlockedNS        int64   `json:"total_blocked_ns"`
	PeakInflightDepth     int64   `json:"peak_inflight_depth"`
	FractionOpsBacklogged float64 `json:"fraction_ops_backlogged"`
}

// CoverageSummary records how completely the trace was replayed.
type CoverageSummary struct {
	OpsInTrace int64 `json:"ops_in_trace"`
	OpsIssued  int64 `json:"ops_issued"`
	OpsSkipped int64 `json:"ops_skipped"`
}

// ConcurrencyCheck confirms that no stream ran more than one op in-flight at
// a time (the strict-sequentiality invariant for no-group traces).
type ConcurrencyCheck struct {
	MaxPerStreamInflight int64   `json:"max_per_stream_inflight"`
	Violations           []int64 `json:"violations,omitempty"`
}

// FidelityReport is attached to every Results and summarises how faithfully
// the replay reproduced the trace.
type FidelityReport struct {
	ScheduleDrift     PercentileSummary `json:"schedule_drift"`
	CompletionLag     PercentileSummary `json:"completion_lag"`
	Backlog           BacklogSummary    `json:"backlog"`
	Coverage          CoverageSummary   `json:"coverage"`
	ConcurrencyCheck  ConcurrencyCheck  `json:"concurrency_check"`
	LowFidelity       bool              `json:"low_fidelity"`
	LowFidelityReason string            `json:"low_fidelity_reason,omitempty"`
}

// Build constructs a FidelityReport from the merged post-run recorder,
// the trace header, the replay mode, the trace's mean inter-arrival time,
// and the per-stream peak-inflight map collected by the scheduler.
//
// peakByStream maps stream ID → maximum in-flight count observed in that
// stream. For strictly-sequential streams this should never exceed 1.
func Build(
	rec *metrics.Recorder,
	hdr trace.Header,
	mode string,
	meanInterArrivalNS int64,
	peakByStream map[int64]int64,
) FidelityReport {
	var r FidelityReport

	// --- Schedule drift and completion lag (timeline/scaled only) ---
	if rec.DriftHist != nil {
		r.ScheduleDrift = summaryOf(rec.DriftHist)
	}
	if rec.CompletionLagHist != nil {
		r.CompletionLag = summaryOf(rec.CompletionLagHist)
	}

	// --- Backlog ---
	opsIssued := rec.TotalOps()
	r.Backlog = BacklogSummary{
		TotalEvents:       rec.BacklogEvents,
		TotalBlockedNS:    rec.BacklogBlockedNS,
		PeakInflightDepth: rec.MaxInflightDepth,
	}
	if opsIssued > 0 {
		r.Backlog.FractionOpsBacklogged = float64(rec.BacklogEvents) / float64(opsIssued)
	}

	// --- Coverage ---
	r.Coverage = CoverageSummary{
		OpsInTrace: hdr.Summary.NumOps,
		OpsIssued:  opsIssued,
		OpsSkipped: hdr.Summary.NumOps - opsIssued,
	}
	if r.Coverage.OpsSkipped < 0 {
		r.Coverage.OpsSkipped = 0
	}

	// --- Concurrency check ---
	r.ConcurrencyCheck = buildConcurrencyCheck(peakByStream)

	// --- Low-fidelity gate ---
	r.LowFidelity, r.LowFidelityReason = assessFidelity(r, meanInterArrivalNS, mode)

	return r
}

// summaryOf converts a histogram into a PercentileSummary.
func summaryOf(h *metrics.Histogram) PercentileSummary {
	return PercentileSummary{
		P50NS:  h.Percentile(50),
		P99NS:  h.Percentile(99),
		P999NS: h.Percentile(99.9),
		MaxNS:  h.Max(),
		MeanNS: h.Mean(),
	}
}

// buildConcurrencyCheck scans peakByStream and flags violations (peak > 1).
func buildConcurrencyCheck(peakByStream map[int64]int64) ConcurrencyCheck {
	var cc ConcurrencyCheck
	sids := make([]int64, 0, len(peakByStream))
	for sid := range peakByStream {
		sids = append(sids, sid)
	}
	sort.Slice(sids, func(i, j int) bool { return sids[i] < sids[j] })
	for _, sid := range sids {
		peak := peakByStream[sid]
		if peak > cc.MaxPerStreamInflight {
			cc.MaxPerStreamInflight = peak
		}
		if peak > 1 {
			cc.Violations = append(cc.Violations, sid)
		}
	}
	return cc
}

// assessFidelity returns (lowFidelity bool, reason string).
func assessFidelity(r FidelityReport, meanInterArrivalNS int64, mode string) (bool, string) {
	// Only timeline/scaled modes produce meaningful drift data.
	isTimeline := mode == "timeline" || mode == "scaled"

	if isTimeline {
		threshold := LowFidelityDriftFallbackNS
		if meanInterArrivalNS > 0 {
			threshold = int64(float64(meanInterArrivalNS) * LowFidelityDriftFraction)
		}
		if r.ScheduleDrift.P99NS > threshold {
			return true, fmt.Sprintf(
				"p99 schedule drift %dns exceeds %.0f%% of mean inter-arrival %dns (threshold %dns)",
				r.ScheduleDrift.P99NS,
				LowFidelityDriftFraction*100,
				meanInterArrivalNS,
				threshold,
			)
		}
	}

	if r.Backlog.FractionOpsBacklogged > LowFidelityBacklogFraction {
		return true, fmt.Sprintf(
			"%.1f%% of ops were backlog-blocked (threshold %.0f%%)",
			r.Backlog.FractionOpsBacklogged*100,
			LowFidelityBacklogFraction*100,
		)
	}

	if r.Coverage.OpsIssued < r.Coverage.OpsInTrace {
		return true, fmt.Sprintf(
			"%d of %d ops were skipped",
			r.Coverage.OpsSkipped, r.Coverage.OpsInTrace,
		)
	}

	return false, ""
}
