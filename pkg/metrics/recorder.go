package metrics

import (
	"sort"

	"github.com/chanuollala/ioflux/pkg/trace"
)

// Recorder accumulates per-op-type latency histograms and counters for one
// replay stream. Not safe for concurrent use; use one Recorder per stream and
// Merge after all streams finish.
type Recorder struct {
	hists  map[trace.OpKind]*Histogram
	counts map[trace.OpKind]int64
	Bytes  int64
	Errors int64

	// BacklogEvents is the number of times an op had to wait for a semaphore
	// slot because the worker-level MaxInflight cap was reached.
	BacklogEvents int64
	// BacklogBlockedNS is the total nanoseconds spent waiting for a semaphore
	// slot across all backlog events.
	BacklogBlockedNS int64
	// MaxInflightDepth is the peak number of concurrent in-flight ops observed
	// across all streams during the run.
	MaxInflightDepth int64
	// DriftHist records per-op schedule drift: actualIssue − intendedArrival.
	// Nil when no drift was recorded (e.g., asap mode with no backlog).
	DriftHist *Histogram

	// CompletionLagHist records per-op completion lag: completionTime − intendedArrival.
	// Only populated in timeline/scaled mode. Nil otherwise.
	CompletionLagHist *Histogram

	// PeakInflight is the maximum number of concurrent in-flight ops observed
	// within a single stream over the run. For strictly-sequential streams (no
	// group tags) this must never exceed 1; values >1 indicate a concurrency
	// violation. Each per-stream Recorder sets this independently; Merge takes max.
	PeakInflight int64
}

// NewRecorder returns an empty Recorder.
func NewRecorder() *Recorder {
	return &Recorder{
		hists:  make(map[trace.OpKind]*Histogram),
		counts: make(map[trace.OpKind]int64),
	}
}

// RecordDrift records one schedule-drift sample in nanoseconds. driftNS is
// the gap between actualIssue and intendedArrival for one op (clamped to ≥0
// by the caller). Must not be called concurrently.
func (r *Recorder) RecordDrift(driftNS int64) {
	if r.DriftHist == nil {
		r.DriftHist = New()
	}
	r.DriftHist.RecordValue(driftNS)
}

// RecordCompletionLag records one completion-lag sample: completionTime −
// intendedArrival. Only called in timeline/scaled mode. Must not be called
// concurrently.
func (r *Recorder) RecordCompletionLag(lagNS int64) {
	if lagNS < 0 {
		lagNS = 0
	}
	if r.CompletionLagHist == nil {
		r.CompletionLagHist = New()
	}
	r.CompletionLagHist.RecordValue(lagNS)
}

// DriftP99 returns the p99 schedule drift in nanoseconds, or 0 if no drift
// has been recorded.
func (r *Recorder) DriftP99() int64 {
	if r.DriftHist == nil {
		return 0
	}
	return r.DriftHist.Percentile(99)
}

// Record records one op completion. latencyNS is the op's wall-clock duration;
// bytesN is the number of bytes transferred (0 for non-I/O ops); errored
// indicates the op returned a non-nil error.
func (r *Recorder) Record(kind trace.OpKind, latencyNS, bytesN int64, errored bool) {
	h, ok := r.hists[kind]
	if !ok {
		h = New()
		r.hists[kind] = h
	}
	h.RecordValue(latencyNS)
	r.counts[kind]++
	r.Bytes += bytesN
	if errored {
		r.Errors++
	}
}

// Merge adds all samples and counters from other into r. Called from the
// single-threaded aggregation step after all stream goroutines complete.
func (r *Recorder) Merge(other *Recorder) {
	for kind, oh := range other.hists {
		h, ok := r.hists[kind]
		if !ok {
			h = New()
			r.hists[kind] = h
		}
		h.Merge(oh)
		r.counts[kind] += other.counts[kind]
	}
	r.Bytes += other.Bytes
	r.Errors += other.Errors
	r.BacklogEvents += other.BacklogEvents
	r.BacklogBlockedNS += other.BacklogBlockedNS
	if other.MaxInflightDepth > r.MaxInflightDepth {
		r.MaxInflightDepth = other.MaxInflightDepth
	}
	if other.DriftHist != nil {
		if r.DriftHist == nil {
			r.DriftHist = New()
		}
		r.DriftHist.Merge(other.DriftHist)
	}
	if other.CompletionLagHist != nil {
		if r.CompletionLagHist == nil {
			r.CompletionLagHist = New()
		}
		r.CompletionLagHist.Merge(other.CompletionLagHist)
	}
	if other.PeakInflight > r.PeakInflight {
		r.PeakInflight = other.PeakInflight
	}
}

// Histogram returns the Histogram for kind, or nil if no ops of that kind were
// recorded.
func (r *Recorder) Histogram(kind trace.OpKind) *Histogram {
	return r.hists[kind]
}

// Count returns the ops-completed count for kind.
func (r *Recorder) Count(kind trace.OpKind) int64 {
	return r.counts[kind]
}

// TotalOps returns the total number of ops recorded across all kinds.
func (r *Recorder) TotalOps() int64 {
	var n int64
	for _, c := range r.counts {
		n += c
	}
	return n
}

// OpKinds returns the op kinds that have been recorded, in sorted order.
func (r *Recorder) OpKinds() []trace.OpKind {
	kinds := make([]trace.OpKind, 0, len(r.hists))
	for k := range r.hists {
		kinds = append(kinds, k)
	}
	sort.Slice(kinds, func(i, j int) bool { return kinds[i] < kinds[j] })
	return kinds
}
