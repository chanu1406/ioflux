// Package metrics provides HDR-histogram wrappers and per-stream recorders for
// IOFlux replay runs.
package metrics

import hdrhistogram "github.com/HdrHistogram/hdrhistogram-go"

const (
	minLatencyNS int64 = 1_000           // 1 µs — samples below this are clamped
	maxLatencyNS int64 = 100_000_000_000 // 100 s
	sigFigs      int   = 3
)

// Histogram wraps an HDR histogram configured for I/O latency measurements.
// Range 1 µs – 100 s, 3 significant digits.
type Histogram struct {
	h *hdrhistogram.Histogram
}

// New returns a new Histogram.
func New() *Histogram {
	return &Histogram{h: hdrhistogram.New(minLatencyNS, maxLatencyNS, sigFigs)}
}

// RecordValue records one latency sample in nanoseconds. Values below
// minLatencyNS are clamped to minLatencyNS so they stay within the histogram's
// trackable range.
func (h *Histogram) RecordValue(ns int64) {
	if ns < minLatencyNS {
		ns = minLatencyNS
	}
	_ = h.h.RecordValue(ns)
}

// Merge adds all samples from other into h without losing histogram precision.
func (h *Histogram) Merge(other *Histogram) {
	h.h.Merge(other.h)
}

// Percentile returns the value at the given quantile q (0.0–100.0).
func (h *Histogram) Percentile(q float64) int64 {
	return h.h.ValueAtQuantile(q)
}

// Max returns the maximum recorded value.
func (h *Histogram) Max() int64 { return h.h.Max() }

// Mean returns the arithmetic mean of all recorded values.
func (h *Histogram) Mean() float64 { return h.h.Mean() }

// TotalCount returns the number of values recorded.
func (h *Histogram) TotalCount() int64 { return h.h.TotalCount() }
