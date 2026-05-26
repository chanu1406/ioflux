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
}

// NewRecorder returns an empty Recorder.
func NewRecorder() *Recorder {
	return &Recorder{
		hists:  make(map[trace.OpKind]*Histogram),
		counts: make(map[trace.OpKind]int64),
	}
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
