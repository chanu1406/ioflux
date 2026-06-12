package metrics

import (
	hdrhistogram "github.com/HdrHistogram/hdrhistogram-go"

	"github.com/chanuollala/ioflux/pkg/trace"
)

// HistSnapshot is a serializable, lossless HDR histogram snapshot.
type HistSnapshot struct {
	Low     int64   `json:"low"`
	High    int64   `json:"high"`
	SigFigs int     `json:"sig_figs"`
	Counts  []int64 `json:"counts"`
}

// RecorderSnapshot is a serializable, lossless snapshot of a Recorder.
type RecorderSnapshot struct {
	Histograms        map[trace.OpKind]HistSnapshot `json:"histograms,omitempty"`
	ServiceHistograms map[trace.OpKind]HistSnapshot `json:"service_histograms,omitempty"`
	DriftHist         *HistSnapshot                 `json:"drift_hist,omitempty"`
	CompletionLagHist *HistSnapshot                 `json:"completion_lag_hist,omitempty"`
	Counts            map[trace.OpKind]int64        `json:"counts,omitempty"`

	Bytes            int64 `json:"bytes"`
	Errors           int64 `json:"errors"`
	ShortReads       int64 `json:"short_reads"`
	BacklogEvents    int64 `json:"backlog_events"`
	BacklogBlockedNS int64 `json:"backlog_blocked_ns"`
	MaxInflightDepth int64 `json:"max_inflight_depth"`
	PeakInflight     int64 `json:"peak_inflight"`
}

// Export returns a lossless snapshot of h. Trailing zero buckets are trimmed:
// the default 1µs–100s range has tens of thousands of buckets but real runs
// populate a small prefix, and ImportHistogram reconstructs the tail, so
// trimming keeps results.json and the gRPC Collect payload proportional to
// the recorded data.
func (h *Histogram) Export() HistSnapshot {
	if h == nil || h.h == nil {
		return HistSnapshot{Low: minLatencyNS, High: maxLatencyNS, SigFigs: sigFigs}
	}
	s := h.h.Export()
	counts := s.Counts
	n := len(counts)
	for n > 0 && counts[n-1] == 0 {
		n--
	}
	return HistSnapshot{
		Low:     s.LowestTrackableValue,
		High:    s.HighestTrackableValue,
		SigFigs: int(s.SignificantFigures),
		Counts:  append([]int64(nil), counts[:n]...),
	}
}

// ImportHistogram reconstructs a Histogram from a lossless snapshot. Counts
// shorter than the range's full bucket array (Export trims trailing zeros)
// are zero-padded back to full length, which hdrhistogram.Import requires.
func ImportHistogram(s HistSnapshot) *Histogram {
	low := s.Low
	if low <= 0 {
		low = minLatencyNS
	}
	high := s.High
	if high <= 0 {
		high = maxLatencyNS
	}
	sig := s.SigFigs
	if sig <= 0 {
		sig = sigFigs
	}
	if len(s.Counts) == 0 {
		return &Histogram{h: hdrhistogram.New(low, high, sig)}
	}
	counts := append([]int64(nil), s.Counts...)
	if need := len(hdrhistogram.New(low, high, sig).Export().Counts); len(counts) < need {
		counts = append(counts, make([]int64, need-len(counts))...)
	}
	return &Histogram{h: hdrhistogram.Import(&hdrhistogram.Snapshot{
		LowestTrackableValue:  low,
		HighestTrackableValue: high,
		SignificantFigures:    int64(sig),
		Counts:                counts,
	})}
}

// Export returns a lossless snapshot of r.
func (r *Recorder) Export() RecorderSnapshot {
	if r == nil {
		return RecorderSnapshot{}
	}
	s := RecorderSnapshot{
		Histograms:        make(map[trace.OpKind]HistSnapshot, len(r.hists)),
		ServiceHistograms: make(map[trace.OpKind]HistSnapshot, len(r.serviceHists)),
		Counts:            make(map[trace.OpKind]int64, len(r.counts)),
		Bytes:             r.Bytes,
		Errors:            r.Errors,
		ShortReads:        r.ShortReads,
		BacklogEvents:     r.BacklogEvents,
		BacklogBlockedNS:  r.BacklogBlockedNS,
		MaxInflightDepth:  r.MaxInflightDepth,
		PeakInflight:      r.PeakInflight,
	}
	for kind, h := range r.hists {
		s.Histograms[kind] = h.Export()
	}
	for kind, h := range r.serviceHists {
		s.ServiceHistograms[kind] = h.Export()
	}
	for kind, count := range r.counts {
		s.Counts[kind] = count
	}
	if r.DriftHist != nil {
		drift := r.DriftHist.Export()
		s.DriftHist = &drift
	}
	if r.CompletionLagHist != nil {
		lag := r.CompletionLagHist.Export()
		s.CompletionLagHist = &lag
	}
	return s
}

// ImportRecorder reconstructs a Recorder from a lossless snapshot.
func ImportRecorder(s RecorderSnapshot) *Recorder {
	r := NewRecorder()
	r.Bytes = s.Bytes
	r.Errors = s.Errors
	r.ShortReads = s.ShortReads
	r.BacklogEvents = s.BacklogEvents
	r.BacklogBlockedNS = s.BacklogBlockedNS
	r.MaxInflightDepth = s.MaxInflightDepth
	r.PeakInflight = s.PeakInflight

	for kind, hs := range s.Histograms {
		r.hists[kind] = ImportHistogram(hs)
	}
	for kind, hs := range s.ServiceHistograms {
		r.serviceHists[kind] = ImportHistogram(hs)
	}
	for kind, count := range s.Counts {
		r.counts[kind] = count
	}
	if s.DriftHist != nil {
		r.DriftHist = ImportHistogram(*s.DriftHist)
	}
	if s.CompletionLagHist != nil {
		r.CompletionLagHist = ImportHistogram(*s.CompletionLagHist)
	}
	return r
}
