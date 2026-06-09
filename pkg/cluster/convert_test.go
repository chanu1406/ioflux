package cluster

import (
	"reflect"
	"testing"

	clusterpb "github.com/chanuollala/ioflux/pkg/cluster/proto"
	s3engine "github.com/chanuollala/ioflux/pkg/engine/s3"
	"github.com/chanuollala/ioflux/pkg/metrics"
	"github.com/chanuollala/ioflux/pkg/replay"
	"github.com/chanuollala/ioflux/pkg/results"
	"github.com/chanuollala/ioflux/pkg/targetmap"
	"github.com/chanuollala/ioflux/pkg/trace"
	"google.golang.org/protobuf/proto"
)

// TestPlanProtoRoundTrip asserts every Plan field survives the wire encoding,
// including a real protobuf marshal/unmarshal (not just struct copying).
func TestPlanProtoRoundTrip(t *testing.T) {
	in := Plan{
		TracePath:       "/traces/run.ioflux",
		TraceBytes:      []byte("header\nop1\nop2\n"),
		AssignedStreams: []int64{2001, 4, 7},
		Engine: EngineSpec{
			Name:           "s3",
			CacheMode:      "cold",
			AllowDirect:    true,
			DirectFallback: true,
			DirectAlign:    4096,
			S3: s3engine.Config{
				Endpoint:             "http://minio:9000",
				Region:               "us-east-1",
				Bucket:               "bench",
				PathStyle:            true,
				AccessKey:            "ak",
				SecretKey:            "sk",
				SessionToken:         "st",
				MultipartThreshold:   64 << 20,
				MultipartPartSize:    16 << 20,
				DisableHTTPKeepAlive: true,
				HeadOnOpen:           true,
			},
			TargetSizes: map[string]int64{"shard_0": 1024, "shard_1": 2048},
		},
		Mode:             "timeline",
		MaxInflight:      256,
		SpeedupFactor:    2.5,
		TargetRewrite:    []targetmap.Rule{{From: "/mnt/a/", To: "s3://bench/a/"}, {From: "/mnt/b/", To: "s3://bench/b/"}},
		AllowPassthrough: true,
		PrepareMode:      "materialize-synthetic",
		SourceRoot:       "/src",
		CacheMode:        "cold",
	}

	wire, err := proto.Marshal(planToProto(in))
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	pb := &clusterpb.Plan{}
	if err := proto.Unmarshal(wire, pb); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	got := planFromProto(pb)

	if !reflect.DeepEqual(got, in) {
		t.Errorf("Plan round-trip mismatch:\n got=%+v\nwant=%+v", got, in)
	}
}

// TestRecorderSnapshotProtoRoundTrip asserts the lossless HDR snapshot survives
// the proto encoding: percentiles, counters, and drift/lag histograms are
// preserved exactly through marshal/unmarshal.
func TestRecorderSnapshotProtoRoundTrip(t *testing.T) {
	rec := metrics.NewRecorder()
	for i := int64(1); i <= 1000; i++ {
		rec.Record(trace.OpRead, i*1000, 4096, false)
		rec.RecordDrift(i * 10)
		rec.RecordCompletionLag(i * 20)
	}
	rec.Record(trace.OpOpen, 500, 0, false)
	rec.Record(trace.OpWrite, 700, 8192, true)
	rec.BacklogEvents = 3
	rec.BacklogBlockedNS = 12345
	rec.MaxInflightDepth = 9
	rec.PeakInflight = 1

	wire, err := proto.Marshal(recorderSnapshotToProto(rec.Export()))
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	pb := &clusterpb.RecorderSnapshot{}
	if err := proto.Unmarshal(wire, pb); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	got := metrics.ImportRecorder(recorderSnapshotFromProto(pb))

	if got.TotalOps() != rec.TotalOps() {
		t.Errorf("TotalOps=%d, want %d", got.TotalOps(), rec.TotalOps())
	}
	if got.Bytes != rec.Bytes {
		t.Errorf("Bytes=%d, want %d", got.Bytes, rec.Bytes)
	}
	if got.Errors != rec.Errors {
		t.Errorf("Errors=%d, want %d", got.Errors, rec.Errors)
	}
	if got.BacklogEvents != rec.BacklogEvents || got.BacklogBlockedNS != rec.BacklogBlockedNS {
		t.Errorf("backlog counters mismatch: got (%d,%d) want (%d,%d)",
			got.BacklogEvents, got.BacklogBlockedNS, rec.BacklogEvents, rec.BacklogBlockedNS)
	}
	if got.MaxInflightDepth != rec.MaxInflightDepth || got.PeakInflight != rec.PeakInflight {
		t.Errorf("inflight counters mismatch")
	}
	for _, pct := range []float64{50, 90, 99, 99.9} {
		g := got.Histogram(trace.OpRead).Percentile(pct)
		w := rec.Histogram(trace.OpRead).Percentile(pct)
		if g != w {
			t.Errorf("READ p%.1f=%d, want %d", pct, g, w)
		}
	}
	if got.DriftP99() != rec.DriftP99() {
		t.Errorf("drift p99=%d, want %d", got.DriftP99(), rec.DriftP99())
	}
	if got.CompletionLagHist.Percentile(99) != rec.CompletionLagHist.Percentile(99) {
		t.Errorf("completion-lag p99 mismatch")
	}
}

// TestWorkerOutputProtoRoundTrip checks the full WorkerResults message: recorder,
// CPU, timing, hostname, and per-stream peaks survive the wire.
func TestWorkerOutputProtoRoundTrip(t *testing.T) {
	rec := metrics.NewRecorder()
	rec.Record(trace.OpRead, 5000, 4096, false)
	out := &replay.WorkerOutput{
		Hostname:     "hostA",
		Recorder:     rec,
		PeakByStream: map[int64]int64{0: 1, 3: 1},
		CPU:          results.CPU{UserNS: 100, SysNS: 50, WallNS: 1000},
		ActualNumOps: 1,
		FirstDoneNS:  111,
		LastDoneNS:   222,
	}

	wire, err := proto.Marshal(workerOutputToProto(out))
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	pb := &clusterpb.WorkerResults{}
	if err := proto.Unmarshal(wire, pb); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	got := workerOutputFromProto(pb)

	if got.Hostname != "hostA" || got.ActualNumOps != 1 || got.FirstDoneNS != 111 || got.LastDoneNS != 222 {
		t.Errorf("scalar fields mismatch: %+v", got)
	}
	if got.CPU != out.CPU {
		t.Errorf("CPU=%+v, want %+v", got.CPU, out.CPU)
	}
	if !reflect.DeepEqual(got.PeakByStream, out.PeakByStream) {
		t.Errorf("PeakByStream=%v, want %v", got.PeakByStream, out.PeakByStream)
	}
	if got.Recorder.Count(trace.OpRead) != 1 {
		t.Errorf("recorder READ count=%d, want 1", got.Recorder.Count(trace.OpRead))
	}
}
