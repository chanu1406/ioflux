package cluster

import (
	"github.com/chanuollala/ioflux/pkg/cache"
	clusterpb "github.com/chanuollala/ioflux/pkg/cluster/proto"
	s3engine "github.com/chanuollala/ioflux/pkg/engine/s3"
	"github.com/chanuollala/ioflux/pkg/metrics"
	"github.com/chanuollala/ioflux/pkg/prepare"
	"github.com/chanuollala/ioflux/pkg/replay"
	"github.com/chanuollala/ioflux/pkg/results"
	"github.com/chanuollala/ioflux/pkg/targetmap"
	"github.com/chanuollala/ioflux/pkg/trace"
)

// convert.go is the single boundary between the in-memory cluster types and the
// generated protobuf wire types. The localWorker never touches it; only the gRPC
// Server (decoding requests) and remoteWorker (encoding requests / decoding
// responses) do.

// --- Plan ---

func planToProto(p Plan) *clusterpb.Plan {
	rules := make([]*clusterpb.TargetRule, len(p.TargetRewrite))
	for i, r := range p.TargetRewrite {
		rules[i] = &clusterpb.TargetRule{From: r.From, To: r.To}
	}
	return &clusterpb.Plan{
		TracePath:        p.TracePath,
		TraceBytes:       p.TraceBytes,
		AssignedStreams:  p.AssignedStreams,
		Engine:           engineSpecToProto(p.Engine),
		Mode:             p.Mode,
		MaxInflight:      int32(p.MaxInflight),
		SpeedupFactor:    p.SpeedupFactor,
		TargetRewrite:    rules,
		AllowPassthrough: p.AllowPassthrough,
		PrepareMode:      p.PrepareMode,
		SourceRoot:       p.SourceRoot,
		CacheMode:        p.CacheMode,
	}
}

func planFromProto(pb *clusterpb.Plan) Plan {
	rules := make([]targetmap.Rule, len(pb.TargetRewrite))
	for i, r := range pb.TargetRewrite {
		rules[i] = targetmap.Rule{From: r.GetFrom(), To: r.GetTo()}
	}
	return Plan{
		TracePath:        pb.GetTracePath(),
		TraceBytes:       pb.GetTraceBytes(),
		AssignedStreams:  pb.GetAssignedStreams(),
		Engine:           engineSpecFromProto(pb.GetEngine()),
		Mode:             pb.GetMode(),
		MaxInflight:      int(pb.GetMaxInflight()),
		SpeedupFactor:    pb.GetSpeedupFactor(),
		TargetRewrite:    rules,
		AllowPassthrough: pb.GetAllowPassthrough(),
		PrepareMode:      pb.GetPrepareMode(),
		SourceRoot:       pb.GetSourceRoot(),
		CacheMode:        pb.GetCacheMode(),
	}
}

// --- EngineSpec / S3 ---

func engineSpecToProto(s EngineSpec) *clusterpb.EngineSpec {
	return &clusterpb.EngineSpec{
		Name:           s.Name,
		CacheMode:      s.CacheMode,
		AllowDirect:    s.AllowDirect,
		DirectFallback: s.DirectFallback,
		DirectAlign:    s.DirectAlign,
		S3:             s3ConfigToProto(s.S3),
		TargetSizes:    s.TargetSizes,
	}
}

func engineSpecFromProto(pb *clusterpb.EngineSpec) EngineSpec {
	if pb == nil {
		return EngineSpec{}
	}
	return EngineSpec{
		Name:           pb.GetName(),
		CacheMode:      pb.GetCacheMode(),
		AllowDirect:    pb.GetAllowDirect(),
		DirectFallback: pb.GetDirectFallback(),
		DirectAlign:    pb.GetDirectAlign(),
		S3:             s3ConfigFromProto(pb.GetS3()),
		TargetSizes:    pb.GetTargetSizes(),
	}
}

func s3ConfigToProto(c s3engine.Config) *clusterpb.S3Config {
	return &clusterpb.S3Config{
		Endpoint:             c.Endpoint,
		Region:               c.Region,
		Bucket:               c.Bucket,
		PathStyle:            c.PathStyle,
		AccessKey:            c.AccessKey,
		SecretKey:            c.SecretKey,
		SessionToken:         c.SessionToken,
		MultipartThreshold:   c.MultipartThreshold,
		MultipartPartSize:    c.MultipartPartSize,
		DisableHttpKeepAlive: c.DisableHTTPKeepAlive,
		HeadOnOpen:           c.HeadOnOpen,
	}
}

func s3ConfigFromProto(pb *clusterpb.S3Config) s3engine.Config {
	if pb == nil {
		return s3engine.Config{}
	}
	return s3engine.Config{
		Endpoint:             pb.GetEndpoint(),
		Region:               pb.GetRegion(),
		Bucket:               pb.GetBucket(),
		PathStyle:            pb.GetPathStyle(),
		AccessKey:            pb.GetAccessKey(),
		SecretKey:            pb.GetSecretKey(),
		SessionToken:         pb.GetSessionToken(),
		MultipartThreshold:   pb.GetMultipartThreshold(),
		MultipartPartSize:    pb.GetMultipartPartSize(),
		DisableHTTPKeepAlive: pb.GetDisableHttpKeepAlive(),
		HeadOnOpen:           pb.GetHeadOnOpen(),
	}
}

// --- WorkerInfo ---

func workerInfoToProto(w WorkerInfo) *clusterpb.WorkerInfo {
	return &clusterpb.WorkerInfo{Hostname: w.Hostname, Cpus: int32(w.CPUs), Version: w.Version}
}

func workerInfoFromProto(pb *clusterpb.WorkerInfo) WorkerInfo {
	if pb == nil {
		return WorkerInfo{}
	}
	return WorkerInfo{Hostname: pb.GetHostname(), CPUs: int(pb.GetCpus()), Version: pb.GetVersion()}
}

// --- PrepareAck (prep stats + cache result) ---

func prepareAckToProto(r PrepareResult) *clusterpb.PrepareAck {
	return &clusterpb.PrepareAck{
		PrepStats: &clusterpb.PrepStats{
			Verified:           int32(r.PrepStats.Verified),
			Created:            int32(r.PrepStats.Created),
			Copied:             int32(r.PrepStats.Copied),
			SkippedSizeUnknown: int32(r.PrepStats.SkippedSizeUnknown),
			DerivedSizeFromOps: int32(r.PrepStats.DerivedSizeFromOps),
			TouchedSameData:    r.PrepStats.TouchedSameData,
		},
		CacheResult: &clusterpb.CacheResult{
			Actions:     r.CacheResult.Actions,
			Limitations: r.CacheResult.Limitations,
			Primed:      int32(r.CacheResult.Primed),
		},
	}
}

func prepareAckFromProto(pb *clusterpb.PrepareAck) PrepareResult {
	if pb == nil {
		return PrepareResult{}
	}
	var r PrepareResult
	if ps := pb.GetPrepStats(); ps != nil {
		r.PrepStats = prepare.Stats{
			Verified:           int(ps.GetVerified()),
			Created:            int(ps.GetCreated()),
			Copied:             int(ps.GetCopied()),
			SkippedSizeUnknown: int(ps.GetSkippedSizeUnknown()),
			DerivedSizeFromOps: int(ps.GetDerivedSizeFromOps()),
			TouchedSameData:    ps.GetTouchedSameData(),
		}
	}
	if cr := pb.GetCacheResult(); cr != nil {
		r.CacheResult = cache.Result{
			Actions:     cr.GetActions(),
			Limitations: cr.GetLimitations(),
			Primed:      int(cr.GetPrimed()),
		}
	}
	return r
}

// --- WorkerResults (recorder snapshot + timing) ---

func workerOutputToProto(out *replay.WorkerOutput) *clusterpb.WorkerResults {
	return &clusterpb.WorkerResults{
		Recorder:          recorderSnapshotToProto(out.Recorder.Export()),
		Cpu:               &clusterpb.CPU{UserNs: out.CPU.UserNS, SysNs: out.CPU.SysNS, WallNs: out.CPU.WallNS},
		ActualNumOps:      out.ActualNumOps,
		FirstDoneNs:       out.FirstDoneNS,
		LastDoneNs:        out.LastDoneNS,
		Hostname:          out.Hostname,
		PeakByStream:      out.PeakByStream,
		EngineLimitations: out.EngineLimitations,
	}
}

func workerOutputFromProto(pb *clusterpb.WorkerResults) *replay.WorkerOutput {
	out := &replay.WorkerOutput{
		Recorder:          metrics.ImportRecorder(recorderSnapshotFromProto(pb.GetRecorder())),
		ActualNumOps:      pb.GetActualNumOps(),
		FirstDoneNS:       pb.GetFirstDoneNs(),
		LastDoneNS:        pb.GetLastDoneNs(),
		Hostname:          pb.GetHostname(),
		PeakByStream:      pb.GetPeakByStream(),
		EngineLimitations: pb.GetEngineLimitations(),
	}
	if c := pb.GetCpu(); c != nil {
		out.CPU = results.CPU{UserNS: c.GetUserNs(), SysNS: c.GetSysNs(), WallNS: c.GetWallNs()}
	}
	if out.PeakByStream == nil {
		out.PeakByStream = map[int64]int64{}
	}
	return out
}

// --- RecorderSnapshot / HistSnapshot ---

func recorderSnapshotToProto(s metrics.RecorderSnapshot) *clusterpb.RecorderSnapshot {
	pb := &clusterpb.RecorderSnapshot{
		Histograms:       make(map[string]*clusterpb.HistSnapshot, len(s.Histograms)),
		Counts:           make(map[string]int64, len(s.Counts)),
		Bytes:            s.Bytes,
		Errors:           s.Errors,
		BacklogEvents:    s.BacklogEvents,
		BacklogBlockedNs: s.BacklogBlockedNS,
		MaxInflightDepth: s.MaxInflightDepth,
		PeakInflight:     s.PeakInflight,
	}
	for kind, h := range s.Histograms {
		pb.Histograms[string(kind)] = histSnapshotToProto(h)
	}
	for kind, c := range s.Counts {
		pb.Counts[string(kind)] = c
	}
	if s.DriftHist != nil {
		pb.DriftHist = histSnapshotToProto(*s.DriftHist)
	}
	if s.CompletionLagHist != nil {
		pb.CompletionLagHist = histSnapshotToProto(*s.CompletionLagHist)
	}
	return pb
}

func recorderSnapshotFromProto(pb *clusterpb.RecorderSnapshot) metrics.RecorderSnapshot {
	s := metrics.RecorderSnapshot{
		Histograms: make(map[trace.OpKind]metrics.HistSnapshot),
		Counts:     make(map[trace.OpKind]int64),
	}
	if pb == nil {
		return s
	}
	s.Bytes = pb.GetBytes()
	s.Errors = pb.GetErrors()
	s.BacklogEvents = pb.GetBacklogEvents()
	s.BacklogBlockedNS = pb.GetBacklogBlockedNs()
	s.MaxInflightDepth = pb.GetMaxInflightDepth()
	s.PeakInflight = pb.GetPeakInflight()
	for kind, h := range pb.GetHistograms() {
		s.Histograms[trace.OpKind(kind)] = histSnapshotFromProto(h)
	}
	for kind, c := range pb.GetCounts() {
		s.Counts[trace.OpKind(kind)] = c
	}
	if d := pb.GetDriftHist(); d != nil {
		h := histSnapshotFromProto(d)
		s.DriftHist = &h
	}
	if l := pb.GetCompletionLagHist(); l != nil {
		h := histSnapshotFromProto(l)
		s.CompletionLagHist = &h
	}
	return s
}

func histSnapshotToProto(h metrics.HistSnapshot) *clusterpb.HistSnapshot {
	return &clusterpb.HistSnapshot{
		Low:     h.Low,
		High:    h.High,
		SigFigs: int32(h.SigFigs),
		Counts:  h.Counts,
	}
}

func histSnapshotFromProto(pb *clusterpb.HistSnapshot) metrics.HistSnapshot {
	if pb == nil {
		return metrics.HistSnapshot{}
	}
	return metrics.HistSnapshot{
		Low:     pb.GetLow(),
		High:    pb.GetHigh(),
		SigFigs: int(pb.GetSigFigs()),
		Counts:  pb.GetCounts(),
	}
}
