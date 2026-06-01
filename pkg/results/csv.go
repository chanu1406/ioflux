package results

import (
	"encoding/csv"
	"os"
	"strconv"
)

var csvColumns = []string{
	"timestamp", "trace_path", "trace_kind", "engine", "mode",
	"num_streams", "num_ops", "max_inflight", "cache_mode", "prepare_mode",
	"duration_ns", "ops_completed", "bytes_moved", "errors",
	"read_p50_ns", "read_p99_ns", "read_p999_ns",
	"ops_per_sec", "gib_per_sec",
	"cpu_user_ns", "cpu_sys_ns",
	"backlog_events", "schedule_drift_p99_ns", "low_fidelity",
}

// AppendCSV appends r as one CSV row to path. The header is written only when
// the file does not exist or is empty, so multiple runs accumulate cleanly.
func AppendCSV(path string, r *Results) error {
	fi, statErr := os.Stat(path)
	writeHeader := statErr != nil || fi.Size() == 0

	f, err := os.OpenFile(path, os.O_WRONLY|os.O_CREATE|os.O_APPEND, 0o644)
	if err != nil {
		return err
	}
	defer f.Close()

	w := csv.NewWriter(f)
	if writeHeader {
		if err := w.Write(csvColumns); err != nil {
			return err
		}
	}
	if err := w.Write(csvRowFor(r)); err != nil {
		return err
	}
	w.Flush()
	return w.Error()
}

func csvRowFor(r *Results) []string {
	read := r.PerOpMap()["READ"]

	var opsPerSec, gibPerSec float64
	if r.DurationNS > 0 {
		secs := float64(r.DurationNS) / 1e9
		opsPerSec = float64(r.OpsCompleted) / secs
		gibPerSec = float64(r.BytesMoved) / (1 << 30) / secs
	}

	return []string{
		r.GeneratedAt,
		r.Plan.TracePath,
		r.Plan.TraceKind,
		r.Plan.Engine,
		r.Plan.Mode,
		strconv.Itoa(r.Plan.NumStreams),
		strconv.FormatInt(r.Plan.NumOps, 10),
		strconv.Itoa(r.Plan.MaxInflight),
		r.RunEnv.CacheMode,
		r.Plan.PrepareMode,
		strconv.FormatInt(r.DurationNS, 10),
		strconv.FormatInt(r.OpsCompleted, 10),
		strconv.FormatInt(r.BytesMoved, 10),
		strconv.FormatInt(r.Errors, 10),
		strconv.FormatInt(read.P50NS, 10),
		strconv.FormatInt(read.P99NS, 10),
		strconv.FormatInt(read.P999NS, 10),
		strconv.FormatFloat(opsPerSec, 'f', 3, 64),
		strconv.FormatFloat(gibPerSec, 'f', 3, 64),
		strconv.FormatInt(r.CPU.UserNS, 10),
		strconv.FormatInt(r.CPU.SysNS, 10),
		strconv.FormatInt(r.BacklogEvents, 10),
		strconv.FormatInt(r.ScheduleDrift.P99NS, 10),
		strconv.FormatBool(r.Fidelity.LowFidelity),
	}
}
