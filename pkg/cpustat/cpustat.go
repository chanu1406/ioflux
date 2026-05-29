// Package cpustat samples per-process CPU usage via getrusage(RUSAGE_SELF).
//
// CPU time is reported alongside throughput because a CPU-bound result is
// commonly mistaken for a storage-bound one — S3 client TLS, checksumming,
// and SDK work can dominate wall time on object-store backends.
package cpustat

import "syscall"

// Sample is a point-in-time snapshot of process-cumulative CPU time.
type Sample struct {
	UserNS int64
	SysNS  int64
}

// Now returns a Sample for the calling process. UserNS/SysNS come from
// getrusage(RUSAGE_SELF). On platforms where getrusage fails, the returned
// fields are zero. Wall-clock duration is intentionally not measured here —
// callers should compute it from a monotonic clock (e.g. time.Since).
func Now() Sample {
	var ru syscall.Rusage
	_ = syscall.Getrusage(syscall.RUSAGE_SELF, &ru)
	return Sample{
		UserNS: timevalNS(ru.Utime),
		SysNS:  timevalNS(ru.Stime),
	}
}

// Sub returns s − other (per-field). Use it to compute the delta between two
// samples taken at the start and end of a run.
func (s Sample) Sub(other Sample) Sample {
	return Sample{
		UserNS: s.UserNS - other.UserNS,
		SysNS:  s.SysNS - other.SysNS,
	}
}
