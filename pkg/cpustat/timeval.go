package cpustat

import "syscall"

// timevalNS converts a syscall.Timeval to nanoseconds. The Timeval struct's
// field types differ across Unix platforms (Linux: int64; Darwin: int32 Usec)
// but both convert cleanly to int64.
func timevalNS(tv syscall.Timeval) int64 {
	return int64(tv.Sec)*1_000_000_000 + int64(tv.Usec)*1_000
}
