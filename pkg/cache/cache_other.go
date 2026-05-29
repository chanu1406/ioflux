//go:build !linux

package cache

import "github.com/chanuollala/ioflux/pkg/trace"

// applyPOSIXCold records a platform limitation; posix_fadvise DONTNEED is
// only implemented on Linux.
func applyPOSIXCold(targets []trace.TargetInfo) (actions, limitations []string) {
	if len(targets) == 0 {
		return
	}
	return nil, []string{"cold POSIX cache eviction (posix_fadvise DONTNEED) is not available on this platform; run on Linux for page-cache eviction"}
}
