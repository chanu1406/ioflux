//go:build linux

package cache

import (
	"fmt"
	"os"
	"syscall"

	"github.com/chanuollala/ioflux/pkg/trace"
)

// applyPOSIXCold calls posix_fadvise(POSIX_FADV_DONTNEED) on each file target
// to hint the kernel to evict the file's pages from the page cache.
func applyPOSIXCold(targets []trace.TargetInfo) (actions, limitations []string) {
	for _, tgt := range targets {
		f, err := os.Open(tgt.Name)
		if err != nil {
			limitations = append(limitations, fmt.Sprintf("cold: cannot open %q for fadvise: %v", tgt.Name, err))
			continue
		}
		ferr := syscall.Fadvise(int(f.Fd()), 0, 0, syscall.FADV_DONTNEED)
		f.Close()
		if ferr != nil {
			limitations = append(limitations, fmt.Sprintf("cold: fadvise DONTNEED %q: %v", tgt.Name, ferr))
		} else {
			actions = append(actions, fmt.Sprintf("cold: fadvised DONTNEED %q", tgt.Name))
		}
	}
	return
}
