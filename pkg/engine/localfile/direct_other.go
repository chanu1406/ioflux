//go:build !linux

package localfile

import "os"

// openDirectFlag is 0 on non-Linux platforms where O_DIRECT is unavailable.
const openDirectFlag = 0

// canDirect reports whether the running platform supports O_DIRECT.
const canDirect = false

// isDirectNotSupported always returns false on non-Linux (O_DIRECT cannot be
// requested, so this path is never reached).
func isDirectNotSupported(_ error) bool { return false }

// detectAlign returns 4096 on non-Linux; only called when direct=true, which
// canDirect prevents from happening.
func detectAlign(_ *os.File, override int64) int64 {
	if override > 0 {
		return override
	}
	return 4096
}

// alignedReadAt and alignedWriteAt are unreachable on non-Linux (direct=false
// always because canDirect=false).

func alignedReadAt(_ *os.File, _ []byte, _, _, _ int64) (int, error) {
	panic("localfile: alignedReadAt called on non-Linux platform")
}

func alignedWriteAt(_ *os.File, _ []byte, _, _ int64) (int, error) {
	panic("localfile: alignedWriteAt called on non-Linux platform")
}
