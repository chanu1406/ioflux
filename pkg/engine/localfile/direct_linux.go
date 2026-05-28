//go:build linux

package localfile

import "syscall"

// openDirectFlag is OR'd into the syscall open flags when O_DIRECT is
// requested and the engine was created with WithAllowDirect(true).
const openDirectFlag = syscall.O_DIRECT

// canDirect reports whether the running platform supports O_DIRECT.
const canDirect = true
