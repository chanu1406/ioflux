//go:build !linux

package localfile

// openDirectFlag is 0 on non-Linux platforms where O_DIRECT is unavailable.
const openDirectFlag = 0

// canDirect reports whether the running platform supports O_DIRECT.
const canDirect = false
