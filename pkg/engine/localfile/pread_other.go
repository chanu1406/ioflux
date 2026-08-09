//go:build !unix

package localfile

import "os"

// readAtOnce falls back to File.ReadAt where a single-shot positional read is
// not available. ReadAt retries until buf is full, so on this platform a source
// read that came back short costs one extra zero-byte read at end-of-file. The
// transferred count and the reported outcome are the same; only the syscall
// count differs.
func readAtOnce(f *os.File, buf []byte, off int64) (int, error) {
	return f.ReadAt(buf, off)
}
