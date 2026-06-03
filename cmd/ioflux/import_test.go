package main

import (
	"bytes"
	"compress/gzip"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/chanuollala/ioflux/pkg/trace"
)

const sampleStrace = `3201  10:00:00.000000 openat(AT_FDCWD, "/data/a.bin", O_RDONLY) = 3 <0.000010>
3201  10:00:00.000100 read(3, "abcd"..., 4096) = 4096 <0.000050>
3201  10:00:00.000200 read(3, "efgh"..., 4096) = 4096 <0.000050>
3201  10:00:00.000300 close(3) = 0 <0.000010>
`

func runImportCLI(args []string) (int, string, string) {
	var stdout, stderr bytes.Buffer
	code := runImport(args, &stdout, &stderr)
	return code, stdout.String(), stderr.String()
}

func writeTemp(t *testing.T, name, content string) string {
	t.Helper()
	p := filepath.Join(t.TempDir(), name)
	if err := os.WriteFile(p, []byte(content), 0o644); err != nil {
		t.Fatal(err)
	}
	return p
}

func validateBytes(t *testing.T, b []byte) trace.Header {
	t.Helper()
	r, err := trace.NewReader(bytes.NewReader(b))
	if err != nil {
		t.Fatalf("NewReader: %v", err)
	}
	rep, err := trace.Validate(r)
	if err != nil {
		t.Fatalf("Validate I/O error: %v", err)
	}
	if !rep.OK() {
		t.Fatalf("imported trace failed validation: %v", rep.Errors)
	}
	return rep.Header
}

func TestImportCmd_NoArgsExitsTwo(t *testing.T) {
	code, _, stderr := runImportCLI(nil)
	if code != 2 {
		t.Fatalf("exit=%d, want 2", code)
	}
	if !strings.Contains(stderr, "Usage:") {
		t.Errorf("stderr should contain usage")
	}
}

func TestImportCmd_UnknownSource(t *testing.T) {
	code, _, stderr := runImportCLI([]string{"darshan", "-o", "-", "x"})
	if code != 2 {
		t.Fatalf("exit=%d, want 2", code)
	}
	if !strings.Contains(stderr, "unknown source") {
		t.Errorf("stderr should report unknown source; got %q", stderr)
	}
}

func TestImportCmd_MissingOutput(t *testing.T) {
	in := writeTemp(t, "in.strace", sampleStrace)
	code, _, stderr := runImportCLI([]string{"strace", in})
	if code != 2 {
		t.Fatalf("exit=%d, want 2", code)
	}
	if !strings.Contains(stderr, "-o is required") {
		t.Errorf("stderr should require -o; got %q", stderr)
	}
}

func TestImportCmd_FlagsBeforeFile(t *testing.T) {
	in := writeTemp(t, "in.strace", sampleStrace)
	out := filepath.Join(t.TempDir(), "out.ioflux")
	code, stdout, stderr := runImportCLI([]string{"strace", "-o", out, in})
	if code != 0 {
		t.Fatalf("exit=%d; stderr=%s", code, stderr)
	}
	if !strings.Contains(stdout, "wrote") {
		t.Errorf("stdout should confirm write; got %q", stdout)
	}
	// Honest summary printed to stderr.
	if !strings.Contains(stderr, "capture limitations:") {
		t.Errorf("stderr should print capture limitations; got %q", stderr)
	}
	b, err := os.ReadFile(out)
	if err != nil {
		t.Fatal(err)
	}
	hdr := validateBytes(t, b)
	if hdr.Kind != trace.TraceImported {
		t.Errorf("kind=%q, want imported", hdr.Kind)
	}
}

func TestImportCmd_FlagAfterFileErrors(t *testing.T) {
	in := writeTemp(t, "in.strace", sampleStrace)
	out := filepath.Join(t.TempDir(), "out.ioflux")
	// Go's flag parser stops at the positional, so -o is never seen.
	code, _, _ := runImportCLI([]string{"strace", in, "-o", out})
	if code != 2 {
		t.Fatalf("exit=%d, want 2 (flags must precede the input file)", code)
	}
	if _, err := os.Stat(out); !os.IsNotExist(err) {
		t.Errorf("output file should not have been created on usage error")
	}
}

func TestImportCmd_Stdout(t *testing.T) {
	in := writeTemp(t, "in.strace", sampleStrace)
	code, stdout, stderr := runImportCLI([]string{"strace", "-o", "-", in})
	if code != 0 {
		t.Fatalf("exit=%d; stderr=%s", code, stderr)
	}
	validateBytes(t, []byte(stdout))
}

func TestImportCmd_GzipInput(t *testing.T) {
	dir := t.TempDir()
	gzPath := filepath.Join(dir, "in.strace.gz")
	f, err := os.Create(gzPath)
	if err != nil {
		t.Fatal(err)
	}
	gw := gzip.NewWriter(f)
	if _, err := gw.Write([]byte(sampleStrace)); err != nil {
		t.Fatal(err)
	}
	gw.Close()
	f.Close()

	out := filepath.Join(dir, "out.ioflux")
	code, _, stderr := runImportCLI([]string{"strace", "-o", out, gzPath})
	if code != 0 {
		t.Fatalf("exit=%d; stderr=%s", code, stderr)
	}
	b, err := os.ReadFile(out)
	if err != nil {
		t.Fatal(err)
	}
	validateBytes(t, b)
}

func TestImportCmd_FailureDoesNotTruncateOutput(t *testing.T) {
	dir := t.TempDir()
	out := filepath.Join(dir, "out.ioflux")
	const sentinel = "PRE-EXISTING CONTENT\n"
	if err := os.WriteFile(out, []byte(sentinel), 0o644); err != nil {
		t.Fatal(err)
	}
	// A file with gzip magic but a corrupt body fails before any output write.
	bad := filepath.Join(dir, "bad.gz")
	if err := os.WriteFile(bad, []byte{0x1f, 0x8b, 0x00, 0x01, 0x02}, 0o644); err != nil {
		t.Fatal(err)
	}

	code, _, _ := runImportCLI([]string{"strace", "-o", out, bad})
	if code == 0 {
		t.Fatalf("exit=%d, want non-zero for corrupt input", code)
	}
	got, err := os.ReadFile(out)
	if err != nil {
		t.Fatal(err)
	}
	if string(got) != sentinel {
		t.Errorf("output was modified on failed import: got %q, want sentinel unchanged", got)
	}
}
