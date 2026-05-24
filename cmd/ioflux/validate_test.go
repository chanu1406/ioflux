package main

import (
	"bytes"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

// fixturePath returns the absolute path to a testdata fixture in pkg/trace.
// Tests in cmd/ioflux reuse those fixtures rather than maintaining duplicates.
func fixturePath(t *testing.T, name string) string {
	t.Helper()
	wd, err := os.Getwd()
	if err != nil {
		t.Fatalf("Getwd: %v", err)
	}
	// from cmd/ioflux/ → ../../pkg/trace/testdata/
	return filepath.Join(wd, "..", "..", "pkg", "trace", "testdata", name)
}

func runCLI(args []string) (int, string, string) {
	var stdout, stderr bytes.Buffer
	code := runValidate(args, &stdout, &stderr)
	return code, stdout.String(), stderr.String()
}

func TestValidateCmd_ValidFixtureExitsZero(t *testing.T) {
	code, stdout, stderr := runCLI([]string{fixturePath(t, "minimal_valid.ioflux")})
	if code != 0 {
		t.Fatalf("exit=%d want 0; stdout=%s stderr=%s", code, stdout, stderr)
	}
	if !strings.Contains(stdout, "OK") {
		t.Errorf("stdout should contain OK, got: %s", stdout)
	}
	if !strings.Contains(stdout, "kind             synthetic") {
		t.Errorf("stdout should show kind synthetic, got: %s", stdout)
	}
}

func TestValidateCmd_CapturedValidPrintsCaptureMethod(t *testing.T) {
	code, stdout, stderr := runCLI([]string{fixturePath(t, "captured_valid.ioflux")})
	if code != 0 {
		t.Fatalf("exit=%d want 0; stdout=%s stderr=%s", code, stdout, stderr)
	}
	if !strings.Contains(stdout, "capture_method   python-io-hooks") {
		t.Errorf("stdout should show capture_method, got: %s", stdout)
	}
	if !strings.Contains(stdout, "kind             captured") {
		t.Errorf("stdout should show kind captured, got: %s", stdout)
	}
}

func TestValidateCmd_MissingCloseShowsWarning(t *testing.T) {
	// Construct a temp trace with an unclosed handle.
	tmp, err := os.CreateTemp("", "ioflux-warn-*.ioflux")
	if err != nil {
		t.Fatalf("CreateTemp: %v", err)
	}
	defer os.Remove(tmp.Name())
	content := `{"ioflux_trace_version":1,"kind":"synthetic","time_unit":"ns","scrubbed":false,"targets":[{"id":0,"name":"a","kind":"file","size":1024}],"summary":{"num_ops":2,"num_streams":1,"num_groups":0,"total_bytes":1024,"duration_ns":0}}
{"t":0,"op_id":0,"s":0,"op":"OPEN","tgt":0,"h":42,"mode":"r"}
{"t":1,"op_id":1,"s":0,"op":"READ","h":42,"off":0,"len":1024}
`
	if _, err := tmp.WriteString(content); err != nil {
		t.Fatalf("write: %v", err)
	}
	tmp.Close()

	code, stdout, _ := runCLI([]string{tmp.Name()})
	if code != 0 {
		t.Fatalf("exit=%d want 0 (warnings don't fail), stdout=%s", code, stdout)
	}
	if !strings.Contains(stdout, "warnings:") {
		t.Errorf("stdout should list warnings, got: %s", stdout)
	}
	if !strings.Contains(stdout, "never CLOSEd") {
		t.Errorf("stdout should mention never CLOSEd, got: %s", stdout)
	}
}

func TestValidateCmd_InvalidFixtureExitsOne(t *testing.T) {
	cases := []struct {
		fixture string
		want    string // substring required in stdout
	}{
		{"out_of_order_t.ioflux", "non-monotonic timestamp"},
		{"read_without_open.ioflux", "unknown handle"},
		{"tgt_out_of_range.ioflux", "out of range"},
		{"missing_required_header.ioflux", "ioflux_trace_version"},
		{"wrong_version.ioflux", "unsupported version"},
		{"captured_missing_method.ioflux", "capture_method"},
	}
	for _, c := range cases {
		t.Run(c.fixture, func(t *testing.T) {
			code, stdout, stderr := runCLI([]string{fixturePath(t, c.fixture)})
			if code != 1 {
				t.Fatalf("exit=%d want 1; stdout=%q stderr=%q", code, stdout, stderr)
			}
			if !strings.Contains(stdout, "INVALID") {
				t.Errorf("stdout should contain INVALID, got: %s", stdout)
			}
			if !strings.Contains(stdout, c.want) {
				t.Errorf("stdout should mention %q, got: %s", c.want, stdout)
			}
		})
	}
}

func TestValidateCmd_NoArgsExitsTwo(t *testing.T) {
	code, _, stderr := runCLI(nil)
	if code != 2 {
		t.Fatalf("exit=%d want 2", code)
	}
	if !strings.Contains(stderr, "Usage:") {
		t.Errorf("stderr should contain Usage:, got: %s", stderr)
	}
}

func TestValidateCmd_TooManyArgsExitsTwo(t *testing.T) {
	code, _, stderr := runCLI([]string{"a.ioflux", "b.ioflux"})
	if code != 2 {
		t.Fatalf("exit=%d want 2", code)
	}
	if !strings.Contains(stderr, "Usage:") {
		t.Errorf("stderr should contain Usage:, got: %s", stderr)
	}
}

func TestValidateCmd_MissingFileExitsTwo(t *testing.T) {
	code, _, stderr := runCLI([]string{"/nonexistent/path/trace.ioflux"})
	if code != 2 {
		t.Fatalf("exit=%d want 2", code)
	}
	if !strings.Contains(stderr, "ioflux validate") {
		t.Errorf("stderr should contain command name, got: %s", stderr)
	}
}

func TestValidateCmd_MalformedHeaderExitsOne(t *testing.T) {
	tmp, err := os.CreateTemp("", "ioflux-bad-*.ioflux")
	if err != nil {
		t.Fatalf("CreateTemp: %v", err)
	}
	defer os.Remove(tmp.Name())
	if _, err := tmp.WriteString("not json at all\n"); err != nil {
		t.Fatalf("write: %v", err)
	}
	tmp.Close()

	code, stdout, _ := runCLI([]string{tmp.Name()})
	if code != 1 {
		t.Fatalf("exit=%d want 1; stdout=%s", code, stdout)
	}
	if !strings.Contains(stdout, "parse error") {
		t.Errorf("stdout should mention parse error, got: %s", stdout)
	}
}

func TestValidateCmd_UnknownFlagExitsTwo(t *testing.T) {
	code, _, _ := runCLI([]string{"--bogus-flag"})
	if code != 2 {
		t.Fatalf("exit=%d want 2", code)
	}
}
