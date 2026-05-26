package main

import (
	"bytes"
	"io"
	"os"
	"strconv"
	"strings"
	"testing"
)

func runGenCLI(args []string) (int, string, string) {
	var stdout, stderr bytes.Buffer
	code := runGen(args, &stdout, &stderr)
	return code, stdout.String(), stderr.String()
}

func genToStdout(t *testing.T, args ...string) []byte {
	t.Helper()
	var out bytes.Buffer
	code := runGen(append([]string{"training-read"}, args...), &out, io.Discard)
	if code != 0 {
		t.Fatalf("runGen exit=%d", code)
	}
	return out.Bytes()
}

// TestGenCmd_NoArgsExitsTwo ensures `ioflux gen` with no profile prints usage.
func TestGenCmd_NoArgsExitsTwo(t *testing.T) {
	code, _, stderr := runGenCLI(nil)
	if code != 2 {
		t.Fatalf("exit=%d want 2", code)
	}
	if !strings.Contains(stderr, "Usage:") {
		t.Errorf("stderr should contain Usage:, got: %s", stderr)
	}
}

// TestGenCmd_UnknownProfileExitsTwo ensures an unrecognised profile is rejected.
func TestGenCmd_UnknownProfileExitsTwo(t *testing.T) {
	code, _, stderr := runGenCLI([]string{"checkpoint-write"})
	if code != 2 {
		t.Fatalf("exit=%d want 2", code)
	}
	if !strings.Contains(stderr, "unknown profile") {
		t.Errorf("stderr should mention unknown profile, got: %s", stderr)
	}
}

// TestGenCmd_MissingOutputFlag ensures -o is required.
func TestGenCmd_MissingOutputFlag(t *testing.T) {
	code, _, stderr := runGenCLI([]string{"training-read", "--shards", "4"})
	if code != 2 {
		t.Fatalf("exit=%d want 2", code)
	}
	if !strings.Contains(stderr, "-o is required") {
		t.Errorf("stderr should mention -o, got: %s", stderr)
	}
}

// TestGenCmd_BasicSmoke runs a minimal generation to a temp file and validates
// that the file exists and is non-empty.
func TestGenCmd_BasicSmoke(t *testing.T) {
	tmp, err := os.CreateTemp("", "ioflux-gen-smoke-*.ioflux")
	if err != nil {
		t.Fatal(err)
	}
	tmp.Close()
	defer os.Remove(tmp.Name())

	code, stdout, stderr := runGenCLI([]string{
		"training-read",
		"--shards", "4",
		"--shard-size", "131072",
		"--record-size", "16384",
		"--dataloader-workers", "2",
		"--shuffle=false",
		"--seed", "1",
		"-o", tmp.Name(),
	})
	if code != 0 {
		t.Fatalf("exit=%d want 0; stdout=%s stderr=%s", code, stdout, stderr)
	}
	if !strings.Contains(stdout, "wrote") {
		t.Errorf("stdout should confirm write, got: %s", stdout)
	}
	info, err := os.Stat(tmp.Name())
	if err != nil {
		t.Fatal(err)
	}
	if info.Size() == 0 {
		t.Error("output file is empty")
	}
}

// TestGenCmd_SizeSuffixes verifies that human-readable size suffixes produce
// the same output as the equivalent raw byte counts.
func TestGenCmd_SizeSuffixes(t *testing.T) {
	// These pairs must produce byte-identical traces: (suffix form, raw bytes).
	cases := []struct {
		name      string
		shardSize string
		recSize   string
		wantShard int64
		wantRec   int64
	}{
		{"MiB_KiB", "4MiB", "128KiB", 4 << 20, 128 << 10},
		{"lowercase_mib_kib", "4mib", "128kib", 4 << 20, 128 << 10},
		{"M_K", "4M", "128K", 4 << 20, 128 << 10},
		{"MB_KB", "4MB", "128KB", 4_000_000, 128_000},
	}
	baseArgs := []string{
		"--shards", "4",
		"--dataloader-workers", "1",
		"--shuffle=false",
		"--seed", "1",
		"-o", "-",
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			suffixArgs := append(baseArgs,
				"--shard-size", tc.shardSize,
				"--record-size", tc.recSize,
			)
			rawArgs := append(baseArgs,
				"--shard-size", strconv.FormatInt(tc.wantShard, 10),
				"--record-size", strconv.FormatInt(tc.wantRec, 10),
			)
			got := genToStdout(t, suffixArgs...)
			want := genToStdout(t, rawArgs...)
			if !bytes.Equal(got, want) {
				t.Errorf("suffix %q/%q produced different output than raw bytes %d/%d",
					tc.shardSize, tc.recSize, tc.wantShard, tc.wantRec)
			}
		})
	}
}

// TestGenCmd_Deterministic ensures the CLI produces byte-identical output
// for the same flags and seed (no injected non-determinism such as time.Now).
func TestGenCmd_Deterministic(t *testing.T) {
	args := []string{
		"--shards", "8",
		"--shard-size", "256KiB",
		"--record-size", "32KiB",
		"--dataloader-workers", "2",
		"--shuffle=false",
		"--seed", "99",
		"-o", "-",
	}
	out1 := genToStdout(t, args...)
	out2 := genToStdout(t, args...)
	if !bytes.Equal(out1, out2) {
		t.Fatal("CLI output is not deterministic for the same flags and seed")
	}
}

// TestGenCmd_InvalidParamsNoTruncate verifies that invalid params return
// exit code 1 and do not truncate an existing output file.
func TestGenCmd_InvalidParamsNoTruncate(t *testing.T) {
	tmp, err := os.CreateTemp("", "ioflux-no-trunc-*.ioflux")
	if err != nil {
		t.Fatal(err)
	}
	sentinel := []byte("precious existing trace content")
	if _, err := tmp.Write(sentinel); err != nil {
		t.Fatal(err)
	}
	tmp.Close()
	defer os.Remove(tmp.Name())

	// --shards 0 is invalid; the file must be untouched.
	code, _, stderr := runGenCLI([]string{
		"training-read",
		"--shards", "0",
		"--shard-size", "64MiB",
		"--record-size", "512KiB",
		"-o", tmp.Name(),
	})
	if code != 1 {
		t.Fatalf("exit=%d want 1 for invalid params; stderr=%s", code, stderr)
	}
	got, err := os.ReadFile(tmp.Name())
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(got, sentinel) {
		t.Errorf("existing file was modified on invalid params;\n got  %q\n want %q", got, sentinel)
	}
}

// TestGenCmd_InvalidSizeExitsTwo verifies that a malformed size string is
// caught during flag parsing (exit 2, distinct from invalid-params exit 1).
func TestGenCmd_InvalidSizeExitsTwo(t *testing.T) {
	code, _, stderr := runGenCLI([]string{
		"training-read",
		"--shard-size", "notasize",
		"--record-size", "512KiB",
		"-o", "-",
	})
	if code != 2 {
		t.Fatalf("exit=%d want 2 for malformed size; stderr=%s", code, stderr)
	}
}

// TestGenCmd_Stdout verifies that -o - writes JSON to stdout.
func TestGenCmd_Stdout(t *testing.T) {
	out := genToStdout(t,
		"--shards", "2",
		"--shard-size", "64KiB",
		"--record-size", "8KiB",
		"--dataloader-workers", "1",
		"--shuffle=false",
		"--seed", "1",
		"-o", "-",
	)
	if len(out) == 0 {
		t.Error("nothing written to stdout")
	}
	if out[0] != '{' {
		t.Errorf("stdout should start with JSON '{', got %q", out[:1])
	}
}

// TestParseBytes covers the full surface of parseBytes.
func TestParseBytes(t *testing.T) {
	cases := []struct {
		in      string
		want    int64
		wantErr bool
	}{
		// Plain integers.
		{"0", 0, false},
		{"1", 1, false},
		{"4194304", 4194304, false},
		// Binary IEC suffixes.
		{"1KiB", 1024, false},
		{"1MiB", 1 << 20, false},
		{"1GiB", 1 << 30, false},
		{"64MiB", 64 << 20, false},
		{"512KiB", 512 << 10, false},
		// Case-insensitive.
		{"4mib", 4 << 20, false},
		{"4MIB", 4 << 20, false},
		// Short binary aliases.
		{"4K", 4 << 10, false},
		{"4M", 4 << 20, false},
		{"4G", 4 << 30, false},
		// Decimal suffixes.
		{"1KB", 1_000, false},
		{"1MB", 1_000_000, false},
		{"1GB", 1_000_000_000, false},
		// Error cases.
		{"", 0, true},
		{"badsize", 0, true},
		{"-1", 0, true},
		{"1.5MiB", 0, true},
		{"MiB", 0, true},
		{"-4MiB", 0, true},
	}
	for _, tc := range cases {
		t.Run(tc.in, func(t *testing.T) {
			got, err := parseBytes(tc.in)
			if tc.wantErr {
				if err == nil {
					t.Errorf("parseBytes(%q) = %d, want error", tc.in, got)
				}
				return
			}
			if err != nil {
				t.Errorf("parseBytes(%q) error: %v", tc.in, err)
				return
			}
			if got != tc.want {
				t.Errorf("parseBytes(%q) = %d, want %d", tc.in, got, tc.want)
			}
		})
	}
}
