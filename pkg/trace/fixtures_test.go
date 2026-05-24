package trace

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

// TestFixtures exercises every file in testdata/. A filename containing
// "_valid" is expected to parse and validate cleanly; any other filename
// is expected to fail either NewReader (malformed JSON / structural) or
// Validate (invariant violation).
func TestFixtures(t *testing.T) {
	entries, err := os.ReadDir("testdata")
	if err != nil {
		t.Fatalf("read testdata: %v", err)
	}
	if len(entries) == 0 {
		t.Fatal("no fixtures in testdata/")
	}
	for _, e := range entries {
		if e.IsDir() || !strings.HasSuffix(e.Name(), ".ioflux") {
			continue
		}
		name := e.Name()
		expectValid := strings.Contains(name, "_valid")
		t.Run(name, func(t *testing.T) {
			path := filepath.Join("testdata", name)
			f, err := os.Open(path)
			if err != nil {
				t.Fatalf("open %s: %v", path, err)
			}
			defer f.Close()

			r, readerErr := NewReader(f)
			if readerErr != nil {
				if expectValid {
					t.Fatalf("%s: NewReader unexpectedly failed: %v", name, readerErr)
				}
				return // invalid fixtures may legitimately fail at parse time
			}

			rep, err := Validate(r)
			if err != nil {
				t.Fatalf("%s: Validate I/O error: %v", name, err)
			}
			if expectValid {
				if !rep.OK() {
					t.Fatalf("%s: want valid, got errors=%v", name, rep.Errors)
				}
			} else {
				if rep.OK() {
					t.Fatalf("%s: want invalid, but Validate returned no errors", name)
				}
			}
		})
	}
}
