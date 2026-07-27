package cluster_test

import (
	"strings"
	"testing"

	"github.com/chanuollala/ioflux/pkg/cluster"
	"github.com/chanuollala/ioflux/pkg/engine/localfile"
	"github.com/chanuollala/ioflux/pkg/engine/mem"
	s3engine "github.com/chanuollala/ioflux/pkg/engine/s3"
)

func TestBuildEngineEachName(t *testing.T) {
	tests := []struct {
		name string
		spec cluster.EngineSpec
		want any
	}{
		{
			name: "mem",
			spec: cluster.EngineSpec{Name: "mem", TargetSizes: map[string]int64{"shard": 4096}},
			want: (*mem.MemEngine)(nil),
		},
		{
			name: "local",
			spec: cluster.EngineSpec{Name: "local", AllowDirect: true, DirectFallback: true, DirectAlign: 4096},
			want: (*localfile.LocalFileEngine)(nil),
		},
		{
			name: "s3",
			spec: cluster.EngineSpec{
				Name:      "s3",
				CacheMode: "cold",
				S3: s3engine.Config{
					Endpoint:  "http://127.0.0.1:1",
					Bucket:    "bench",
					PathStyle: true,
					AccessKey: "test-access",
					SecretKey: "test-secret",
				},
			},
			want: (*s3engine.S3Engine)(nil),
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			eng, bucket, err := cluster.BuildEngine(tt.spec)
			if err != nil {
				t.Fatalf("BuildEngine(%s): %v", tt.name, err)
			}
			switch tt.want.(type) {
			case *mem.MemEngine:
				if _, ok := eng.(*mem.MemEngine); !ok {
					t.Fatalf("engine type=%T, want *mem.MemEngine", eng)
				}
			case *localfile.LocalFileEngine:
				if _, ok := eng.(*localfile.LocalFileEngine); !ok {
					t.Fatalf("engine type=%T, want *localfile.LocalFileEngine", eng)
				}
			case *s3engine.S3Engine:
				if _, ok := eng.(*s3engine.S3Engine); !ok {
					t.Fatalf("engine type=%T, want *s3.S3Engine", eng)
				}
				if bucket != "bench" {
					t.Fatalf("bucket=%q, want bench", bucket)
				}
			}
		})
	}
}

func TestBuildEngineUnknownName(t *testing.T) {
	_, _, err := cluster.BuildEngine(cluster.EngineSpec{Name: "nope"})
	if err == nil {
		t.Fatal("BuildEngine unknown name succeeded")
	}
	if !strings.Contains(err.Error(), `unsupported engine "nope"`) {
		t.Fatalf("error=%q, want unsupported engine", err)
	}
}

// TestBuildEngineRejectsRootForNonLocalEngine verifies that a containment root
// the chosen engine cannot enforce stops the run instead of being silently
// dropped: an operator who asked for confinement must never get an unconfined
// run that looks successful.
func TestBuildEngineRejectsRootForNonLocalEngine(t *testing.T) {
	for _, name := range []string{"mem", "s3"} {
		t.Run(name, func(t *testing.T) {
			_, _, err := cluster.BuildEngine(cluster.EngineSpec{Name: name, Root: t.TempDir()})
			if err == nil {
				t.Fatalf("BuildEngine(%s) with a target root succeeded; want rejection", name)
			}
			if !strings.Contains(err.Error(), "only supported by the local engine") {
				t.Fatalf("BuildEngine(%s) err=%v, want an explanation naming the local engine", name, err)
			}
		})
	}
}

// TestBuildEngineAcceptsRootForLocalEngine verifies the local engine builds with
// a root and reports it as enforced rather than as an unconfined run.
func TestBuildEngineAcceptsRootForLocalEngine(t *testing.T) {
	root := t.TempDir()
	eng, _, err := cluster.BuildEngine(cluster.EngineSpec{Name: "local", Root: root})
	if err != nil {
		t.Fatalf("BuildEngine(local) with root: %v", err)
	}
	lfe, ok := eng.(*localfile.LocalFileEngine)
	if !ok {
		t.Fatalf("engine type=%T, want *localfile.LocalFileEngine", eng)
	}
	joined := strings.Join(lfe.Limitations(), "\n")
	if strings.Contains(joined, "no target root configured") {
		t.Fatalf("confined engine reported itself unconfined: %q", joined)
	}
	if !strings.Contains(joined, root) {
		t.Fatalf("limitations %q should name the enforced root %q", joined, root)
	}
}
