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

// TestBuildEngineRejectsRootForMemEngine verifies that a containment root the
// chosen engine cannot enforce stops the run instead of being silently dropped:
// an operator who asked for confinement must never get an unconfined run that
// looks successful. The mem engine has no persistent namespace to confine, so
// accepting a root would imply a guarantee that means nothing.
func TestBuildEngineRejectsRootForMemEngine(t *testing.T) {
	_, _, err := cluster.BuildEngine(cluster.EngineSpec{Name: "mem", Root: t.TempDir()})
	if err == nil {
		t.Fatal("BuildEngine(mem) with a target root succeeded; want rejection")
	}
	if !strings.Contains(err.Error(), "only supported by the local and s3 engines") {
		t.Fatalf("BuildEngine(mem) err=%v, want an explanation of which engines can enforce a root", err)
	}
}

// TestBuildEngineAcceptsRootForS3Engine verifies a root reaches the s3 engine as
// a key prefix and is reported as enforced rather than as an unconfined run.
func TestBuildEngineAcceptsRootForS3Engine(t *testing.T) {
	eng, bucket, err := cluster.BuildEngine(cluster.EngineSpec{
		Name: "s3",
		Root: "datasets/run-1",
		S3: s3engine.Config{
			Endpoint: "http://127.0.0.1:1", Bucket: "bench", PathStyle: true,
			AccessKey: "ak", SecretKey: "sk",
		},
	})
	if err != nil {
		t.Fatalf("BuildEngine(s3) with root: %v", err)
	}
	if bucket != "bench" {
		t.Fatalf("bucket=%q, want bench", bucket)
	}
	s3e, ok := eng.(*s3engine.S3Engine)
	if !ok {
		t.Fatalf("engine type=%T, want *s3engine.S3Engine", eng)
	}
	// The prefix is enforced: a key outside it is refused before any request.
	if err := s3e.CheckTarget("other/obj"); err == nil {
		t.Fatal("s3 engine accepted a key outside the configured prefix")
	}
	if err := s3e.CheckTarget("datasets/run-1/obj"); err != nil {
		t.Fatalf("s3 engine rejected a key inside the configured prefix: %v", err)
	}
	joined := strings.Join(s3e.Limitations(), "\n")
	if strings.Contains(joined, "no target root configured") {
		t.Fatalf("confined s3 engine reported itself unconfined: %q", joined)
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
