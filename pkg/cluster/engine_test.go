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
