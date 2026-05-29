package targetmap_test

import (
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/chanuollala/ioflux/pkg/targetmap"
	"github.com/chanuollala/ioflux/pkg/trace"
)

var benchEC = targetmap.EngineContext{EngineKind: "s3", Bucket: "bench"}

func TestPrefixRewrite_Hits(t *testing.T) {
	m := &targetmap.Map{Rules: []targetmap.Rule{{From: "/mnt/train/", To: "/tmp/bench/"}}}
	tgts := []trace.TargetInfo{{ID: 0, Name: "/mnt/train/shard_0.tar", Kind: trace.TargetFile}}
	out, unmatched, err := m.Rewrite(tgts, targetmap.EngineContext{})
	if err != nil {
		t.Fatal(err)
	}
	if len(unmatched) != 0 {
		t.Fatalf("unmatched=%d, want 0", len(unmatched))
	}
	if out[0].Name != "/tmp/bench/shard_0.tar" {
		t.Errorf("Name=%q, want /tmp/bench/shard_0.tar", out[0].Name)
	}
	if out[0].Kind != trace.TargetFile {
		t.Errorf("Kind=%q, want file", out[0].Kind)
	}
}

func TestPrefixRewrite_MissNoPassthrough(t *testing.T) {
	m := &targetmap.Map{Rules: []targetmap.Rule{{From: "/other/", To: "/tmp/"}}}
	tgts := []trace.TargetInfo{{ID: 0, Name: "/mnt/train/shard_0.tar", Kind: trace.TargetFile}}
	_, _, err := m.Rewrite(tgts, targetmap.EngineContext{})
	if err == nil {
		t.Fatal("Rewrite should fail when target misses and passthrough not allowed")
	}
	if !strings.Contains(err.Error(), "matched no rule") {
		t.Errorf("error=%v, want 'matched no rule'", err)
	}
}

func TestPrefixRewrite_MissWithPassthrough(t *testing.T) {
	m := &targetmap.Map{Rules: []targetmap.Rule{{From: "/other/", To: "/tmp/"}}, AllowPassthrough: true}
	tgts := []trace.TargetInfo{{ID: 0, Name: "/mnt/train/shard_0.tar", Kind: trace.TargetFile}}
	out, unmatched, err := m.Rewrite(tgts, targetmap.EngineContext{})
	if err != nil {
		t.Fatal(err)
	}
	if len(unmatched) != 1 {
		t.Fatalf("unmatched=%d, want 1", len(unmatched))
	}
	if out[0].Name != "/mnt/train/shard_0.tar" {
		t.Errorf("passthrough target Name changed to %q", out[0].Name)
	}
}

func TestRewriteParsesS3URI(t *testing.T) {
	m := &targetmap.Map{Rules: []targetmap.Rule{{From: "/mnt/imagenet/", To: "s3://bench/imagenet/"}}}
	tgts := []trace.TargetInfo{{ID: 0, Name: "/mnt/imagenet/shard_0001.tar", Kind: trace.TargetFile}}
	out, _, err := m.Rewrite(tgts, benchEC)
	if err != nil {
		t.Fatal(err)
	}
	if out[0].Name != "imagenet/shard_0001.tar" {
		t.Errorf("Name=%q, want imagenet/shard_0001.tar", out[0].Name)
	}
	if out[0].Kind != trace.TargetObject {
		t.Errorf("Kind=%q, want object", out[0].Kind)
	}
}

func TestRewriteRejectsBucketMismatch(t *testing.T) {
	m := &targetmap.Map{Rules: []targetmap.Rule{{From: "", To: "s3://other/prefix/"}}}
	tgts := []trace.TargetInfo{{ID: 0, Name: "shard_0.tar", Kind: trace.TargetFile}}
	_, _, err := m.Rewrite(tgts, benchEC)
	if err == nil {
		t.Fatal("Rewrite should fail on bucket mismatch")
	}
	if !strings.Contains(err.Error(), "bucket") {
		t.Errorf("error=%v, want bucket mismatch message", err)
	}
}

func TestEmptyFromMatchesBareNames(t *testing.T) {
	m := &targetmap.Map{Rules: []targetmap.Rule{{From: "", To: "s3://bench/imagenet/"}}}
	tgts := []trace.TargetInfo{
		{ID: 0, Name: "shard_0000.tar", Kind: trace.TargetFile},
		{ID: 1, Name: "shard_0001.tar", Kind: trace.TargetFile},
	}
	out, _, err := m.Rewrite(tgts, benchEC)
	if err != nil {
		t.Fatal(err)
	}
	if out[0].Name != "imagenet/shard_0000.tar" {
		t.Errorf("out[0].Name=%q, want imagenet/shard_0000.tar", out[0].Name)
	}
	if out[1].Name != "imagenet/shard_0001.tar" {
		t.Errorf("out[1].Name=%q, want imagenet/shard_0001.tar", out[1].Name)
	}
	for _, o := range out {
		if o.Kind != trace.TargetObject {
			t.Errorf("Kind=%q, want object", o.Kind)
		}
	}
}

func TestRuleOrdering_SpecificBeatsCatchAll(t *testing.T) {
	m := &targetmap.Map{Rules: []targetmap.Rule{
		{From: "shard_0001", To: "s3://bench/hot/"},
		{From: "", To: "s3://bench/imagenet/"},
	}}
	tgts := []trace.TargetInfo{
		{ID: 0, Name: "shard_0001.tar", Kind: trace.TargetFile},
		{ID: 1, Name: "shard_0002.tar", Kind: trace.TargetFile},
	}
	out, _, err := m.Rewrite(tgts, benchEC)
	if err != nil {
		t.Fatal(err)
	}
	if !strings.HasPrefix(out[0].Name, "hot/") {
		t.Errorf("shard_0001.tar → Name=%q, want hot/... prefix", out[0].Name)
	}
	if !strings.HasPrefix(out[1].Name, "imagenet/") {
		t.Errorf("shard_0002.tar → Name=%q, want imagenet/... prefix", out[1].Name)
	}
}

func TestRewriteUnknownSchemeFlipsToObject(t *testing.T) {
	// Per plan: any URI scheme not in {file://, ""} → object target.
	m := &targetmap.Map{Rules: []targetmap.Rule{{From: "", To: "gs://bench/imagenet/"}}}
	tgts := []trace.TargetInfo{{ID: 0, Name: "shard_0.tar", Kind: trace.TargetFile}}
	out, _, err := m.Rewrite(tgts, targetmap.EngineContext{})
	if err != nil {
		t.Fatal(err)
	}
	if out[0].Kind != trace.TargetObject {
		t.Errorf("Kind=%q, want object for gs:// rewrite", out[0].Kind)
	}
	if out[0].Name != "imagenet/shard_0.tar" {
		t.Errorf("Name=%q, want imagenet/shard_0.tar", out[0].Name)
	}
}

func TestRewriteFileSchemeStaysFile(t *testing.T) {
	// file:// → strip scheme, keep file Kind.
	m := &targetmap.Map{Rules: []targetmap.Rule{{From: "/mnt/train/", To: "file:///tmp/bench/"}}}
	tgts := []trace.TargetInfo{{ID: 0, Name: "/mnt/train/shard_0.tar", Kind: trace.TargetFile}}
	out, _, err := m.Rewrite(tgts, targetmap.EngineContext{})
	if err != nil {
		t.Fatal(err)
	}
	if out[0].Kind != trace.TargetFile {
		t.Errorf("Kind=%q, want file (file:// is normalized)", out[0].Kind)
	}
	if out[0].Name != "/tmp/bench/shard_0.tar" {
		t.Errorf("Name=%q, want /tmp/bench/shard_0.tar", out[0].Name)
	}
}

func TestLoad_ParsesYAML(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "map.yaml")
	content := `
target_rewrite:
  - from: "/mnt/train/"
    to: "s3://bench/train/"
allow_passthrough: true
`
	if err := os.WriteFile(path, []byte(content), 0o644); err != nil {
		t.Fatal(err)
	}
	m, err := targetmap.Load(path)
	if err != nil {
		t.Fatal(err)
	}
	if len(m.Rules) != 1 {
		t.Fatalf("Rules=%d, want 1", len(m.Rules))
	}
	if m.Rules[0].From != "/mnt/train/" {
		t.Errorf("From=%q, want /mnt/train/", m.Rules[0].From)
	}
	if m.Rules[0].To != "s3://bench/train/" {
		t.Errorf("To=%q, want s3://bench/train/", m.Rules[0].To)
	}
	if !m.AllowPassthrough {
		t.Error("AllowPassthrough should be true")
	}
}

func TestLoad_FileNotFound(t *testing.T) {
	_, err := targetmap.Load("/nonexistent/map.yaml")
	if err == nil {
		t.Fatal("Load should fail on missing file")
	}
}

func TestRewrite_EmptyMap_NoPassthrough(t *testing.T) {
	m := &targetmap.Map{}
	tgts := []trace.TargetInfo{{ID: 0, Name: "shard_0.tar"}}
	_, _, err := m.Rewrite(tgts, targetmap.EngineContext{})
	if err == nil {
		t.Fatal("empty map with no passthrough should error on any target")
	}
}

func TestRewrite_EmptyMap_WithPassthrough(t *testing.T) {
	m := &targetmap.Map{AllowPassthrough: true}
	tgts := []trace.TargetInfo{{ID: 0, Name: "shard_0.tar", Kind: trace.TargetFile}}
	out, unmatched, err := m.Rewrite(tgts, targetmap.EngineContext{})
	if err != nil {
		t.Fatal(err)
	}
	if len(unmatched) != 1 {
		t.Fatalf("unmatched=%d, want 1", len(unmatched))
	}
	if out[0].Name != "shard_0.tar" {
		t.Errorf("Name changed unexpectedly: %q", out[0].Name)
	}
}
