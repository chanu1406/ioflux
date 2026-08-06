package main

import (
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
)

func writeConfig(t *testing.T, body string) string {
	t.Helper()
	p := filepath.Join(t.TempDir(), "exp.yaml")
	if err := os.WriteFile(p, []byte(body), 0o644); err != nil {
		t.Fatal(err)
	}
	return p
}

func TestExpConfigResolvesArms(t *testing.T) {
	p := writeConfig(t, `
trials: 10
warmup: 2
run:
  trace: w.ioflux
  engine: local
  cache_mode: cold
  max_inflight: 8
baseline: {}
treatment:
  max_inflight: 1
`)
	c, err := loadExpConfig(p)
	if err != nil {
		t.Fatal(err)
	}
	base, treat := c.arms()

	if base.TracePath != "w.ioflux" || treat.TracePath != "w.ioflux" {
		t.Errorf("shared trace not applied to both arms: %q / %q", base.TracePath, treat.TracePath)
	}
	if base.EngineName != "local" || treat.EngineName != "local" {
		t.Errorf("shared engine not applied to both arms: %q / %q", base.EngineName, treat.EngineName)
	}
	if base.MaxInflight != 8 {
		t.Errorf("baseline max_inflight = %d, want 8 (from the shared block)", base.MaxInflight)
	}
	if treat.MaxInflight != 1 {
		t.Errorf("treatment max_inflight = %d, want 1 (its own override)", treat.MaxInflight)
	}
	// Anything neither block set keeps the same default a bare `ioflux run` uses.
	if base.Mode != "asap" {
		t.Errorf("mode = %q, want the run default asap", base.Mode)
	}
}

func TestExpConfigDerivesTreatmentVariables(t *testing.T) {
	p := writeConfig(t, `
trials: 6
run:
  trace: w.ioflux
  engine: local
  cache_mode: cold
treatment:
  engine: s3
  cache_mode: warm
`)
	c, err := loadExpConfig(p)
	if err != nil {
		t.Fatal(err)
	}
	base, treat := c.arms()

	got := treatmentVariables(base, treat)
	want := []string{"cache_mode", "engine"}
	if !reflect.DeepEqual(got, want) {
		t.Errorf("treatment variables = %v, want %v", got, want)
	}
}

// A treatment that restates a value it already had changes nothing, and an
// experiment with no treatment measures nothing. Deriving the variables from
// the resolved settings is what makes that visible.
func TestExpConfigTreatmentRestatingValueIsNoTreatment(t *testing.T) {
	p := writeConfig(t, `
trials: 6
run:
  trace: w.ioflux
  max_inflight: 8
treatment:
  max_inflight: 8
`)
	c, err := loadExpConfig(p)
	if err != nil {
		t.Fatal(err)
	}
	base, treat := c.arms()

	if got := treatmentVariables(base, treat); len(got) != 0 {
		t.Errorf("treatment variables = %v, want none", got)
	}
}

// A zero value in a treatment block must override, not read as "unset" — the
// reason every arm field is a pointer.
func TestExpConfigZeroValueOverrides(t *testing.T) {
	p := writeConfig(t, `
trials: 6
run:
  trace: w.ioflux
  fill_seed: 99
treatment:
  fill_seed: 0
`)
	c, err := loadExpConfig(p)
	if err != nil {
		t.Fatal(err)
	}
	base, treat := c.arms()

	if base.FillSeed != 99 {
		t.Errorf("baseline fill_seed = %d, want 99", base.FillSeed)
	}
	if treat.FillSeed != 0 {
		t.Errorf("treatment fill_seed = %d, want the declared 0", treat.FillSeed)
	}
	if got := treatmentVariables(base, treat); len(got) != 1 || got[0] != "fill_seed" {
		t.Errorf("treatment variables = %v, want [fill_seed]", got)
	}
}

// A misspelled key would otherwise produce an experiment with no treatment,
// whose result reads as "no regression detected".
func TestExpConfigRejectsUnknownFields(t *testing.T) {
	p := writeConfig(t, `
trials: 6
run:
  trace: w.ioflux
treatment:
  max_inflght: 1
`)
	_, err := loadExpConfig(p)
	if err == nil {
		t.Fatal("misspelled treatment key accepted")
	}
	if !strings.Contains(err.Error(), "max_inflght") {
		t.Errorf("error should name the unknown field; got %v", err)
	}
}

func TestExpConfigValidatesCounts(t *testing.T) {
	for _, body := range []string{
		"trials: 0\nrun:\n  trace: w.ioflux\n",
		"trials: -1\nrun:\n  trace: w.ioflux\n",
		"trials: 5\nwarmup: -1\nrun:\n  trace: w.ioflux\n",
	} {
		if _, err := loadExpConfig(writeConfig(t, body)); err == nil {
			t.Errorf("accepted invalid config:\n%s", body)
		}
	}
}

func TestExpConfigPolicyDefaultsAndOverrides(t *testing.T) {
	p := writeConfig(t, "trials: 6\nrun:\n  trace: w.ioflux\n")
	c, err := loadExpConfig(p)
	if err != nil {
		t.Fatal(err)
	}
	if got := c.policy(); got.MinValidTrials != 6 || got.MaxCVPercent != 5 {
		t.Errorf("default policy = %+v, want the built-in floor", got)
	}

	p = writeConfig(t, `
trials: 6
policy:
  min_trials: 12
  max_cv_percent: 2.5
run:
  trace: w.ioflux
`)
	c, err = loadExpConfig(p)
	if err != nil {
		t.Fatal(err)
	}
	if got := c.policy(); got.MinValidTrials != 12 || got.MaxCVPercent != 2.5 {
		t.Errorf("declared policy = %+v, want 12 / 2.5", got)
	}
}
