package main

import (
	"bytes"
	"fmt"
	"os"
	"sort"

	"github.com/chanuollala/ioflux/pkg/results"
	"gopkg.in/yaml.v3"
)

// expConfig declares a paired experiment: one shared configuration, the two
// arms' overrides of it, and the rules a conclusion must satisfy.
//
// The shape is deliberate. Writing the shared settings once and the treatment's
// overrides separately makes the treatment variable the thing you can see at a
// glance, and lets the tool derive what actually differs rather than take the
// author's word for it.
type expConfig struct {
	// Claim is free text recording what the experiment is meant to answer. It is
	// copied into the output so the numbers arrive with the question attached.
	Claim string `yaml:"claim"`
	// Trials is the number of measured pairs; Warmup the number of unmeasured
	// rounds run first.
	Trials int `yaml:"trials"`
	Warmup int `yaml:"warmup"`
	// Seed controls the within-pair ordering draw. Recording it is what makes
	// the randomization reproducible rather than merely arbitrary.
	Seed   int64     `yaml:"seed"`
	Policy expPolicy `yaml:"policy"`

	Run       armConfig `yaml:"run"`
	Baseline  armConfig `yaml:"baseline"`
	Treatment armConfig `yaml:"treatment"`
}

type expPolicy struct {
	MinTrials    *int     `yaml:"min_trials"`
	MaxCVPercent *float64 `yaml:"max_cv_percent"`
	// MaxDurationRegressionPercent turns the reported difference into a pass/fail
	// decision. Absent leaves the gate off rather than applying a built-in
	// number: plan.md §11.3 is explicit that thresholds are calibrated per
	// fixture, so a default here would be a decision made on the team's behalf.
	MaxDurationRegressionPercent *float64 `yaml:"max_duration_regression_percent"`
}

// armConfig overrides individual replay settings. Every field is a pointer so
// "absent" stays distinguishable from "set to the zero value" — without that, a
// treatment declaring max_inflight: 0 would be silently indistinguishable from
// one declaring nothing.
//
// S3 credentials are deliberately absent. A config file sits next to its
// results and is meant to be shared; secrets must not travel with the evidence,
// so they come from the environment as the AWS SDK's default chain resolves
// them.
type armConfig struct {
	Trace            *string  `yaml:"trace"`
	Engine           *string  `yaml:"engine"`
	Mode             *string  `yaml:"mode"`
	MaxInflight      *int     `yaml:"max_inflight"`
	Speedup          *float64 `yaml:"speedup"`
	TargetMap        *string  `yaml:"target_map"`
	TargetRoot       *string  `yaml:"target_root"`
	AllowPassthrough *bool    `yaml:"allow_passthrough"`
	Prepare          *string  `yaml:"prepare"`
	PrepareScope     *string  `yaml:"prepare_scope"`
	SourceRoot       *string  `yaml:"source_root"`
	CacheMode        *string  `yaml:"cache_mode"`
	Fill             *string  `yaml:"fill"`
	FillSeed         *int64   `yaml:"fill_seed"`
	Hosts            *string  `yaml:"hosts"`
	Bucket           *string  `yaml:"bucket"`
	Endpoint         *string  `yaml:"endpoint"`
	Region           *string  `yaml:"region"`
	PathStyle        *bool    `yaml:"path_style"`
}

// applyTo returns s with this arm's declared fields overridden.
func (a armConfig) applyTo(s runSettings) runSettings {
	setString(&s.TracePath, a.Trace)
	setString(&s.EngineName, a.Engine)
	setString(&s.Mode, a.Mode)
	setInt(&s.MaxInflight, a.MaxInflight)
	setFloat(&s.Speedup, a.Speedup)
	setString(&s.TargetMapPath, a.TargetMap)
	setString(&s.TargetRoot, a.TargetRoot)
	setBool(&s.AllowPassthrough, a.AllowPassthrough)
	setString(&s.PrepareMode, a.Prepare)
	setString(&s.PrepareScope, a.PrepareScope)
	setString(&s.SourceRoot, a.SourceRoot)
	setString(&s.CacheMode, a.CacheMode)
	setString(&s.FillMode, a.Fill)
	setInt64(&s.FillSeed, a.FillSeed)
	setString(&s.Hosts, a.Hosts)
	setString(&s.S3.Bucket, a.Bucket)
	setString(&s.S3.Endpoint, a.Endpoint)
	setString(&s.S3.Region, a.Region)
	setBool(&s.S3.PathStyle, a.PathStyle)
	return s
}

func setString(dst *string, v *string) {
	if v != nil {
		*dst = *v
	}
}
func setInt(dst *int, v *int) {
	if v != nil {
		*dst = *v
	}
}
func setInt64(dst *int64, v *int64) {
	if v != nil {
		*dst = *v
	}
}
func setFloat(dst *float64, v *float64) {
	if v != nil {
		*dst = *v
	}
}
func setBool(dst *bool, v *bool) {
	if v != nil {
		*dst = *v
	}
}

// loadExpConfig reads and validates an experiment config.
func loadExpConfig(path string) (*expConfig, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}
	var c expConfig
	// Unknown fields are an error, not a shrug: a misspelled treatment key would
	// otherwise silently produce an experiment with no treatment at all, whose
	// result would look like "no regression detected".
	dec := yaml.NewDecoder(bytes.NewReader(data))
	dec.KnownFields(true)
	if err := dec.Decode(&c); err != nil {
		return nil, fmt.Errorf("parse %s: %w", path, err)
	}
	if c.Trials < 1 {
		return nil, fmt.Errorf("%s: trials must be >= 1, got %d", path, c.Trials)
	}
	if c.Warmup < 0 {
		return nil, fmt.Errorf("%s: warmup must be >= 0, got %d", path, c.Warmup)
	}
	return &c, nil
}

// policy resolves the declared policy against the built-in floor.
func (c *expConfig) policy() results.TrialPolicy {
	p := results.DefaultTrialPolicy()
	if c.Policy.MinTrials != nil {
		p.MinValidTrials = *c.Policy.MinTrials
	}
	if c.Policy.MaxCVPercent != nil {
		p.MaxCVPercent = *c.Policy.MaxCVPercent
	}
	if c.Policy.MaxDurationRegressionPercent != nil {
		p.MaxDurationRegressionPercent = *c.Policy.MaxDurationRegressionPercent
	}
	return p
}

// arms returns the two arms' fully resolved settings.
func (c *expConfig) arms() (baseline, treatment runSettings) {
	base := c.Run.applyTo(defaultRunSettings())
	return c.Baseline.applyTo(base), c.Treatment.applyTo(base)
}

// treatmentVariables names the settings that actually differ between the arms.
//
// It is derived from the resolved settings rather than from which keys the
// treatment block happened to list, so a treatment that "overrides" a value
// with the one it already had is correctly reported as changing nothing — an
// experiment whose treatment is empty measures nothing, and should say so.
func treatmentVariables(baseline, treatment runSettings) []string {
	a, b := settingsFields(baseline), settingsFields(treatment)
	var diff []string
	for k, av := range a {
		if b[k] != av {
			diff = append(diff, k)
		}
	}
	sort.Strings(diff)
	return diff
}

// settingsFields renders settings as comparable name/value pairs.
func settingsFields(s runSettings) map[string]string {
	return map[string]string{
		"trace":             s.TracePath,
		"engine":            s.EngineName,
		"mode":              s.Mode,
		"max_inflight":      fmt.Sprint(s.MaxInflight),
		"speedup":           fmt.Sprint(s.Speedup),
		"target_map":        s.TargetMapPath,
		"target_root":       s.TargetRoot,
		"allow_passthrough": fmt.Sprint(s.AllowPassthrough),
		"prepare":           s.PrepareMode,
		"prepare_scope":     s.PrepareScope,
		"source_root":       s.SourceRoot,
		"cache_mode":        s.CacheMode,
		"fill":              s.FillMode,
		"fill_seed":         fmt.Sprint(s.FillSeed),
		"hosts":             s.Hosts,
		"bucket":            s.S3.Bucket,
		"endpoint":          s.S3.Endpoint,
		"region":            s.S3.Region,
		"path_style":        fmt.Sprint(s.S3.PathStyle),
	}
}
