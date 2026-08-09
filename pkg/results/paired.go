package results

import "time"

// Arm names one side of a paired experiment.
const (
	ArmBaseline  = "baseline"
	ArmTreatment = "treatment"
)

// PairedExperiment is the output of running two configurations interleaved.
//
// Interleaving is what makes the pairing possible, and the pairing is the point:
// if the machine drifts — thermal throttling, a neighbouring tenant, a cache
// warming up — running one arm to completion and then the other assigns that
// drift entirely to the second arm, where it is indistinguishable from the
// treatment's effect. Alternating the arms spreads the drift across both, and
// differencing within a pair cancels whatever the two shared.
type PairedExperiment struct {
	SchemaVersion int    `json:"result_schema_version,omitempty"`
	GeneratedAt   string `json:"generated_at"`
	Tool          Tool   `json:"tool,omitempty"`
	Host          Host   `json:"host,omitempty"`
	// Claim records what the experiment was meant to answer, so the numbers
	// arrive with the question attached.
	Claim string `json:"claim,omitempty"`
	// Seed is the RNG seed that decided within-pair ordering. Recorded because a
	// randomization nobody can reproduce is indistinguishable from an arbitrary
	// one.
	Seed int64 `json:"seed"`
	// TreatmentVariables names the settings that differ between the arms. Empty
	// means the two arms were identical and the experiment measured nothing.
	TreatmentVariables []string `json:"treatment_variables"`
	// PairOrder[i] is the arm that ran first in pair i.
	PairOrder []string `json:"pair_order"`

	Baseline  *TrialSet `json:"baseline"`
	Treatment *TrialSet `json:"treatment"`

	Paired      PairedSummary `json:"paired"`
	Eligibility Eligibility   `json:"eligibility"`
	Policy      TrialPolicy   `json:"policy"`
	// Regression is the decision the policy's threshold produced. It is always
	// present; when no threshold was declared its verdict is not_assessed, so a
	// reader can tell "within budget" from "nobody set a budget".
	Regression RegressionGate `json:"regression"`
}

// PairedSummary describes the within-pair differences.
type PairedSummary struct {
	// Pairs is the number of pairs where both arms executed validly; only those
	// can be differenced.
	Pairs int `json:"pairs"`
	// Delta summarizes treatment − baseline per pair, in nanoseconds. Positive
	// means the treatment took longer.
	Delta MetricSummary `json:"delta_ns"`
	// DeltaPercent is the median difference relative to the baseline median.
	DeltaPercent float64 `json:"delta_percent"`
	// ExcludesZero reports whether the interval on the median difference lies
	// entirely on one side of zero. When it does, the difference is unlikely to
	// be noise. When it does not, these pairs did not establish a difference —
	// which is not the same as establishing there is none.
	ExcludesZero bool `json:"excludes_zero"`
}

// BuildPaired assembles a paired experiment from two arms measured in
// alternation. baseline.Trials[i] and treatment.Trials[i] are the two halves of
// pair i; pairOrder[i] records which of them ran first.
func BuildPaired(
	claim string,
	baseline, treatment *TrialSet,
	policy TrialPolicy,
	seed int64,
	pairOrder []string,
	treatmentVars []string,
) *PairedExperiment {
	pe := &PairedExperiment{
		SchemaVersion:      SchemaVersion,
		GeneratedAt:        time.Now().UTC().Format(time.RFC3339),
		Tool:               CurrentTool(),
		Host:               CurrentHost(),
		Claim:              claim,
		Seed:               seed,
		TreatmentVariables: treatmentVars,
		PairOrder:          pairOrder,
		Baseline:           baseline,
		Treatment:          treatment,
		Policy:             policy,
	}

	// The per-arm gate applies unchanged: enough trials, stable enough, none
	// failed, and the two arms comparable on trace, environment, and build.
	tc := CompareTrialSets(baseline, treatment, policy)
	pe.Eligibility = tc.Eligibility
	// A declared treatment is not drift. The gate reports every difference it
	// finds, which for an experiment includes the one deliberately introduced —
	// and warning about the change the experiment exists to measure is how
	// readers learn to skip warnings. The treatment is displayed on its own
	// line; what remains here is what nobody chose.
	pe.Eligibility.Caveats = dropDeclaredTreatments(pe.Eligibility.Caveats, treatmentVars)
	// The verdict was decided before that filtering, so recompute it: an
	// experiment whose only difference was the treatment it declared is
	// comparable outright, not caveated by its own design.
	pe.Eligibility.Verdict = verdictFor(pe.Eligibility)

	// An experiment whose arms are identical measures nothing. That is a
	// property of the design, not of the data, so it is reported even when every
	// trial succeeded.
	if len(treatmentVars) == 0 {
		pe.Eligibility.Caveats = append(pe.Eligibility.Caveats, Difference{
			Field: "treatment",
			A:     "(none)",
			B:     "(none)",
			Note: "the two arms were configured identically, so any difference below is " +
				"run-to-run variation and the experiment has no treatment to attribute it to",
		})
		pe.Eligibility.Verdict = verdictFor(pe.Eligibility)
	}

	pe.Paired = pairedSummary(baseline, treatment)
	// Decided last: the gate reads the eligibility verdict and the differenced
	// pairs, so both must already be final.
	pe.Regression = EvaluateRegression(pe, policy.MaxDurationRegressionPercent)
	return pe
}

// eligibilityFieldForSetting maps an experiment's setting name to the name the
// comparison gate reports that difference under. Settings absent from this map
// are ones the gate does not compare, so declaring them as a treatment removes
// nothing.
var eligibilityFieldForSetting = map[string]string{
	"trace":        "trace",
	"engine":       "engine",
	"cache_mode":   "cache mode",
	"mode":         "replay mode",
	"max_inflight": "max-inflight",
	"prepare":      "prepare",
	"fill":         "fill",
	"fill_seed":    "fill",
	"target_root":  "target root",
	"bucket":       "bucket",
	"endpoint":     "endpoint",
	"speedup":      "speedup",
}

// dropDeclaredTreatments removes the caveats that merely restate a declared
// treatment variable.
func dropDeclaredTreatments(caveats []Difference, treatmentVars []string) []Difference {
	if len(caveats) == 0 || len(treatmentVars) == 0 {
		return caveats
	}
	declared := make(map[string]bool, len(treatmentVars))
	for _, v := range treatmentVars {
		if field, ok := eligibilityFieldForSetting[v]; ok {
			declared[field] = true
		}
	}
	kept := caveats[:0:0]
	for _, c := range caveats {
		if !declared[c.Field] {
			kept = append(kept, c)
		}
	}
	return kept
}

// pairedSummary differences the arms pair by pair. A pair contributes only when
// both of its trials executed validly: a failed run's duration measures how
// long the failure took, and differencing against it would produce a number
// that looks like a performance result.
func pairedSummary(baseline, treatment *TrialSet) PairedSummary {
	n := len(baseline.Trials)
	if len(treatment.Trials) < n {
		n = len(treatment.Trials)
	}

	deltas := make([]float64, 0, n)
	for i := 0; i < n; i++ {
		a, b := baseline.Trials[i], treatment.Trials[i]
		if len(a.ExecutionInvalidReasons()) > 0 || len(b.ExecutionInvalidReasons()) > 0 {
			continue
		}
		deltas = append(deltas, float64(b.DurationNS-a.DurationNS))
	}

	s := PairedSummary{Pairs: len(deltas), Delta: summarize(deltas)}
	if base := baseline.Summary.DurationNS.Median; base != 0 && len(deltas) > 0 {
		s.DeltaPercent = s.Delta.Median / base * 100
	}
	if s.Delta.CI95Available {
		s.ExcludesZero = s.Delta.CI95Lo > 0 || s.Delta.CI95Hi < 0
	}
	return s
}
