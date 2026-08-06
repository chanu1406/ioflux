package results_test

import (
	"reflect"
	"strings"
	"testing"

	"github.com/chanuollala/ioflux/pkg/fidelity"
	"github.com/chanuollala/ioflux/pkg/results"
)

const (
	digestA = "sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
	digestB = "sha256:bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"
)

// comparableResult builds a current-schema result that agrees with every other
// comparableResult on all gated fields, so a test only has to state what it is
// changing.
func comparableResult() *results.Results {
	return &results.Results{
		SchemaVersion: results.SchemaVersion,
		Tool:          results.Tool{Version: "0.4.0", Revision: "abc123"},
		Host:          results.Host{Hostname: "bench-01", OS: "linux", Arch: "amd64", CPUs: 28},
		Plan: results.PlanInfo{
			TracePath:         "/data/trace.ioflux",
			TraceDigest:       digestA,
			Engine:            "local",
			Mode:              "asap",
			MaxInflight:       512,
			PrepareMode:       "assume-existing",
			FillMode:          "seeded",
			FillSeed:          1,
			TargetRoot:        "/srv/scratch",
			ReplayEquivalence: results.EquivalenceSyscallLevel,
		},
		RunEnv:       results.RunEnv{CacheMode: "cold"},
		DurationNS:   1_000_000_000,
		OpsCompleted: 1024,
	}
}

// fieldsOf returns the caveat field names, for assertions that care which
// difference was reported rather than how it was worded.
func fieldsOf(e results.Eligibility) []string {
	out := make([]string, 0, len(e.Caveats))
	for _, c := range e.Caveats {
		out = append(out, c.Field)
	}
	return out
}

func hasField(e results.Eligibility, field string) bool {
	for _, c := range e.Caveats {
		if c.Field == field {
			return true
		}
	}
	return false
}

func TestEligibilityComparableWhenEverythingAgrees(t *testing.T) {
	a, b := comparableResult(), comparableResult()
	// Identity is the digest, not the path: the same bytes under another name
	// are the same workload.
	b.Plan.TracePath = "/elsewhere/copy.ioflux"

	e := results.CheckEligibility(a, b)

	if e.Verdict != results.VerdictComparable {
		t.Errorf("verdict = %q, want %q; caveats=%v blocking=%v",
			e.Verdict, results.VerdictComparable, fieldsOf(e), e.Blocking)
	}
	if !e.Comparable() {
		t.Error("Comparable() = false, want true")
	}
}

// A failed run cannot be either side of a comparison. This is the case the
// two-report path used to present as a clean delta while the single-report path
// called the same file INVALID.
func TestEligibilityRefusesRunWithOperationFailures(t *testing.T) {
	for _, tc := range []struct {
		name   string
		mutate func(*results.Results)
		want   string
	}{
		{"errors", func(r *results.Results) { r.Errors = 417 }, "417 operation failure(s)"},
		{"histogram overflow", func(r *results.Results) { r.HistogramOverflows = 3 },
			"3 latency sample(s) exceeded the histogram's trackable range"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			a := comparableResult()
			b := comparableResult()
			tc.mutate(b)

			e := results.CheckEligibility(a, b)

			if e.Verdict != results.VerdictIncomparable {
				t.Fatalf("verdict = %q, want %q", e.Verdict, results.VerdictIncomparable)
			}
			if e.Comparable() {
				t.Error("Comparable() = true, want false")
			}
			joined := strings.Join(e.Blocking, "\n")
			if !strings.Contains(joined, tc.want) {
				t.Errorf("blocking reasons %q missing %q", joined, tc.want)
			}
			if !strings.HasPrefix(joined, "B: ") {
				t.Errorf("blocking reason should name the side it came from; got %q", joined)
			}
		})
	}
}

// Invalidity on the A side must block just as it does on B — the gate is not
// allowed to be order-dependent.
func TestEligibilityRefusalIsSymmetric(t *testing.T) {
	bad := comparableResult()
	bad.Errors = 1
	good := comparableResult()

	first := results.CheckEligibility(bad, good)
	second := results.CheckEligibility(good, bad)

	if first.Verdict != results.VerdictIncomparable || second.Verdict != results.VerdictIncomparable {
		t.Fatalf("verdicts = %q / %q, want both %q", first.Verdict, second.Verdict, results.VerdictIncomparable)
	}
	if !strings.HasPrefix(first.Blocking[0], "A: ") {
		t.Errorf("first blocking reason = %q, want it attributed to A", first.Blocking[0])
	}
	if !strings.HasPrefix(second.Blocking[0], "B: ") {
		t.Errorf("second blocking reason = %q, want it attributed to B", second.Blocking[0])
	}
}

// A short read is already counted as an operation failure, so it reaches the
// gate through Errors; the reason text should still name it, because "417
// failures" and "417 failures, 12 of which disagreed about file length" send a
// reader to different places.
func TestEligibilityNamesShortReadsWithinFailures(t *testing.T) {
	a := comparableResult()
	b := comparableResult()
	b.Errors = 12
	b.ShortReads = 12

	e := results.CheckEligibility(a, b)

	if !strings.Contains(strings.Join(e.Blocking, "\n"), "including 12 read(s)") {
		t.Errorf("blocking reasons should name the short reads; got %v", e.Blocking)
	}
}

// A different trace is a caveat rather than a refusal: comparing a
// checkpoint-write run against a training-read one is a supported use. What
// must not happen is presenting that delta as a backend measurement.
func TestEligibilityCaveatsDifferentTrace(t *testing.T) {
	a := comparableResult()
	b := comparableResult()
	b.Plan.TraceDigest = digestB

	e := results.CheckEligibility(a, b)

	if e.Verdict != results.VerdictCaveated {
		t.Fatalf("verdict = %q, want %q", e.Verdict, results.VerdictCaveated)
	}
	if !hasField(e, "trace") {
		t.Fatalf("expected a trace caveat; got %v", fieldsOf(e))
	}
	for _, c := range e.Caveats {
		if c.Field == "trace" && !strings.Contains(c.Note, "not between two backends") {
			t.Errorf("trace caveat should say the delta is not a backend measurement; got %q", c.Note)
		}
	}
}

// A missing digest must not read as agreement. This is the legacy-artifact rule:
// absent evidence is reported as absent, never as a pass.
func TestEligibilityCaveatsMissingTraceIdentity(t *testing.T) {
	a := comparableResult()
	b := comparableResult()
	b.Plan.TraceDigest = ""

	e := results.CheckEligibility(a, b)

	if e.Verdict != results.VerdictCaveated {
		t.Fatalf("verdict = %q, want %q", e.Verdict, results.VerdictCaveated)
	}
	if !hasField(e, "trace") {
		t.Errorf("expected a trace caveat when identity is unavailable; got %v", fieldsOf(e))
	}
}

// A pre-schema result omits fields that cannot be told apart from empty ones,
// so the gate says so once instead of emitting a difference per field.
func TestEligibilityCaveatsLegacyResultOnce(t *testing.T) {
	a := comparableResult()
	b := comparableResult()
	b.SchemaVersion = 0
	b.Plan.TraceDigest = ""
	b.Plan.TargetRoot = ""
	b.Host = results.Host{}
	b.Tool = results.Tool{}

	e := results.CheckEligibility(a, b)

	if hasField(e, "target root") || hasField(e, "host") || hasField(e, "build") {
		t.Errorf("fields absent from a legacy result must not be reported as differing; got %v", fieldsOf(e))
	}
	if !hasField(e, "result schema") {
		t.Errorf("expected a result-schema caveat; got %v", fieldsOf(e))
	}
}

func TestEligibilityCaveatsEnvironmentDifferences(t *testing.T) {
	for _, tc := range []struct {
		name   string
		field  string
		mutate func(*results.Results)
	}{
		{"engine", "engine", func(r *results.Results) { r.Plan.Engine = "s3" }},
		{"cache mode", "cache mode", func(r *results.Results) { r.RunEnv.CacheMode = "warm" }},
		{"replay mode", "replay mode", func(r *results.Results) { r.Plan.Mode = "timeline" }},
		{"max-inflight", "max-inflight", func(r *results.Results) { r.Plan.MaxInflight = 4 }},
		{"prepare", "prepare", func(r *results.Results) { r.Plan.PrepareMode = "materialize-synthetic" }},
		{"fill", "fill", func(r *results.Results) { r.Plan.FillSeed = 99 }},
		{"target root", "target root", func(r *results.Results) { r.Plan.TargetRoot = "" }},
		{"host", "host", func(r *results.Results) { r.Host.Hostname = "bench-02" }},
		{"build", "build", func(r *results.Results) { r.Tool.Revision = "def456" }},
		{"bucket", "bucket", func(r *results.Results) { r.Plan.Bucket = "other" }},
		{"endpoint", "endpoint", func(r *results.Results) { r.Plan.Endpoint = "http://minio:9000" }},
		{"equivalence", "replay equivalence", func(r *results.Results) {
			r.Plan.ReplayEquivalence = results.EquivalenceObjectLevel
		}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			a := comparableResult()
			b := comparableResult()
			tc.mutate(b)

			e := results.CheckEligibility(a, b)

			if e.Verdict != results.VerdictCaveated {
				t.Fatalf("verdict = %q, want %q", e.Verdict, results.VerdictCaveated)
			}
			if !hasField(e, tc.field) {
				t.Errorf("expected a %q caveat; got %v", tc.field, fieldsOf(e))
			}
			for _, c := range e.Caveats {
				if c.Field == tc.field && c.Note == "" {
					t.Errorf("caveat %q carries no note explaining what it does to the comparison", tc.field)
				}
			}
		})
	}
}

// Speedup only shapes the schedule in scaled mode. Reporting a difference in a
// field the run ignored would be noise that trains readers to skip caveats.
func TestEligibilitySpeedupOnlyMattersInScaledMode(t *testing.T) {
	a := comparableResult()
	b := comparableResult()
	b.Plan.SpeedupFactor = 4

	if e := results.CheckEligibility(a, b); hasField(e, "speedup") {
		t.Errorf("speedup must not be compared outside scaled mode; got %v", fieldsOf(e))
	}

	a.Plan.Mode, b.Plan.Mode = "scaled", "scaled"
	if e := results.CheckEligibility(a, b); !hasField(e, "speedup") {
		t.Errorf("speedup must be compared in scaled mode; got %v", fieldsOf(e))
	}
}

func TestEligibilityCaveatsFidelityMismatch(t *testing.T) {
	a := comparableResult()
	b := comparableResult()
	b.Fidelity = fidelity.FidelityReport{LowFidelity: true, LowFidelityCategory: "behind_schedule"}

	e := results.CheckEligibility(a, b)

	if !hasField(e, "fidelity") {
		t.Fatalf("expected a fidelity caveat; got %v", fieldsOf(e))
	}
	for _, c := range e.Caveats {
		if c.Field == "fidelity" && !strings.Contains(c.B, "behind_schedule") {
			t.Errorf("fidelity caveat should carry the category; got B=%q", c.B)
		}
	}
}

// Invalidity outranks every difference: a run that failed is refused, not
// merely caveated, however much else also differs.
func TestEligibilityBlockingOutranksCaveats(t *testing.T) {
	a := comparableResult()
	b := comparableResult()
	b.Plan.TraceDigest = digestB
	b.Plan.Engine = "s3"
	b.Errors = 1

	if e := results.CheckEligibility(a, b); e.Verdict != results.VerdictIncomparable {
		t.Errorf("verdict = %q, want %q", e.Verdict, results.VerdictIncomparable)
	}
}

func TestExecutionInvalidReasonsEmptyForCleanRun(t *testing.T) {
	if reasons := comparableResult().ExecutionInvalidReasons(); len(reasons) != 0 {
		t.Errorf("clean run reported invalid: %v", reasons)
	}
}

// CheckEligibility must not mutate either input; the report command prints from
// the same structs afterwards.
func TestCheckEligibilityDoesNotMutateInputs(t *testing.T) {
	a, b := comparableResult(), comparableResult()
	b.Plan.Engine = "s3"
	beforeA, beforeB := *a, *b

	results.CheckEligibility(a, b)

	if !reflect.DeepEqual(*a, beforeA) || !reflect.DeepEqual(*b, beforeB) {
		t.Error("CheckEligibility mutated one of its inputs")
	}
}
