// Package targetmap implements prefix-rewrite rules that translate trace target
// names from a captured namespace to the replay backend's namespace.
//
// Rules are evaluated in order: non-empty From rules sorted by descending
// length (longest match first), then any empty-From catch-all rules.
package targetmap

import (
	"fmt"
	"net/url"
	"os"
	"sort"
	"strings"

	"gopkg.in/yaml.v3"

	"github.com/chanuollala/ioflux/pkg/trace"
)

// Rule is a single prefix-rewrite entry.
type Rule struct {
	From string `yaml:"from"`
	To   string `yaml:"to"`
}

// Map holds a loaded set of rewrite rules.
type Map struct {
	Rules            []Rule
	AllowPassthrough bool
}

// EngineContext carries engine parameters used to validate s3:// URIs.
type EngineContext struct {
	EngineKind string // e.g. "s3", "local"
	Bucket     string // S3 bucket configured on the engine; empty = skip check
}

// Unmatched records a target that matched no rule.
type Unmatched struct {
	Target trace.TargetInfo
}

type yamlFile struct {
	Rules            []Rule `yaml:"target_rewrite"`
	AllowPassthrough bool   `yaml:"allow_passthrough"`
}

// Load parses a YAML target-map config from path.
func Load(path string) (*Map, error) {
	b, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("targetmap: read %q: %w", path, err)
	}
	var f yamlFile
	if err := yaml.Unmarshal(b, &f); err != nil {
		return nil, fmt.Errorf("targetmap: parse %q: %w", path, err)
	}
	return &Map{Rules: f.Rules, AllowPassthrough: f.AllowPassthrough}, nil
}

// Rewrite returns a new []TargetInfo with names and kinds rewritten according
// to m's rules. Rules are tried in order: non-empty From rules by descending
// length, then empty-From rules. Targets that match no rule land in unmatched.
// When AllowPassthrough is false, the first unmatched target causes an error.
func (m *Map) Rewrite(targets []trace.TargetInfo, ec EngineContext) ([]trace.TargetInfo, []Unmatched, error) {
	ordered := sortedRules(m.Rules)
	out := make([]trace.TargetInfo, len(targets))
	var unmatched []Unmatched
	for i, tgt := range targets {
		rewritten, matched, err := applyFirst(ordered, tgt, ec)
		if err != nil {
			return nil, nil, err
		}
		if !matched {
			unmatched = append(unmatched, Unmatched{Target: tgt})
			if !m.AllowPassthrough {
				return nil, unmatched, fmt.Errorf("targetmap: target %q matched no rule (use allow_passthrough: true to permit)", tgt.Name)
			}
			out[i] = tgt
		} else {
			out[i] = rewritten
		}
	}
	return out, unmatched, nil
}

// sortedRules orders non-empty From rules by descending length, then any
// empty-From catch-all rules at the end.
func sortedRules(rules []Rule) []Rule {
	var specific, catchAll []Rule
	for _, r := range rules {
		if r.From == "" {
			catchAll = append(catchAll, r)
		} else {
			specific = append(specific, r)
		}
	}
	sort.SliceStable(specific, func(i, j int) bool {
		return len(specific[i].From) > len(specific[j].From)
	})
	return append(specific, catchAll...)
}

// applyFirst tries each rule and returns the first match.
func applyFirst(rules []Rule, tgt trace.TargetInfo, ec EngineContext) (trace.TargetInfo, bool, error) {
	for _, r := range rules {
		if !strings.HasPrefix(tgt.Name, r.From) {
			continue
		}
		suffix := tgt.Name[len(r.From):]
		result, err := rewriteTo(r.To, suffix, tgt, ec)
		if err != nil {
			return trace.TargetInfo{}, false, err
		}
		return result, true, nil
	}
	return trace.TargetInfo{}, false, nil
}

// rewriteTo computes the new TargetInfo when a rule matched. Any URI scheme
// not in {"", "file"} flips Kind to object; "file://" is normalized away and
// the target stays a file. s3:// additionally validates the bucket against the
// engine context.
func rewriteTo(to, suffix string, tgt trace.TargetInfo, ec EngineContext) (trace.TargetInfo, error) {
	out := tgt
	scheme := uriScheme(to)
	switch scheme {
	case "":
		// Plain path rewrite — keep the existing Kind.
		out.Name = to + suffix
		return out, nil
	case "file":
		// file:// → strip the scheme; treat as a plain path; keep Kind.
		u, err := url.Parse(to)
		if err != nil {
			return trace.TargetInfo{}, fmt.Errorf("targetmap: invalid URI %q: %w", to, err)
		}
		out.Name = u.Path + suffix
		return out, nil
	case "s3":
		u, err := url.Parse(to)
		if err != nil {
			return trace.TargetInfo{}, fmt.Errorf("targetmap: invalid URI %q: %w", to, err)
		}
		bucket := u.Host
		if ec.Bucket != "" && bucket != ec.Bucket {
			return trace.TargetInfo{}, fmt.Errorf("targetmap: rule targets bucket %q but engine uses bucket %q", bucket, ec.Bucket)
		}
		keyPrefix := strings.TrimPrefix(u.Path, "/")
		out.Name = keyPrefix + suffix
		out.Kind = trace.TargetObject
		return out, nil
	default:
		// Any other URI scheme (gs://, az://, …) → object target. Strip scheme
		// + host, store path-as-key. Engine-specific validation lives in its
		// own engine package; we only flip the Kind.
		u, err := url.Parse(to)
		if err != nil {
			return trace.TargetInfo{}, fmt.Errorf("targetmap: invalid URI %q: %w", to, err)
		}
		keyPrefix := strings.TrimPrefix(u.Path, "/")
		out.Name = keyPrefix + suffix
		out.Kind = trace.TargetObject
		return out, nil
	}
}

// uriScheme returns the lowercased scheme of to if it looks like a URI
// (scheme://...), otherwise "".
func uriScheme(to string) string {
	idx := strings.Index(to, "://")
	if idx <= 0 {
		return ""
	}
	scheme := to[:idx]
	for _, c := range scheme {
		isAlnum := (c >= 'a' && c <= 'z') || (c >= 'A' && c <= 'Z') || (c >= '0' && c <= '9') || c == '+' || c == '-' || c == '.'
		if !isAlnum {
			return ""
		}
	}
	return strings.ToLower(scheme)
}
