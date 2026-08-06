// Package buildinfo identifies the IOFlux build that produced an artifact.
//
// This is the tool version, which is distinct from the coordinator/worker
// protocol version in pkg/cluster: the protocol version gates whether two
// processes may talk to each other, while this one records which build wrote a
// result so a later comparison can tell whether two results came from the same
// measuring instrument.
package buildinfo

import "runtime/debug"

// Version is the declared release version of the tool.
const Version = "0.4.0"

// Revision returns the VCS commit the binary was built from, suffixed with
// "-dirty" when the working tree had uncommitted changes at build time. It
// returns "" when the build carries no VCS stamp, which is what `go run` and
// builds from a source tarball produce.
//
// The dirty marker is worth keeping rather than trimming: a result produced by
// a modified working tree is not reproducible from any commit, and a comparison
// that treats it as if it were would attribute a code difference to storage.
func Revision() string {
	info, ok := debug.ReadBuildInfo()
	if !ok {
		return ""
	}
	var rev string
	var modified bool
	for _, s := range info.Settings {
		switch s.Key {
		case "vcs.revision":
			rev = s.Value
		case "vcs.modified":
			modified = s.Value == "true"
		}
	}
	if rev == "" {
		return ""
	}
	if modified {
		return rev + "-dirty"
	}
	return rev
}
