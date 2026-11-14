package obs

import (
	"runtime/debug"
	"strings"
)

// Set at build time, optionally (the Dockerfile owns the flags):
//
//	-ldflags "-X <module>/internal/obs.Version=1.4.2 -X <module>/internal/obs.Commit=abc1234 -X <module>/internal/obs.BuildTime=2026-11-13T10:00:00Z"
//
// Without them the VCS data Go embeds in the binary is used, then "dev"/"unknown".
var (
	Version   string
	Commit    string
	BuildTime string
)

// readBuildInfo is a variable so tests can fake the embedded build info.
var readBuildInfo = debug.ReadBuildInfo

// ResolveBuild returns the version and commit of this binary.
// version: SERVICE_VERSION, else the ldflags Version, else the embedded vcs.revision
// (12 characters, "-dirty" if the tree was modified), else "dev".
// commit: the ldflags Commit, else vcs.revision, else "unknown".
func ResolveBuild(get Getenv) (version, commit string) {
	var rev string
	var dirty bool
	if bi, ok := readBuildInfo(); ok {
		for _, kv := range bi.Settings {
			switch kv.Key {
			case "vcs.revision":
				rev = kv.Value
			case "vcs.modified":
				dirty = kv.Value == "true"
			}
		}
	}
	short := rev
	if len(short) > 12 {
		short = short[:12]
	}
	if short != "" && dirty {
		short += "-dirty"
	}

	commit = strings.TrimSpace(Commit)
	if commit == "" {
		commit = rev
	}
	if commit == "" {
		commit = "unknown"
	}

	version = strings.TrimSpace(get("SERVICE_VERSION"))
	if version == "" {
		version = strings.TrimSpace(Version)
	}
	if version == "" {
		version = short
	}
	if version == "" {
		version = "dev"
	}
	return version, commit
}
