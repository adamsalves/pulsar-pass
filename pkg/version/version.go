// Package version carries the build metadata embedded at release time
// via -ldflags -X. In development builds it reports "dev".
package version

var (
	// Version is the semantic version of the build (e.g. "1.2.3").
	Version = "dev"
	// Commit is the git commit the binary was built from.
	Commit = "none"
	// Date is the RFC 3339 build timestamp.
	Date = "unknown"
)
