package hsdi

import "regexp"

var (
	// A session ID is 24 hex digits whose last eight are a time (4-9). Any
	// run of 16 or more hex digits is treated as one.
	hexRunRE = regexp.MustCompile(`[0-9A-Fa-f]{16,}`)
	// ISO 8601 dates, with or without a time.
	dateRE = regexp.MustCompile(`\d{4}-\d{2}-\d{2}(T[0-9:.]+(Z|[+-]\d{2}:?\d{2})?)?`)
)

// Redact removes session IDs and dates from free text the gateway sends, such
// as the message attribute of <binEnd> or an errorString, so that it can be
// logged or reported. The gateway puts session IDs in that text, for example
// "BinEnd sent for sessionID ... to client ...".
func Redact(s string) string {
	s = hexRunRE.ReplaceAllString(s, "<session ID>")
	return dateRE.ReplaceAllString(s, "<date>")
}
