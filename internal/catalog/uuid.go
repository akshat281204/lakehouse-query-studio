package catalog

import (
	"crypto/rand"
	"fmt"
	"regexp"
)

// newUUID returns a random version-4 UUID string.
func newUUID() (string, error) {
	var b [16]byte
	if _, err := rand.Read(b[:]); err != nil {
		return "", fmt.Errorf("generate uuid: %w", err)
	}
	b[6] = (b[6] & 0x0f) | 0x40 // version 4
	b[8] = (b[8] & 0x3f) | 0x80 // RFC 4122 variant
	return fmt.Sprintf("%x-%x-%x-%x-%x", b[0:4], b[4:6], b[6:8], b[8:10], b[10:16]), nil
}

// NewUUID returns a random version-4 UUID string for callers that must mint an
// id before the catalog row exists (e.g. an upload key).
func NewUUID() (string, error) { return newUUID() }

var uuidPattern = regexp.MustCompile(`^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$`)

// validUUID reports whether s is a canonical lowercase UUID, so malformed ids
// are reported as not found instead of reaching the database as a type error.
func validUUID(s string) bool { return uuidPattern.MatchString(s) }
