package auth

import (
	"errors"
	"strings"
)

// ParseEnabled accepts the same configuration values for authentication and
// production startup checks, including the empty default used in development.
func ParseEnabled(raw string) (bool, error) {
	switch strings.ToLower(strings.TrimSpace(raw)) {
	case "1", "true", "yes", "on":
		return true, nil
	case "", "0", "false", "no", "off":
		return false, nil
	default:
		return false, errors.New("expected a boolean setting")
	}
}
