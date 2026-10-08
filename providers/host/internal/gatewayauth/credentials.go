package gatewayauth

import (
	"errors"
	"strings"
	"unicode"
)

const MetadataKey = "authorization"

// ValidateToken enforces the shared minimum format for Agent Gateway bearer
// credentials without imposing a particular encoding.
func ValidateToken(token string) error {
	if len(token) < 32 || strings.TrimSpace(token) != token {
		return errors.New("agent bearer token must contain at least 32 non-whitespace characters")
	}
	for _, character := range token {
		if unicode.IsSpace(character) || unicode.IsControl(character) {
			return errors.New("agent bearer token must contain at least 32 non-whitespace characters")
		}
	}
	return nil
}

// BearerValue formats a validated token for gRPC authorization metadata.
func BearerValue(token string) string {
	return "Bearer " + token
}

// ParseBearerValue extracts and validates a bearer token from authorization
// metadata.
func ParseBearerValue(value string) (string, bool) {
	parts := strings.Fields(value)
	if len(parts) != 2 || !strings.EqualFold(parts[0], "Bearer") {
		return "", false
	}
	if err := ValidateToken(parts[1]); err != nil {
		return "", false
	}
	return parts[1], true
}
