// Package profiles contains the built-in BACnet profile distributed with the
// provider.
package profiles

import _ "embed"

// Standard returns the provider's built-in profile. It is embedded so the
// provider does not depend on a runtime filesystem layout.
//
//go:embed standard.yaml
var standard []byte

func Standard() []byte {
	return append([]byte(nil), standard...)
}
