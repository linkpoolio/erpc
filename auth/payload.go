package auth

import (
	"net/http"

	"github.com/erpc/erpc/common"
)

type AuthPayload struct {
	Method string
	Type   common.AuthType
	Secret *SecretPayload
	Jwt    *JwtPayload
	Siwe   *SiwePayload

	// Headers and RemoteAddr carry the raw transport context for strategies
	// that need it rather than a credential extracted up front (e.g.
	// forwardedClientId reads its configured header and checks the direct
	// peer). They are only populated for HTTP and WebSocket requests.
	Headers    http.Header
	RemoteAddr string
}

// This payload is used by both "secret" and "database" strategies
type SecretPayload struct {
	Value string
	// Location is where an additional-location secret was found (see
	// common.SecretLocation); "" for the standard locations.
	Location common.SecretLocation
}

// secretAccepted reports whether ap carries a secret from a location a
// strategy accepts: a standard one, or one of its additional locations.
func secretAccepted(ap *AuthPayload, additional []common.SecretLocation) bool {
	if ap.Secret == nil {
		return false
	}
	if ap.Secret.Location == "" {
		return true
	}
	for _, l := range additional {
		if l == ap.Secret.Location {
			return true
		}
	}
	return false
}

type JwtPayload struct {
	Token string
}

type SiwePayload struct {
	Signature string
	Message   string
}
