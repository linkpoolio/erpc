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
}

type JwtPayload struct {
	Token string
}

type SiwePayload struct {
	Signature string
	Message   string
}
