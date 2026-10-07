package auth

import (
	"encoding/base64"
	"errors"
	"net/http"
	"net/url"
	"path"
	"strings"

	"github.com/erpc/erpc/common"
)

func NewPayloadFromHttp(method string, remoteAddr string, headers http.Header, args url.Values, requestPath string) (*AuthPayload, error) {
	ap := &AuthPayload{
		Method: method,
	}

	if token := args.Get("token"); token != "" { // deprecated
		ap.Type = common.AuthTypeSecret
		ap.Secret = &SecretPayload{
			Value: token,
		}
	} else if secret := args.Get("secret"); secret != "" {
		ap.Type = common.AuthTypeSecret
		ap.Secret = &SecretPayload{
			Value: secret,
		}
	} else if apikey := args.Get("apikey"); apikey != "" {
		// Alias used by edge gateways / clients that speak "apikey" rather than "secret".
		ap.Type = common.AuthTypeSecret
		ap.Secret = &SecretPayload{
			Value: apikey,
		}
	} else if tkn := headers.Get("X-ERPC-Secret-Token"); tkn != "" {
		ap.Type = common.AuthTypeSecret
		ap.Secret = &SecretPayload{
			Value: tkn,
		}
	} else if apikey := firstNonEmptyHeader(headers, "apikey", "X-Api-Key"); apikey != "" {
		ap.Type = common.AuthTypeSecret
		ap.Secret = &SecretPayload{
			Value: apikey,
		}
	} else if ath := headers.Get("Authorization"); ath != "" {
		ath = strings.TrimSpace(ath)
		parts := strings.SplitN(ath, " ", 2)
		if len(parts) == 2 {
			authType := strings.ToLower(parts[0])
			authValue := parts[1]

			if authType == "basic" {
				basicAuth, err := base64.StdEncoding.DecodeString(authValue)
				if err != nil {
					return nil, err
				}
				creds := strings.SplitN(string(basicAuth), ":", 2)
				if len(creds) != 2 {
					return nil, errors.New("invalid basic auth: must be base64 of username:password")
				}
				ap.Type = common.AuthTypeSecret
				ap.Secret = &SecretPayload{
					// Password is considered the secret value; username is ignored.
					Value: creds[1],
				}
			} else if authType == "bearer" {
				ap.Type = common.AuthTypeJwt
				ap.Jwt = &JwtPayload{
					Token: authValue,
				}
			}
		}
	} else if jwt := args.Get("jwt"); jwt != "" {
		ap.Type = common.AuthTypeJwt
		ap.Jwt = &JwtPayload{
			Token: jwt,
		}
	} else if signature := args.Get("signature"); signature != "" && args.Get("message") != "" {
		ap.Type = common.AuthTypeSiwe
		ap.Siwe = &SiwePayload{
			Signature: signature,
			Message:   normalizeSiweMessage(args.Get("message")),
		}
	} else if msg := headers.Get("X-Siwe-Message"); msg != "" {
		if sig := headers.Get("X-Siwe-Signature"); sig != "" {
			ap.Type = common.AuthTypeSiwe
			ap.Siwe = &SiwePayload{
				Signature: sig,
				Message:   normalizeSiweMessage(msg),
			}
		}
	} else if pathSecret := singlePathSegmentSecret(requestPath); pathSecret != "" {
		// Path form: https://host/<SECRET> (with domain aliasing so the segment
		// is not consumed as project/network). Avoids edge Lua/WASM filters.
		ap.Type = common.AuthTypeSecret
		ap.Secret = &SecretPayload{
			Value: pathSecret,
		}
	} else if clientId := firstNonEmptyHeader(headers, "X-Client-Id", "x-client-id"); clientId != "" {
		// Gateway-injected identity after edge API-key auth (Envoy forwardClientIDHeader).
		ap.Type = common.AuthTypeForwardedClientId
		ap.ForwardedClientId = &ForwardedClientIdPayload{
			Value: clientId,
		}
	}

	// Default to network strategy when no other auth signals are present.
	if ap.Type == "" {
		ap.Type = common.AuthTypeNetwork
	}

	return ap, nil
}

// singlePathSegmentSecret returns the sole path segment when the URL is
// `/<secret>` (or `/<secret>/`). Multi-segment eRPC paths and reserved
// endpoints are ignored so routing/healthchecks stay unchanged.
func singlePathSegmentSecret(requestPath string) string {
	if requestPath == "" {
		return ""
	}
	ps := path.Clean(requestPath)
	if ps == "/" || ps == "." {
		return ""
	}
	seg := strings.TrimPrefix(ps, "/")
	if seg == "" || strings.Contains(seg, "/") {
		return ""
	}
	switch seg {
	case "admin", "healthcheck", "metrics":
		return ""
	}
	return seg
}

func firstNonEmptyHeader(headers http.Header, names ...string) string {
	for _, name := range names {
		if v := strings.TrimSpace(headers.Get(name)); v != "" {
			return v
		}
	}
	return ""
}

func normalizeSiweMessage(msg string) string {
	decoded, err := base64.StdEncoding.DecodeString(msg)
	if err != nil {
		return msg
	}
	return string(decoded)
}
