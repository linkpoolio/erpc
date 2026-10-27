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

// NewPayloadFromHttp extracts auth signals from an HTTP (or WebSocket
// upgrade) request. credentialPath is the request path when the server does
// not use it for routing (a domain alias preselects project, architecture and
// chain), and "" otherwise; see PathSegmentSecret.
func NewPayloadFromHttp(method string, remoteAddr string, headers http.Header, args url.Values, credentialPath string) (*AuthPayload, error) {
	ap := &AuthPayload{
		Method:     method,
		Headers:    headers,
		RemoteAddr: remoteAddr,
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
	} else if tkn := headers.Get("X-ERPC-Secret-Token"); tkn != "" {
		ap.Type = common.AuthTypeSecret
		ap.Secret = &SecretPayload{
			Value: tkn,
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
	}

	// Secondary secret locations, for clients and gateways that use the
	// common "API key" conventions. They fill ap.Secret only when no other
	// secret was found and never change ap.Type, so they do not alter which
	// strategies a request without one of the credentials above reaches:
	// a request that only carries an X-Api-Key header still reaches a
	// network strategy, and only secret/database strategies that list the
	// location in additionalLocations pick the key up.
	if ap.Secret == nil {
		if apikey := args.Get("apikey"); apikey != "" {
			ap.Secret = &SecretPayload{Value: apikey, Location: common.SecretLocationApikeyQuery}
		} else if apikey := firstNonEmptyHeader(headers, "X-Api-Key", "Apikey"); apikey != "" {
			ap.Secret = &SecretPayload{Value: apikey, Location: common.SecretLocationApikeyHeader}
		} else if pathSecret := PathSegmentSecret(credentialPath); pathSecret != "" {
			ap.Secret = &SecretPayload{Value: pathSecret, Location: common.SecretLocationPath}
		}
	}

	// Default to network strategy when no other auth signals are present.
	if ap.Type == "" {
		ap.Type = common.AuthTypeNetwork
	}

	return ap, nil
}

// reservedPathSegments are single-segment paths eRPC serves itself; they are
// never treated as a secret.
var reservedPathSegments = map[string]struct{}{
	"admin":       {},
	"healthcheck": {},
	"metrics":     {},
}

// PathSegmentSecret returns the secret carried as the only path segment of
// requestPath ("/<secret>" or "/<secret>/"), or "" when the path is empty,
// "/", has several segments, or is a reserved endpoint.
func PathSegmentSecret(requestPath string) string {
	if requestPath == "" {
		return ""
	}
	seg := strings.TrimPrefix(path.Clean("/"+requestPath), "/")
	if seg == "" || strings.Contains(seg, "/") {
		return ""
	}
	if _, reserved := reservedPathSegments[seg]; reserved {
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
