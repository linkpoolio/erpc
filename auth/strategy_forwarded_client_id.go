package auth

import (
	"context"
	"fmt"
	"net"
	"net/http"
	"strings"

	"github.com/erpc/erpc/common"
)

// maxForwardedClientIdLength caps the accepted id length. The id becomes the
// request's user id, which is used in logs, metric labels and rate-limit keys.
const maxForwardedClientIdLength = 256

// ForwardedClientIdStrategy authenticates a caller by a client id that a
// trusted reverse proxy / API gateway placed in a request header after
// authenticating the caller itself. eRPC performs no credential check of its
// own, so this strategy must only be enabled behind a proxy that strips or
// overwrites the header on incoming requests (see
// common.ForwardedClientIdStrategyConfig).
type ForwardedClientIdStrategy struct {
	cfg            *common.ForwardedClientIdStrategyConfig
	header         string
	allowedIds     map[string]struct{}
	allowedIPs     []net.IP
	allowedCIDRs   []*net.IPNet
	restrictToPeer bool
}

var _ AuthStrategy = &ForwardedClientIdStrategy{}

func NewForwardedClientIdStrategy(cfg *common.ForwardedClientIdStrategyConfig) (*ForwardedClientIdStrategy, error) {
	s := &ForwardedClientIdStrategy{
		cfg:    cfg,
		header: http.CanonicalHeaderKey(cfg.Header),
	}
	if s.header == "" {
		s.header = common.DefaultForwardedClientIdHeader
	}

	if len(cfg.AllowedIds) > 0 {
		s.allowedIds = make(map[string]struct{}, len(cfg.AllowedIds))
		for _, id := range cfg.AllowedIds {
			s.allowedIds[strings.TrimSpace(id)] = struct{}{}
		}
	}

	for _, entry := range cfg.AllowedProxies {
		if ip := net.ParseIP(entry); ip != nil {
			s.allowedIPs = append(s.allowedIPs, ip)
			continue
		}
		_, cidr, err := net.ParseCIDR(entry)
		if err != nil {
			return nil, fmt.Errorf("invalid allowedProxies entry (must be an IP or CIDR): %s", entry)
		}
		s.allowedCIDRs = append(s.allowedCIDRs, cidr)
	}
	s.restrictToPeer = len(s.allowedIPs) > 0 || len(s.allowedCIDRs) > 0

	return s, nil
}

// Supports reports whether the configured header is present. It does not
// depend on ap.Type, so a request that also carries another credential (for
// example an API key the proxy already consumed) is still offered to this
// strategy; strategies are tried in configuration order.
func (s *ForwardedClientIdStrategy) Supports(ap *AuthPayload) bool {
	if ap == nil || ap.Headers == nil {
		return false
	}
	return len(ap.Headers.Values(s.header)) > 0
}

func (s *ForwardedClientIdStrategy) Authenticate(ctx context.Context, req *common.NormalizedRequest, ap *AuthPayload) (*common.User, error) {
	if ap == nil || ap.Headers == nil {
		return nil, common.NewErrAuthUnauthorized(string(common.AuthTypeForwardedClientId), "missing client id header")
	}

	if s.restrictToPeer && !s.isAllowedPeer(ap.RemoteAddr) {
		return nil, common.NewErrAuthUnauthorized(string(common.AuthTypeForwardedClientId), "client id header not accepted from this peer")
	}

	// More than one value means the header was appended to rather than
	// overwritten somewhere on the way, so a client-supplied copy may be
	// first. Refuse instead of guessing which one the proxy set.
	values := ap.Headers.Values(s.header)
	if len(values) != 1 {
		return nil, common.NewErrAuthUnauthorized(string(common.AuthTypeForwardedClientId), "client id header must have exactly one value")
	}
	id := strings.TrimSpace(values[0])
	if !isValidForwardedClientId(id) {
		return nil, common.NewErrAuthUnauthorized(string(common.AuthTypeForwardedClientId), "invalid client id")
	}

	if s.allowedIds != nil {
		if _, ok := s.allowedIds[id]; !ok {
			return nil, common.NewErrAuthUnauthorized(string(common.AuthTypeForwardedClientId), "client id is not allowed")
		}
	}

	user := &common.User{Id: id}
	if s.cfg.RateLimitBudget != "" {
		user.RateLimitBudget = s.cfg.RateLimitBudget
	}
	return user, nil
}

func (s *ForwardedClientIdStrategy) isAllowedPeer(remoteAddr string) bool {
	host := remoteAddr
	if h, _, err := net.SplitHostPort(remoteAddr); err == nil {
		host = h
	}
	ip := net.ParseIP(host)
	if ip == nil {
		return false
	}
	for _, allowed := range s.allowedIPs {
		if allowed.Equal(ip) {
			return true
		}
	}
	for _, cidr := range s.allowedCIDRs {
		if cidr.Contains(ip) {
			return true
		}
	}
	return false
}

// isValidForwardedClientId accepts non-empty, bounded, visible-ASCII ids
// without commas (a comma suggests several values were joined into one).
func isValidForwardedClientId(id string) bool {
	if id == "" || len(id) > maxForwardedClientIdLength {
		return false
	}
	for i := 0; i < len(id); i++ {
		c := id[i]
		if c < 0x21 || c > 0x7e || c == ',' {
			return false
		}
	}
	return true
}
