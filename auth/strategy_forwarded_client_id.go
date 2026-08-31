package auth

import (
	"context"
	"strings"

	"github.com/erpc/erpc/common"
)

// ForwardedClientIdStrategy authenticates using a non-secret client id
// header injected by a trusted gateway after API-key verification
// (e.g. Envoy SecurityPolicy apiKeyAuth.forwardClientIDHeader).
type ForwardedClientIdStrategy struct {
	cfg *common.ForwardedClientIdStrategyConfig
}

var _ AuthStrategy = &ForwardedClientIdStrategy{}

func NewForwardedClientIdStrategy(cfg *common.ForwardedClientIdStrategyConfig) *ForwardedClientIdStrategy {
	return &ForwardedClientIdStrategy{cfg: cfg}
}

func (s *ForwardedClientIdStrategy) Supports(ap *AuthPayload) bool {
	return ap != nil && ap.Type == common.AuthTypeForwardedClientId
}

func (s *ForwardedClientIdStrategy) Authenticate(ctx context.Context, req *common.NormalizedRequest, ap *AuthPayload) (*common.User, error) {
	if ap == nil || ap.ForwardedClientId == nil || strings.TrimSpace(ap.ForwardedClientId.Value) == "" {
		return nil, common.NewErrAuthUnauthorized("forwardedClientId", "missing client id header")
	}

	id := strings.TrimSpace(ap.ForwardedClientId.Value)
	user := &common.User{Id: id}
	if s.cfg != nil && s.cfg.RateLimitBudget != "" {
		user.RateLimitBudget = s.cfg.RateLimitBudget
	} else if ap.ForwardedClientId.RateLimitBudget != "" {
		user.RateLimitBudget = ap.ForwardedClientId.RateLimitBudget
	}
	return user, nil
}
