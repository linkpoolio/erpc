package auth

import (
	"context"
	"strings"

	"github.com/erpc/erpc/common"
)

type SecretStrategy struct {
	cfg *common.SecretStrategyConfig
}

var _ AuthStrategy = &SecretStrategy{}

func NewSecretStrategy(cfg *common.SecretStrategyConfig) *SecretStrategy {
	return &SecretStrategy{cfg: cfg}
}

func (s *SecretStrategy) Supports(ap *AuthPayload) bool {
	return ap.Type == common.AuthTypeSecret
}

func (s *SecretStrategy) Authenticate(ctx context.Context, req *common.NormalizedRequest, ap *AuthPayload) (*common.User, error) {
	if ap == nil || ap.Secret == nil {
		return nil, common.NewErrAuthUnauthorized("secret", "missing secret")
	}
	// Reject empty configured or presented secrets so a missing env expansion
	// (value="") can never authenticate an empty path/query credential.
	if strings.TrimSpace(s.cfg.Value) == "" || strings.TrimSpace(ap.Secret.Value) == "" {
		return nil, common.NewErrAuthUnauthorized("secret", "invalid secret")
	}
	if ap.Secret.Value != s.cfg.Value {
		return nil, common.NewErrAuthUnauthorized("secret", "invalid secret")
	}

	user := &common.User{Id: s.cfg.Id}
	if s.cfg.RateLimitBudget != "" {
		user.RateLimitBudget = s.cfg.RateLimitBudget
	}
	return user, nil
}
