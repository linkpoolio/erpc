package common

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestSecretStrategyConfig_ValidateAdditionalLocations(t *testing.T) {
	ok := &SecretStrategyConfig{Value: "s", AdditionalLocations: []SecretLocation{SecretLocationApikeyQuery, SecretLocationApikeyHeader, SecretLocationPath}}
	assert.NoError(t, ok.Validate())

	bad := &SecretStrategyConfig{Value: "s", AdditionalLocations: []SecretLocation{"cookie"}}
	assert.ErrorContains(t, bad.Validate(), `unknown location "cookie"`)

	db := &DatabaseStrategyConfig{Connector: &ConnectorConfig{Id: "c", Driver: DriverMemory, Memory: &MemoryConnectorConfig{MaxItems: 10}}, AdditionalLocations: []SecretLocation{"cookie"}}
	assert.ErrorContains(t, db.Validate(), `unknown location "cookie"`)
}

func TestAuthConfig_AcceptsSecretLocation(t *testing.T) {
	var nilCfg *AuthConfig
	assert.False(t, nilCfg.AcceptsSecretLocation(SecretLocationPath))

	cfg := &AuthConfig{Strategies: []*AuthStrategyConfig{
		{Type: AuthTypeNetwork, Network: &NetworkStrategyConfig{}},
		{Type: AuthTypeSecret, Secret: &SecretStrategyConfig{Value: "s", AdditionalLocations: []SecretLocation{SecretLocationApikeyHeader}}},
	}}
	assert.True(t, cfg.AcceptsSecretLocation(SecretLocationApikeyHeader))
	assert.False(t, cfg.AcceptsSecretLocation(SecretLocationPath))

	cfg.Strategies = append(cfg.Strategies, &AuthStrategyConfig{Type: AuthTypeDatabase, Database: &DatabaseStrategyConfig{AdditionalLocations: []SecretLocation{SecretLocationPath}}})
	assert.True(t, cfg.AcceptsSecretLocation(SecretLocationPath))
}
