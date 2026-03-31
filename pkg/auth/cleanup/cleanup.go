package cleanup

import (
	"encoding/json"
	"errors"

	"github.com/rancher/rancher/pkg/auth/api/secrets"
	v3 "github.com/rancher/rancher/pkg/generated/controllers/management.cattle.io/v3"
	"github.com/rancher/rancher/pkg/log"
	wcorev1 "github.com/rancher/wrangler/v3/pkg/generated/controllers/core/v1"
	"k8s.io/apimachinery/pkg/types"
)

var cleanupProviders = []string{"genericoidc", "cognito"}

const cleanedUpSecretsAnnotation = "auth.cattle.io/unused-secrets-cleaned"

// CleanupUnusedSecretTokens removes tokens from the cattle-system namespace that have
// been removed from the PerUserCacheProviders.
//
// The AuthConfig is annotated to indicate that the secrets have been cleaned.
func CleanupUnusedSecretTokens(secretsInterface wcorev1.SecretController, authConfigs v3.AuthConfigController) (cleanupErr error) {
	for _, name := range cleanupProviders {
		authConfig, err := authConfigs.Cache().Get(name)
		if err != nil {
			log.Error("Getting AuthConfig", "operation", "cleanup_unused_secret_tokens", "auth_config", name, "error", err)
			cleanupErr = errors.Join(cleanupErr, err)
			continue
		}

		if val := authConfig.Annotations[cleanedUpSecretsAnnotation]; val == "true" {
			continue
		}

		log.Info("Cleaning unused tokens from provider", "operation", "cleanup_unused_secret_tokens", "provider", name)
		if err := secrets.CleanupOAuthTokens(secretsInterface, name); err != nil {
			cleanupErr = errors.Join(cleanupErr, err)
			continue
		}

		patch, err := json.Marshal(map[string]any{
			"metadata": map[string]any{
				"annotations": map[string]string{
					cleanedUpSecretsAnnotation: "true",
				},
			},
		})
		if err != nil {
			cleanupErr = errors.Join(cleanupErr, err)
			continue
		}

		if _, err := authConfigs.Patch(name, types.MergePatchType, patch); err != nil {
			cleanupErr = errors.Join(cleanupErr, err)
		}
	}

	return
}
