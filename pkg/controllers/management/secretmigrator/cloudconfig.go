package secretmigrator

import (
	"strings"

	"github.com/rancher/norman/types/convert"
	v1 "github.com/rancher/rancher/pkg/apis/provisioning.cattle.io/v1"
	"github.com/rancher/rancher/pkg/log"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

// cloudConfigSecretRemover deletes any detected secrets within the clusters cloud-provider-config
// field which have the expected secret format, the AuthorizedSecretAnnotation,
// and the AuthorizedSecretDeletesOnClusterRemovalAnnotation. Secrets without the AuthorizedSecretDeletesOnClusterRemovalAnnotation
// annotation are not removed, and are assumed to be user created. Only secrets without any OwnerReferences are removed.
func (h *handler) cloudConfigSecretRemover(_ string, cluster *v1.Cluster) (*v1.Cluster, error) {
	if cluster == nil || cluster.Spec.RKEConfig == nil || cluster.Name == "local" {
		return cluster, nil
	}

	for _, e := range cluster.Spec.RKEConfig.MachineSelectorConfig {
		cloudProviderConfig := convert.ToString(e.Config.Data["cloud-provider-config"])

		// check if the cloud-provider-config value points to a secret
		if cloudProviderConfig == "" || !strings.HasPrefix(cloudProviderConfig, "secret://") {
			continue
		}

		cleanSecretNamespaceAndName := strings.TrimPrefix(cloudProviderConfig, "secret://")
		namespaceAndName := strings.Split(cleanSecretNamespaceAndName, ":")

		// ensure the secret format is proper
		if len(namespaceAndName) != 2 {
			log.Error("Provided secret value is not of form secret://namespace:name", "operation", "remove_cloud_config_secret")
			continue
		}

		secret, err := h.migrator.secrets.GetNamespaced(namespaceAndName[0], namespaceAndName[1], metav1.GetOptions{})
		if err != nil {
			log.Error("Error retrieving secret defined within cloud-provider-config", "operation", "remove_cloud_config_secret", "namespace", namespaceAndName[0], "secret", namespaceAndName[1], "error", err)
			continue
		}

		authorizedForCluster := secret.Annotations[AuthorizedSecretAnnotation]
		deleteSecretOnClusterRemoval := secret.Annotations[AuthorizedSecretDeletesOnClusterRemovalAnnotation]

		if authorizedForCluster == cluster.Name && deleteSecretOnClusterRemoval == "true" {
			if len(secret.OwnerReferences) == 0 {
				h.migrator.CleanupKnownSecrets([]*corev1.Secret{secret})
			}
		}
	}

	return cluster, nil
}
