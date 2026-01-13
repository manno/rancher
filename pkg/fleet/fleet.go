package fleet

import (
	"errors"
	"fmt"
	"os"

	"github.com/rancher/rancher/pkg/log"
	"k8s.io/client-go/rest"
	"k8s.io/client-go/tools/clientcmd"
	"k8s.io/client-go/tools/clientcmd/api"
)

// GetClusterHost returns the API server host and CA for the specified client configuration.
func GetClusterHost(clientCfg clientcmd.ClientConfig) (string, []byte, error) {
	icc, err := rest.InClusterConfig()
	if err == nil {
		ca, err := os.ReadFile(icc.CAFile)
		return icc.Host, ca, err
	}

	fail := func(err error) (string, []byte, error) {
		return "", []byte{}, fmt.Errorf("fleet.GetClusterHost: unable to determine cluster host: %w", err)
	}

	if clientCfg == nil {
		return fail(errors.New("client config not set"))
	}

	rawConfig, err := clientCfg.RawConfig()
	if err != nil {
		return fail(fmt.Errorf("no configuration available: %w", err))
	}

	cluster, ok := rawConfig.Clusters[rawConfig.CurrentContext]
	if ok {
		ca, err := getCA(cluster)
		return cluster.Server, ca, err
	}

	log.Warn("Api server host retrieval: no cluster found for current context", "operation", "get_cluster_host", "context", rawConfig.CurrentContext)

	for k, v := range rawConfig.Clusters {
		log.Warn("Api server host retrieval: picking server randomly from configured clusters", "operation", "get_cluster_host", "server", v.Server, "reference", k)
		ca, err := getCA(v)
		return v.Server, ca, err
	}

	return "", []byte{}, errors.New("failed to find cluster server parameter")
}

// getCA retrieves certificate authority information for the specified cluster.
func getCA(cluster *api.Cluster) ([]byte, error) {
	if len(cluster.CertificateAuthorityData) > 0 {
		return cluster.CertificateAuthorityData, nil
	}
	return os.ReadFile(cluster.CertificateAuthority)
}
