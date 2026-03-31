package dialer

import (
	"context"
	"errors"
	"fmt"
	"net"
	"net/url"
	"os"
	"strings"
	"sync"
	"time"

	v32 "github.com/rancher/rancher/pkg/apis/management.cattle.io/v3"
	v3 "github.com/rancher/rancher/pkg/generated/norman/management.cattle.io/v3"
	"github.com/rancher/rancher/pkg/log"
	"github.com/rancher/rancher/pkg/types/config"
	"github.com/rancher/rancher/pkg/types/config/dialer"
	"github.com/rancher/rancher/pkg/wrangler"
	"github.com/rancher/remotedialer"
	"k8s.io/apimachinery/pkg/util/wait"
	"k8s.io/client-go/transport"
)

const (
	WaitForAgentError = "waiting for cluster [%s] agent to connect"
)

var ErrAgentDisconnected = errors.New("cluster agent disconnected")

func NewFactory(apiContext *config.ScaledContext, wrangler *wrangler.Context) (*Factory, error) {
	return &Factory{
		clusterLister: apiContext.Management.Clusters("").Controller().Lister(),
		TunnelServer:  wrangler.TunnelServer,
		dialHolders:   map[string]*transport.DialHolder{},
	}, nil
}

type Factory struct {
	clusterLister v3.ClusterLister
	TunnelServer  *remotedialer.Server

	dialHolders     map[string]*transport.DialHolder
	dialHoldersLock sync.RWMutex
}

func (f *Factory) ClusterDialer(clusterName string, retryOnError bool) (dialer.Dialer, error) {
	return func(ctx context.Context, network, address string) (net.Conn, error) {
		d, err := f.clusterDialer(clusterName, address, retryOnError)
		if err != nil {
			log.Debug("Wait for agent error", "operation", "get_cluster_dialer", "cluster", clusterName)
			return nil, err
		}
		return d(ctx, network, address)
	}, nil
}

func (f *Factory) ClusterDialHolder(clusterName string, retryOnError bool) (*transport.DialHolder, error) {
	// Get cached dialHolder, if available
	f.dialHoldersLock.RLock()
	cached, ok := f.dialHolders[clusterName]
	f.dialHoldersLock.RUnlock()
	if ok {
		return cached, nil
	}

	// Lock for writing
	f.dialHoldersLock.Lock()
	defer f.dialHoldersLock.Unlock()

	// Check for possible writes while waiting
	if cached, ok := f.dialHolders[clusterName]; ok {
		return cached, nil
	}

	// Create new dialHolder
	clusterDialer, err := f.ClusterDialer(clusterName, retryOnError)
	if err != nil {
		return nil, err
	}
	dialHolder := &transport.DialHolder{Dial: clusterDialer}

	// Save in the cache
	f.dialHolders[clusterName] = dialHolder

	return dialHolder, nil
}

func IsCloudDriver(cluster *v3.Cluster) bool {
	return !cluster.Spec.Internal &&
		cluster.Status.Driver != "" &&
		cluster.Status.Driver != v32.ClusterDriverImported &&
		cluster.Status.Driver != v32.ClusterDriverRKE &&
		cluster.Status.Driver != v32.ClusterDriverK3s &&
		cluster.Status.Driver != v32.ClusterDriverK3os &&
		cluster.Status.Driver != v32.ClusterDriverRke2 &&
		cluster.Status.Driver != v32.ClusterDriverRancherD
}

func IsPublicCloudDriver(cluster *v3.Cluster) bool {
	return IsCloudDriver(cluster) && !HasOnlyPrivateAPIEndpoint(cluster)
}

func HasOnlyPrivateAPIEndpoint(cluster *v3.Cluster) bool {
	switch cluster.Status.Driver {
	case v32.ClusterDriverAKS:
		if cluster.Status.AKSStatus.UpstreamSpec != nil &&
			cluster.Status.AKSStatus.UpstreamSpec.PrivateCluster != nil &&
			!*cluster.Status.AKSStatus.UpstreamSpec.PrivateCluster {
			return false
		}
		return cluster.Status.AKSStatus.PrivateRequiresTunnel != nil &&
			*cluster.Status.AKSStatus.PrivateRequiresTunnel
	case v32.ClusterDriverEKS:
		if cluster.Status.EKSStatus.UpstreamSpec != nil &&
			cluster.Status.EKSStatus.UpstreamSpec.PublicAccess != nil &&
			*cluster.Status.EKSStatus.UpstreamSpec.PublicAccess {
			return false
		}
		return cluster.Status.EKSStatus.PrivateRequiresTunnel != nil &&
			*cluster.Status.EKSStatus.PrivateRequiresTunnel
	case v32.ClusterDriverGKE:
		if cluster.Status.GKEStatus.UpstreamSpec != nil &&
			cluster.Status.GKEStatus.UpstreamSpec.PrivateClusterConfig != nil &&
			!cluster.Status.GKEStatus.UpstreamSpec.PrivateClusterConfig.EnablePrivateEndpoint {
			return false
		}
		return cluster.Status.GKEStatus.PrivateRequiresTunnel != nil &&
			*cluster.Status.GKEStatus.PrivateRequiresTunnel
	case v32.ClusterDriverAlibaba:
		if cluster.Status.AliStatus.UpstreamSpec != nil &&
			cluster.Status.AliStatus.UpstreamSpec.EndpointPublicAccess != nil &&
			*cluster.Status.AliStatus.UpstreamSpec.EndpointPublicAccess {
			return false
		}
		return cluster.Status.AliStatus.PrivateRequiresTunnel != nil &&
			*cluster.Status.AliStatus.PrivateRequiresTunnel
	default:
		return false
	}
}

var nativeDialer dialer.Dialer = (&net.Dialer{
	Timeout:   30 * time.Second,
	KeepAlive: 30 * time.Second,
}).DialContext

func (f *Factory) clusterDialer(clusterName, address string, retryOnError bool) (dialer.Dialer, error) {
	cluster, err := f.clusterLister.Get("", clusterName)
	if err != nil {
		return nil, err
	}

	if cluster.Spec.Internal {
		// For local (embedded, or import) we just assume we can connect directly
		return nativeDialer, nil
	}

	hostPort := hostPort(cluster)
	log.Trace("DialerFactory apiEndpoint hostPort for cluster", "operation", "get_cluster_dialer", "cluster", clusterName, "hostPort", hostPort)
	if (address == hostPort || isProxyAddress(address)) && IsPublicCloudDriver(cluster) {
		// For cloud drivers we just connect directly to the k8s API, not through the tunnel.  All other go through tunnel
		return nativeDialer, nil
	}

	if f.TunnelServer.HasSession(cluster.Name) {
		log.Trace("DialerFactory tunnel session found for cluster", "operation", "get_cluster_dialer", "cluster", cluster.Name)
		cd := f.TunnelServer.Dialer(cluster.Name)
		return func(ctx context.Context, network, address string) (net.Conn, error) {
			log.Trace("DialerFactory returning network and address as clusterDialer", "operation", "get_cluster_dialer", "network", network, "address", address)
			return cd(ctx, network, address)
		}, nil
	}

	if !retryOnError {
		log.Debug("No active connection for cluster, returning", "operation", "get_cluster_dialer", "cluster", cluster.Name)
		return nil, ErrAgentDisconnected
	}

	log.Debug("No active connection for cluster, will wait for about 30 seconds", "operation", "get_cluster_dialer", "cluster", cluster.Name)
	for i := 0; i < 4; i++ {
		if f.TunnelServer.HasSession(cluster.Name) {
			log.Debug("Cluster has reconnected, resuming", "operation", "get_cluster_dialer", "cluster", cluster.Name)
			cd := f.TunnelServer.Dialer(cluster.Name)
			return func(ctx context.Context, network, address string) (net.Conn, error) {
				log.Trace("DialerFactory returning network and address as clusterDialer", "operation", "get_cluster_dialer", "network", network, "address", address)
				return cd(ctx, network, address)
			}, nil
		}
		time.Sleep(wait.Jitter(5*time.Second, 1))
	}

	return nil, ErrAgentDisconnected
}

func hostPort(cluster *v3.Cluster) string {
	u, err := url.Parse(cluster.Status.APIEndpoint)
	if err != nil {
		return ""
	}

	if strings.Contains(u.Host, ":") {
		return u.Host
	}
	return u.Host + ":443"
}

func isProxyAddress(address string) bool {
	proxy := getEnvAny("HTTP_PROXY", "http_proxy")
	if proxy == "" {
		proxy = getEnvAny("HTTPS_PROXY", "https_proxy")
	}

	if proxy == "" {
		return false
	}

	parsed, err := parseProxy(proxy)
	if err != nil {
		log.Warn("Failed to parse http_proxy url", "operation", "get_cluster_dialer", "proxy", proxy, "error", err)
		return false
	}
	return parsed.Host == address
}

func getEnvAny(names ...string) string {
	for _, n := range names {
		if val := os.Getenv(n); val != "" {
			return val
		}
	}
	return ""
}

func parseProxy(proxy string) (*url.URL, error) {
	if proxy == "" {
		return nil, nil
	}

	proxyURL, err := url.Parse(proxy)
	if err != nil ||
		(proxyURL.Scheme != "http" &&
			proxyURL.Scheme != "https" &&
			proxyURL.Scheme != "socks5") {
		// proxy was bogus. Try pre-pending "http://" to it and
		// see if that parses correctly. If not, fall through
		if proxyURL, err := url.Parse("http://" + proxy); err == nil {
			return proxyURL, nil
		}
	}
	if err != nil {
		return nil, fmt.Errorf("invalid proxy address %q: %v", proxy, err)
	}
	return proxyURL, nil
}
