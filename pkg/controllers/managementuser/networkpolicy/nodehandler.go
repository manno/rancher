package networkpolicy

import (
	"fmt"

	"github.com/rancher/rancher/pkg/controllers/managementuser/nodesyncer"
	v3 "github.com/rancher/rancher/pkg/generated/norman/management.cattle.io/v3"
	"github.com/rancher/rancher/pkg/log"
	"k8s.io/apimachinery/pkg/runtime"
)

type nodeHandler struct {
	npmgr            *netpolMgr
	clusterLister    v3.ClusterLister
	clusterNamespace string
}

func (nh *nodeHandler) Sync(key string, machine *v3.Node) (runtime.Object, error) {
	if key == fmt.Sprintf("%s/%s", nh.clusterNamespace, nodesyncer.AllNodeKey) {
		disabled, err := isNetworkPolicyDisabled(nh.clusterNamespace, nh.clusterLister)
		if err != nil {
			return nil, err
		}
		if disabled {
			return nil, nil
		}
		log.Debug("Nodehandler: sync", "operation", "sync", "key", key)
		return nil, nh.npmgr.handleHostNetwork(nh.clusterNamespace)
	}
	return nil, nil
}
