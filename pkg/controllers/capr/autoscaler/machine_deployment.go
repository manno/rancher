package autoscaler

import (
	"github.com/rancher/rancher/pkg/capr"
	"github.com/rancher/rancher/pkg/generated/controllers/cluster.x-k8s.io/v1beta1"
	v1 "github.com/rancher/rancher/pkg/generated/controllers/provisioning.cattle.io/v1"
	"github.com/rancher/rancher/pkg/log"
	"k8s.io/apimachinery/pkg/util/wait"
	"k8s.io/client-go/util/retry"
	capi "sigs.k8s.io/cluster-api/api/v1beta1"
)

type machineDeploymentReplicaOverrider struct {
	clusterCache  v1.ClusterCache
	clusterClient v1.ClusterClient

	capiClusterCache v1beta1.ClusterCache
}

// syncMachinePoolReplicas synchronizes machine pool replicas between the capi MachineDeployment and v2prov Cluster object's machinePool field.
// it searches through the list of machinePools and finds the matching one which corresponds to the one the cluster-autoscaler updated, and then updates the quantity field. this triggers a scale up (or scale down).
func (s *machineDeploymentReplicaOverrider) syncMachinePoolReplicas(_ string, md *capi.MachineDeployment) (*capi.MachineDeployment, error) {
	if md == nil || md.DeletionTimestamp != nil {
		return md, nil
	}

	clusterName := md.Spec.Template.ObjectMeta.Labels[capi.ClusterNameLabel]
	if clusterName == "" {
		log.Debug("MachineDeployment has no cluster name label, skipping", "operation", "autoscaler.syncMachinePoolReplicas", "namespace", md.Namespace, "name", md.Name)
		return md, nil
	}

	machinePoolName := md.Spec.Template.ObjectMeta.Labels[capr.RKEMachinePoolNameLabel]
	if machinePoolName == "" {
		log.Debug("MachineDeployment has no machine pool name label, skipping", "operation", "autoscaler.syncMachinePoolReplicas", "namespace", md.Namespace, "name", md.Name)
		return md, nil
	}

	log.Debug("Getting CAPI Cluster", "operation", "autoscaler.syncMachinePoolReplicas", "namespace", md.Namespace, "cluster", clusterName)
	capiCluster, err := s.capiClusterCache.Get(md.Namespace, clusterName)
	if err != nil {
		log.Error("Error getting capi cluster", "operation", "autoscaler.syncMachinePoolReplicas", "namespace", md.Namespace, "cluster", clusterName, "error", err)
		return nil, err
	}

	log.Debug("Getting v2prov cluster from CAPI cluster", "operation", "autoscaler.syncMachinePoolReplicas", "namespace", capiCluster.Namespace, "cluster", capiCluster.Name)
	cluster, err := capr.GetProvisioningClusterFromCAPICluster(capiCluster, s.clusterCache)
	if err != nil {
		return nil, err
	}

	if cluster.Spec.RKEConfig == nil || cluster.Spec.RKEConfig.MachinePools == nil || len(cluster.Spec.RKEConfig.MachinePools) == 0 {
		return md, nil
	}

	needUpdate := false
	cluster = cluster.DeepCopy()
	for i := range cluster.Spec.RKEConfig.MachinePools {
		if !(cluster.Spec.RKEConfig.MachinePools[i].Name == machinePoolName) {
			continue
		}

		if cluster.Spec.RKEConfig.MachinePools[i].Quantity == nil || md.Spec.Replicas == nil {
			continue
		}

		log.Debug("Found matching machine pool", "operation", "autoscaler.syncMachinePoolReplicas", "machinePool", machinePoolName)
		if *cluster.Spec.RKEConfig.MachinePools[i].Quantity != *md.Spec.Replicas {
			log.Info("Updating cluster machine pool quantity", "operation", "autoscaler.syncMachinePoolReplicas", "namespace", cluster.Namespace, "cluster", cluster.Name, "machinePool", machinePoolName, "oldQuantity", *cluster.Spec.RKEConfig.MachinePools[i].Quantity, "newQuantity", *md.Spec.Replicas)
			cluster.Spec.RKEConfig.MachinePools[i].Quantity = md.Spec.Replicas
			needUpdate = true
		}
	}

	if needUpdate {
		log.Debug("Updating cluster", "operation", "autoscaler.syncMachinePoolReplicas", "namespace", cluster.Namespace, "cluster", cluster.Name)
		err := wait.ExponentialBackoff(retry.DefaultBackoff, func() (done bool, err error) {
			_, err = s.clusterClient.Update(cluster)
			if err != nil {
				return false, nil
			}
			return true, nil
		})
		if err != nil {
			log.Warn("Failed to update cluster machine pool to match machineDeployment", "operation", "autoscaler.syncMachinePoolReplicas", "namespace", cluster.Namespace, "cluster", cluster.Name, "machinePool", machinePoolName, "error", err)
			return nil, err
		}

		log.Debug("Successfully updated cluster", "operation", "autoscaler.syncMachinePoolReplicas", "namespace", cluster.Namespace, "cluster", cluster.Name)
	}

	return md, nil
}
