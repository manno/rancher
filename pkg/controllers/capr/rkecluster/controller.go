package rkecluster

import (
	"context"
	"time"

	v1 "github.com/rancher/rancher/pkg/apis/rke.cattle.io/v1"
	"github.com/rancher/rancher/pkg/capr"
	capicontrollers "github.com/rancher/rancher/pkg/generated/controllers/cluster.x-k8s.io/v1beta1"
	rkecontroller "github.com/rancher/rancher/pkg/generated/controllers/rke.cattle.io/v1"
	"github.com/rancher/rancher/pkg/wrangler"
	"github.com/rancher/wrangler/v3/pkg/generic"
	"github.com/rancher/wrangler/v3/pkg/relatedresource"
	"github.com/rancher/rancher/pkg/log"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/runtime"
	capi "sigs.k8s.io/cluster-api/api/v1beta1"
	capiannotations "sigs.k8s.io/cluster-api/util/annotations"
)

type handler struct {
	rkeCluster       rkecontroller.RKEClusterController
	capiClusterCache capicontrollers.ClusterCache
}

func Register(ctx context.Context, clients *wrangler.CAPIContext) {
	h := handler{
		rkeCluster:       clients.RKE.RKECluster(),
		capiClusterCache: clients.CAPI.Cluster().Cache(),
	}

	clients.RKE.RKECluster().OnChange(ctx, "rke-cluster", h.OnChange)
	relatedresource.Watch(ctx, "rke-cluster-trigger", func(namespace, name string, obj runtime.Object) ([]relatedresource.Key, error) {
		if capiCluster, ok := obj.(*capi.Cluster); ok && !capiCluster.Spec.Paused {
			return []relatedresource.Key{{
				Namespace: namespace,
				Name:      name,
			}}, nil
		}
		return nil, nil
	}, clients.RKE.RKECluster(), clients.CAPI.Cluster())
}

func (h *handler) OnChange(_ string, cluster *v1.RKECluster) (*v1.RKECluster, error) {
	if cluster == nil {
		return nil, nil
	}

	capiCluster, err := capr.GetOwnerCAPICluster(cluster, h.capiClusterCache)
	if apierrors.IsNotFound(err) {
		log.Debug("Waiting: CAPI cluster does not exist", "operation", "on_change", "namespace", cluster.Namespace, "cluster", cluster.Name)
		h.rkeCluster.EnqueueAfter(cluster.Namespace, cluster.Name, 10*time.Second)
		return cluster, generic.ErrSkip
	}
	if err != nil {
		log.Error("Error getting CAPI cluster", "operation", "on_change", "namespace", cluster.Namespace, "cluster", cluster.Name, "error", err)
		return cluster, err
	}

	if capiannotations.IsPaused(capiCluster, cluster) {
		log.Info("Waiting: CAPI cluster or RKECluster is paused", "operation", "on_change", "namespace", cluster.Namespace, "cluster", cluster.Name)
		return cluster, generic.ErrSkip
	}

	if cluster.Spec.ControlPlaneEndpoint == nil || !cluster.Spec.ControlPlaneEndpoint.IsValid() {
		cluster := cluster.DeepCopy()
		cluster.Spec.ControlPlaneEndpoint = &capi.APIEndpoint{
			Host: "localhost",
			Port: 6443,
		}
		log.Debug("Setting controlplane endpoint", "operation", "on_change", "namespace", cluster.Namespace, "cluster", cluster.Name)
		return h.rkeCluster.Update(cluster)
	}

	if len(cluster.Status.Conditions) > 0 || cluster.Status.Ready != true {
		cluster := cluster.DeepCopy()
		// the rke2.Ready and rke2.Removed conditions may still be present on the object, remove them if present
		cluster.Status.Conditions = nil
		cluster.Status.Ready = true
		log.Trace("Removing stale conditions", "operation", "on_change", "namespace", cluster.Namespace, "cluster", cluster.Name)
		log.Debug("Marking cluster ready", "operation", "on_change", "namespace", cluster.Namespace, "cluster", cluster.Name)
		return h.rkeCluster.UpdateStatus(cluster)
	}

	return cluster, nil
}
