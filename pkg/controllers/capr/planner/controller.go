package planner

import (
	"context"
	"errors"
	"strings"
	"time"

	"github.com/rancher/wrangler/v3/pkg/condition"

	rkev1 "github.com/rancher/rancher/pkg/apis/rke.cattle.io/v1"
	"github.com/rancher/rancher/pkg/capr"
	caprplanner "github.com/rancher/rancher/pkg/capr/planner"
	v1 "github.com/rancher/rancher/pkg/generated/controllers/rke.cattle.io/v1"
	"github.com/rancher/rancher/pkg/log"
	"github.com/rancher/rancher/pkg/wrangler"
	"github.com/rancher/wrangler/v3/pkg/generic"
	"github.com/rancher/wrangler/v3/pkg/relatedresource"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/equality"
	"k8s.io/apimachinery/pkg/runtime"
	capi "sigs.k8s.io/cluster-api/api/core/v1beta2"
)

var (
	capiScalingUpCondition   = condition.Cond("ScalingUp")
	capiScalingDownCondition = condition.Cond("ScalingDown")
	capiRollingOutCondition  = condition.Cond("RollingOut")
)

type handler struct {
	planner       *caprplanner.Planner
	controlPlanes v1.RKEControlPlaneController
}

func Register(ctx context.Context, clients *wrangler.CAPIContext, planner *caprplanner.Planner) {
	h := handler{
		planner:       planner,
		controlPlanes: clients.RKE.RKEControlPlane(),
	}
	v1.RegisterRKEControlPlaneStatusHandler(ctx, clients.RKE.RKEControlPlane(), "", "planner", h.OnChange)
	relatedresource.Watch(ctx, "planner", func(namespace, name string, obj runtime.Object) ([]relatedresource.Key, error) {
		if secret, ok := obj.(*corev1.Secret); ok {
			var relatedResources []relatedresource.Key
			clusterName := secret.Labels[capr.ClusterNameLabel]
			if clusterName != "" {
				log.Trace("Rkecluster enqueue triggered by secret", "namespace", secret.Namespace, "cluster", clusterName, "secret_namespace", secret.Namespace, "secret_name", secret.Name)
				relatedResources = append(relatedResources, relatedresource.Key{
					Namespace: secret.Namespace,
					Name:      clusterName,
				})
			}
			authorizedObjects := secret.Annotations[capr.AuthorizedObjectAnnotation]
			if authorizedObjects != "" {
				for _, clusterName = range strings.Split(authorizedObjects, ",") {
					log.Trace("Rkecluster enqueue triggered by authorized secret", "namespace", secret.Namespace, "cluster", clusterName, "secret_namespace", secret.Namespace, "secret_name", secret.Name)
					relatedResources = append(relatedResources, relatedresource.Key{
						Namespace: secret.Namespace,
						Name:      clusterName,
					})
				}
			}
			return relatedResources, nil
		} else if machine, ok := obj.(*capi.Machine); ok {
			clusterName := machine.Labels[capi.ClusterNameLabel]
			if clusterName != "" {
				log.Trace("Rkecluster enqueue triggered by machine", "namespace", machine.Namespace, "cluster", clusterName, "machine_namespace", machine.Namespace, "machine_name", machine.Name)
				return []relatedresource.Key{{
					Namespace: machine.Namespace,
					Name:      clusterName,
				}}, nil
			}
		} else if configmap, ok := obj.(*corev1.ConfigMap); ok {
			var relatedResources []relatedresource.Key
			authorizedObjects := configmap.Annotations[capr.AuthorizedObjectAnnotation]
			if authorizedObjects != "" {
				for _, clusterName := range strings.Split(authorizedObjects, ",") {
					log.Trace("Rkecluster enqueue triggered by authorized configmap", "namespace", configmap.Namespace, "cluster", clusterName, "configmap_namespace", configmap.Namespace, "configmap_name", configmap.Name)
					relatedResources = append(relatedResources, relatedresource.Key{
						Namespace: configmap.Namespace,
						Name:      clusterName,
					})
				}
			}
			return relatedResources, nil
		}
		return nil, nil
	}, clients.RKE.RKEControlPlane(), clients.Core.Secret(), clients.CAPI.Machine(), clients.Core.ConfigMap())
}

func (h *handler) OnChange(cp *rkev1.RKEControlPlane, status rkev1.RKEControlPlaneStatus) (rkev1.RKEControlPlaneStatus, error) {
	log.Debug("Rkecluster handler WaitForClient called", "namespace", cp.Namespace, "name", cp.Name)
	if !cp.DeletionTimestamp.IsZero() {
		return status, nil
	}

	// With the upcoming CAPI v1beta2, status objects were changed to add new fields and conditions. Unfortunately, for
	// clusters without machine deployments or machine pools, the controlplane MUST have the `ScalingUp`, `ScalingDown`,
	// and `RollingOut` conditions. See https://github.com/kubernetes-sigs/cluster-api/issues/11820.
	scalingUpFound := false
	scalingDownFound := false
	rollingOutFound := false

	for _, cond := range status.Conditions {
		if cond.Type == string(capiScalingUpCondition) {
			scalingUpFound = true
		} else if cond.Type == string(capiScalingDownCondition) {
			scalingDownFound = true
		} else if cond.Type == string(capiRollingOutCondition) {
			rollingOutFound = true
		}
	}

	if !scalingUpFound || !scalingDownFound || !rollingOutFound {
		log.Debug("Rkecluster setting CAPI v1beta2 conditions", "namespace", cp.Namespace, "name", cp.Name)
		capiScalingUpCondition.False(&status)
		capiScalingDownCondition.False(&status)
		capiRollingOutCondition.False(&status)
		return status, nil
	}

	status.ObservedGeneration = cp.Generation

	log.Debug("Rkecluster calling planner process", "namespace", cp.Namespace, "name", cp.Name)
	status, err := h.planner.Process(cp, status)
	if err != nil {
		// planner.Process can encounter 3 types of errors:
		// * planner.errWaiting - This is an error that indicates we are waiting for something, and will not re-enqueue the object
		// * generic.ErrSkip - These will cause the object to be re-enqueued after 5 seconds.
		// * error - All other errors. This should be an actual error during planner processing.
		if caprplanner.IsErrWaiting(err) {
			log.Info("Rkecluster waiting for resources", "namespace", cp.Namespace, "name", cp.Name, "error", err)
			capr.Ready.SetStatus(&status, "Unknown")
			capr.Ready.Message(&status, err.Error())
			capr.Ready.Reason(&status, "Waiting")
			// Set err to nil so planner doesn't automatically re-enqueue the object, as we're waiting.
			// If the Reconciled condition is already true and the error was NOT an errIgnore/ErrSkip/ErrWaiting and the status.AppliedSpec (from planner.Process) does not match the controlplane spec, set reconciled to unknown.
			if !equality.Semantic.DeepEqual(cp.Spec, status.AppliedSpec) {
				capr.Reconciled.SetStatus(&status, "Unknown")
				capr.Reconciled.Message(&status, "RKEControlPlane has not been fully reconciled yet")
				capr.Reconciled.Reason(&status, "Waiting")
			}
			return status, nil
		}
		if errors.Is(err, generic.ErrSkip) {
			log.Debug("Rkecluster skip processing", "namespace", cp.Namespace, "name", cp.Name, "error", err)
			h.controlPlanes.EnqueueAfter(cp.Namespace, cp.Name, 5*time.Second)
			return status, err
		}
		// An actual error occurred, so set the Ready and Reconciled conditions to this error and return
		log.Error("Rkecluster error during plan processing", "namespace", cp.Namespace, "name", cp.Name, "error", err)
		capr.Ready.SetError(&status, "", err)
		capr.Reconciled.SetError(&status, "", err)
		return status, err
	}
	// No error encountered during planner.Process
	log.Debug("Rkecluster reconciliation complete", "namespace", cp.Namespace, "name", cp.Name)
	capr.Ready.True(&status)
	capr.Ready.Message(&status, "")
	capr.Ready.Reason(&status, "")
	capr.Stable.True(&status)
	capr.Stable.Message(&status, "")
	capr.Stable.Reason(&status, "")
	status.AppliedSpec = &cp.Spec
	capr.Reconciled.True(&status)
	capr.Reconciled.Message(&status, "")
	capr.Reconciled.Reason(&status, "")
	return status, nil
}
