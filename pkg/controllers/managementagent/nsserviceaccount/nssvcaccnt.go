package nsserviceaccount

import (
	"context"

	rv1 "github.com/rancher/rancher/pkg/generated/norman/core/v1"
	"github.com/rancher/rancher/pkg/log"
	"github.com/rancher/rancher/pkg/types/config"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/runtime"
)

const (
	projectIDAnnotation        = "field.cattle.io/projectId"
	sysNamespaceAnnotation     = "management.cattle.io/system-namespace"
	NoDefaultSATokenAnnotation = "management.cattle.io/no-default-sa-token"
)

type defaultSvcAccountHandler struct {
	serviceAccountsLister rv1.ServiceAccountLister
	serviceAccounts       rv1.ServiceAccountInterface
}

func Register(ctx context.Context, cluster *config.UserOnlyContext) {
	log.Debug("Registering defaultSvcAccountHandler for checking default service account of system namespaces", "operation", "register_default_sa_handler")
	nsh := &defaultSvcAccountHandler{
		serviceAccounts:       cluster.Core.ServiceAccounts(""),
		serviceAccountsLister: cluster.Core.ServiceAccounts("").Controller().Lister(),
	}
	cluster.Core.Namespaces("").AddHandler(ctx, "defaultSvcAccountHandler", nsh.Sync)
}

func (nsh *defaultSvcAccountHandler) Sync(key string, ns *corev1.Namespace) (runtime.Object, error) {
	if ns == nil || ns.DeletionTimestamp != nil {
		return nil, nil
	}
	log.Debug("Syncing default service account", "operation", "sync_default_sa", "key", key, "namespace", ns.Name)
	//handle default svcAccount of system namespaces only
	if err := nsh.handleIfSystemNSDefaultSA(ns); err != nil {
		log.Error("Error handling default ServiceAccount", "operation", "sync_default_sa", "key", key, "error", err)
	}
	return nil, nil
}

func (nsh *defaultSvcAccountHandler) handleIfSystemNSDefaultSA(ns *corev1.Namespace) error {
	if ns.Annotations[NoDefaultSATokenAnnotation] != "true" {
		return nil
	}

	defSvcAccnt, err := nsh.serviceAccountsLister.Get(ns.Name, "default")
	if apierrors.IsNotFound(err) {
		return nil
	} else if err != nil {
		return err
	}

	if defSvcAccnt.AutomountServiceAccountToken != nil && !*defSvcAccnt.AutomountServiceAccountToken {
		return nil
	}
	automountServiceAccountToken := false
	defSvcAccnt.AutomountServiceAccountToken = &automountServiceAccountToken
	log.Debug("Updating default service account", "operation", "update_default_sa", "namespace", ns.Name)
	_, err = nsh.serviceAccounts.Update(defSvcAccnt)
	if err != nil {
		log.Error("Error updating default service account flag", "operation", "update_default_sa", "namespace", ns.Name, "error", err)
		return err
	}
	return nil
}
