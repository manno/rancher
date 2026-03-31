package rbac

import (
	"github.com/rancher/rancher/pkg/impersonation"
	"github.com/rancher/rancher/pkg/log"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apiserver/pkg/authentication/user"
)

func (m *manager) ensureServiceAccountImpersonator(username string) error {
	log.Debug("Ensuring service account impersonator", "operation", "ensure_impersonator", "user", username)
	err := m.impersonator.SetUpImpersonation(&user.DefaultInfo{UID: username})
	if apierrors.IsNotFound(err) {
		log.Warn("Could not find user, will not create impersonation account on cluster", "operation", "ensure_impersonator", "user", username)
		return nil
	}
	return err
}

func (m *manager) deleteServiceAccountImpersonator(username string) error {
	crtbs, err := m.crtbIndexer.ByIndex(rtbByClusterAndUserIndex, m.workload.ClusterName+"-"+username)
	if err != nil {
		return err
	}
	prtbs, err := m.prtbIndexer.ByIndex(rtbByClusterAndUserIndex, m.workload.ClusterName+"-"+username)
	if err != nil {
		return err
	}
	if len(crtbs)+len(prtbs) > 0 {
		return nil
	}
	roleName := impersonation.ImpersonationPrefix + username
	log.Debug("Deleting service account impersonator", "operation", "delete_impersonator", "user", username)
	err = m.workload.RBACw.ClusterRole().Delete(roleName, &metav1.DeleteOptions{})
	if apierrors.IsNotFound(err) {
		return nil
	}
	return err
}
