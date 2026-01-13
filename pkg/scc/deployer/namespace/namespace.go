package namespace

import (
	"context"
	"fmt"
	"log/slog"
	"reflect"
	"slices"

	"github.com/cenkalti/backoff/v4"
	"github.com/rancher/rancher/pkg/scc/consts"
	"github.com/rancher/rancher/pkg/scc/deployer/types"
	"github.com/rancher/rancher/pkg/scc/util/generic"
	"github.com/rancher/rancher/pkg/scc/util/log"
	v1core "github.com/rancher/wrangler/v3/pkg/generated/controllers/core/v1"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

// Deployer implements ResourceDeployer for Namespace resources
type Deployer struct {
	log        log.StructuredLogger
	namespaces v1core.NamespaceController
}

// NewDeployer creates a new NamespaceDeployer
func NewDeployer(logger log.StructuredLogger, namespaces v1core.NamespaceController) *Deployer {
	return &Deployer{
		log:        logger.With(slog.String("deployer", "namespace")),
		namespaces: namespaces,
	}
}

func (d *Deployer) HasResource() (bool, error) {
	existing, err := d.namespaces.Get(consts.DefaultSCCNamespace, metav1.GetOptions{})
	if err != nil && !errors.IsNotFound(err) {
		return false, fmt.Errorf("error getting existing SCC namespace: %v", err)
	}

	return existing != nil, nil
}

// Ensure ensures that the SCC namespace exists and is not marked for deletion
func (d *Deployer) Ensure(ctx context.Context, labels map[string]string) error {
	// Check if namespace exists
	existingSccNs, err := d.namespaces.Get(consts.DefaultSCCNamespace, metav1.GetOptions{})
	if err != nil && !errors.IsNotFound(err) {
		return fmt.Errorf("error checking for namespace %s: %w", consts.DefaultSCCNamespace, err)
	}

	desiredSccNs := &corev1.Namespace{
		ObjectMeta: metav1.ObjectMeta{
			Name:   consts.DefaultSCCNamespace,
			Labels: labels,
			Finalizers: []string{
				consts.FinalizerSccNamespace,
			},
		},
	}

	// If namespace doesn't exist, create it
	if err != nil && errors.IsNotFound(err) {
		_, err = d.namespaces.Create(desiredSccNs)
		d.log.Info("Created namespace", "namespace", consts.DefaultSCCNamespace)
		return err
	}

	// Check if namespace is marked for deletion
	if existingSccNs.DeletionTimestamp != nil {
		d.log.Info("Namespace is marked for deletion, cleaning up and recreating", "namespace", consts.DefaultSCCNamespace)

		// Try to remove finalizers to speed up deletion
		finalizerIndex := slices.Index(existingSccNs.Finalizers, consts.FinalizerSccNamespace)
		if len(existingSccNs.Finalizers) > 0 && finalizerIndex != -1 {
			d.log.Info("Removing finalizers from namespace to speed up deletion", "namespace", consts.DefaultSCCNamespace)
			existingSccNs.Finalizers = slices.Delete(existingSccNs.Finalizers, finalizerIndex, 1)
			if _, err := d.namespaces.Update(existingSccNs); err != nil {
				d.log.Warn("Failed to remove finalizers from namespace", "namespace", consts.DefaultSCCNamespace, "error", err)
			}
		}

		// Check if namespace is actually gone after removing finalizers
		var getErr error
		retryErr := backoff.Retry(
			func() error {
				var ns *corev1.Namespace
				ns, getErr = d.namespaces.Get(consts.DefaultSCCNamespace, metav1.GetOptions{})
				if getErr != nil && !errors.IsNotFound(getErr) {
					return nil
				}

				if ns != nil {
					return fmt.Errorf("namespace %s still exists", consts.DefaultSCCNamespace)
				}

				return nil
			},
			backoff.WithMaxRetries(backoff.NewConstantBackOff(3), 3),
		)
		if retryErr != nil {
			return fmt.Errorf("encountered error while waiting for namespace to clean up: %w", retryErr)
		}

		if getErr != nil && errors.IsNotFound(getErr) {
			// Namespace is gone, create a new one
			d.log.Info("Namespace is deleted, creating new one", "namespace", consts.DefaultSCCNamespace)
			_, err = d.namespaces.Create(desiredSccNs)
			if err != nil {
				return fmt.Errorf("failed to create namespace %s after deletion: %w", consts.DefaultSCCNamespace, err)
			}
			d.log.Info("Created namespace after deletion", "namespace", consts.DefaultSCCNamespace)
			return nil
		}

		// Namespace is still being deleted
		return fmt.Errorf("namespace %s is still being deleted, waiting for deletion to complete before recreating", consts.DefaultSCCNamespace)
	}

	// Update the namespace if needed
	patchUpdatedNs, patchCreateErr := generic.PreparePatchUpdated(existingSccNs, desiredSccNs)
	if patchCreateErr != nil {
		return patchCreateErr
	}

	// Check if update is needed
	if reflect.DeepEqual(existingSccNs, patchUpdatedNs) {
		d.log.Debug("Namespace is up to date", "namespace", consts.DefaultSCCNamespace)
		return nil
	}

	// Update the namespace
	d.log.Info("Updating namespace", "namespace", consts.DefaultSCCNamespace)
	if _, err := d.namespaces.Update(patchUpdatedNs); err != nil {
		return fmt.Errorf("failed to update namespace %s: %w", consts.DefaultSCCNamespace, err)
	}

	d.log.Info("Updated namespace", "namespace", consts.DefaultSCCNamespace)

	return nil
}

var _ types.ResourceDeployer = &Deployer{}
