package management

import (
	"time"

	"github.com/rancher/rancher/pkg/agent/clean"
	"github.com/rancher/rancher/pkg/types/config"
	"github.com/rancher/rancher/pkg/wrangler"
	"github.com/rancher/rancher/pkg/log"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	v1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

const (
	dupeBindingsCleanupKey   = "DedupeBindingsDone"
	orphanBindingsCleanupKey = "CleanupOrphanBindingsDone"
)

func CleanupDuplicateBindings(scaledContext *config.ScaledContext, wContext *wrangler.Context) {
	// check if duplicate binding cleanup has run already
	log.Info("checking configmap to determine if duplicate bindings cleanup needs to run", "operation", "cleanup_duplicate_bindings", "namespace", cattleNamespace, "configmap", bootstrapAdminConfig)
	if adminConfig, err := wContext.K8s.CoreV1().ConfigMaps(cattleNamespace).Get(scaledContext.RunContext, bootstrapAdminConfig, v1.GetOptions{}); err != nil {
		if !apierrors.IsNotFound(err) {
			log.Warn("unable to determine if duplicate bindings cleanup has ran, skipping", "operation", "cleanup_duplicate_bindings", "error", err)
			return
		}
	} else {
		// config map already exists, check if the cleanup key is found
		if _, ok := adminConfig.Data[dupeBindingsCleanupKey]; ok {
			//cleanup has been run already, nothing to do here
			log.Info("duplicate bindings cleanup has already run, skipping", "operation", "cleanup_duplicate_bindings")
			return
		}
		// run cleanup after delay to give other controllers a chance to create CRTBs/PRTBs and ease the load on the API at startup
		const delayMinutes = 3
		log.Info("bindings cleanup needed, waiting before starting", "delayMinutes", delayMinutes)
		time.Sleep(time.Minute * delayMinutes)
		log.Info("starting duplicate binding cleanup", "operation", "cleanup_duplicate_bindings")
		err = clean.DuplicateBindings(&scaledContext.RESTConfig)
		if err != nil {
			log.Warn("error in cleaning up duplicate bindings", "operation", "cleanup_duplicate_bindings", "error", err)
			return
		}
		// update configmap
		reloadedConfig, err := wContext.K8s.CoreV1().ConfigMaps(cattleNamespace).Get(scaledContext.RunContext, bootstrapAdminConfig, v1.GetOptions{})
		if err != nil {
			if !apierrors.IsNotFound(err) {
				log.Warn("unable to load configmap", "configmap", bootstrapAdminConfig, "error", err)
				return
			}
		}

		adminConfigCopy := reloadedConfig.DeepCopy()
		if adminConfigCopy.Data == nil {
			adminConfigCopy.Data = make(map[string]string)
		}
		adminConfigCopy.Data[dupeBindingsCleanupKey] = "yes"

		_, err = wContext.K8s.CoreV1().ConfigMaps(cattleNamespace).Update(scaledContext.RunContext, adminConfigCopy, v1.UpdateOptions{})
		if err != nil {
			log.Warn("error in updating configmap to record that the duplicate binding cleanup is done", "operation", "cleanup_duplicate_bindings", "error", err, "configmap", bootstrapAdminConfig)
		}
		log.Info("successfully cleaned up duplicate bindings", "operation", "cleanup_duplicate_bindings")
	}
}

func CleanupOrphanBindings(scaledContext *config.ScaledContext, wContext *wrangler.Context) {
	err := cleanupSpecificOrphanedBindings(scaledContext, wContext, orphanBindingsCleanupKey)
	if err != nil {
		log.Error("failed to cleanup orphan bindings")
	}
}

// Runs the cleanup process for orphaned bindings given a cleanupKey specifying which cleanup job should be run (orphanBindings or orphanCatalogBindings)
func cleanupSpecificOrphanedBindings(scaledContext *config.ScaledContext, wContext *wrangler.Context, cleanupKey string) error {
	log.Info("checking configmap to determine if orphan bindings cleanup needs to run", "namespace", cattleNamespace, "configmap", bootstrapAdminConfig)
	adminConfig, err := wContext.K8s.CoreV1().ConfigMaps(cattleNamespace).Get(scaledContext.RunContext, bootstrapAdminConfig, v1.GetOptions{})
	if err != nil {
		log.Warn("unable to determine if bindings cleanup has ran, skipping", "cleanupKey", cleanupKey, "error", err)
		return err
	}

	// config map exists, check if the cleanup key is found
	if _, ok := adminConfig.Data[cleanupKey]; ok {
		log.Info("orphan bindings cleanup has already run, skipping", "cleanupKey", cleanupKey)
		return nil
	}

	// run cleanup
	if cleanupKey == orphanBindingsCleanupKey {
		err = clean.OrphanBindings(&scaledContext.RESTConfig)
	}
	if err != nil {
		log.Warn("error during orphan binding cleanup", "cleanupKey", cleanupKey, "error", err)
		return err
	}

	// update configmap
	reloadedConfig, err := wContext.K8s.CoreV1().ConfigMaps(cattleNamespace).Get(scaledContext.RunContext, bootstrapAdminConfig, v1.GetOptions{})
	if err != nil {
		log.Warn("unable to get configmap", "cleanupKey", cleanupKey, "configmap", bootstrapAdminConfig, "error", err)
		return err
	}

	adminConfigCopy := reloadedConfig.DeepCopy()
	if adminConfigCopy.Data == nil {
		adminConfigCopy.Data = make(map[string]string)
	}
	adminConfigCopy.Data[cleanupKey] = "yes"

	_, err = wContext.K8s.CoreV1().ConfigMaps(cattleNamespace).Update(scaledContext.RunContext, adminConfigCopy, v1.UpdateOptions{})
	if err != nil {
		log.Warn("error while updating configmap, unable to record completion of orphan binding cleanup", "cleanupKey", cleanupKey, "error", err, "configmap", bootstrapAdminConfig)
	}

	log.Info("successfully cleaned up orphan bindings", "cleanupKey", cleanupKey)
	return nil
}
