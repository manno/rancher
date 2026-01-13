package management

import (
	"time"

	"github.com/rancher/rancher/pkg/agent/clean"
	"github.com/rancher/rancher/pkg/log"
	"github.com/rancher/rancher/pkg/types/config"
	"github.com/rancher/rancher/pkg/wrangler"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	v1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

const (
	dupeBindingsCleanupKey   = "DedupeBindingsDone"
	orphanBindingsCleanupKey = "CleanupOrphanBindingsDone"
)

func CleanupDuplicateBindings(scaledContext *config.ScaledContext, wContext *wrangler.Context) {
	// check if duplicate binding cleanup has run already
	log.Info("Checking configmap to determine if duplicate bindings cleanup needs to run", "operation", "cleanup_duplicate_bindings", "namespace", cattleNamespace, "configmap", bootstrapAdminConfig)
	if adminConfig, err := wContext.K8s.CoreV1().ConfigMaps(cattleNamespace).Get(scaledContext.RunContext, bootstrapAdminConfig, v1.GetOptions{}); err != nil {
		if !apierrors.IsNotFound(err) {
			log.Warn("Unable to determine if duplicate bindings cleanup has ran, skipping", "operation", "cleanup_duplicate_bindings", "error", err)
			return
		}
	} else {
		// config map already exists, check if the cleanup key is found
		if _, ok := adminConfig.Data[dupeBindingsCleanupKey]; ok {
			//cleanup has been run already, nothing to do here
			log.Info("Duplicate bindings cleanup has already run, skipping", "operation", "cleanup_duplicate_bindings")
			return
		}
		// run cleanup after delay to give other controllers a chance to create CRTBs/PRTBs and ease the load on the API at startup
		const delayMinutes = 3
		log.Info("Bindings cleanup needed, waiting before starting", "delayMinutes", delayMinutes)
		time.Sleep(time.Minute * delayMinutes)
		log.Info("Starting duplicate binding cleanup", "operation", "cleanup_duplicate_bindings")
		err = clean.DuplicateBindings(&scaledContext.RESTConfig)
		if err != nil {
			log.Warn("Error in cleaning up duplicate bindings", "operation", "cleanup_duplicate_bindings", "error", err)
			return
		}
		// update configmap
		reloadedConfig, err := wContext.K8s.CoreV1().ConfigMaps(cattleNamespace).Get(scaledContext.RunContext, bootstrapAdminConfig, v1.GetOptions{})
		if err != nil {
			if !apierrors.IsNotFound(err) {
				log.Warn("Unable to load configmap", "configmap", bootstrapAdminConfig, "error", err)
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
			log.Warn("Error in updating configmap to record that the duplicate binding cleanup is done", "operation", "cleanup_duplicate_bindings", "error", err, "configmap", bootstrapAdminConfig)
		}
		log.Info("Successfully cleaned up duplicate bindings", "operation", "cleanup_duplicate_bindings")
	}
}

func CleanupOrphanBindings(scaledContext *config.ScaledContext, wContext *wrangler.Context) {
	err := cleanupSpecificOrphanedBindings(scaledContext, wContext, orphanBindingsCleanupKey)
	if err != nil {
		log.Error("Failed to cleanup orphan bindings")
	}
}

// Runs the cleanup process for orphaned bindings given a cleanupKey specifying which cleanup job should be run (orphanBindings or orphanCatalogBindings)
func cleanupSpecificOrphanedBindings(scaledContext *config.ScaledContext, wContext *wrangler.Context, cleanupKey string) error {
	log.Info("Checking configmap to determine if orphan bindings cleanup needs to run", "namespace", cattleNamespace, "configmap", bootstrapAdminConfig)
	adminConfig, err := wContext.K8s.CoreV1().ConfigMaps(cattleNamespace).Get(scaledContext.RunContext, bootstrapAdminConfig, v1.GetOptions{})
	if err != nil {
		log.Warn("Unable to determine if bindings cleanup has ran, skipping", "cleanupKey", cleanupKey, "error", err)
		return err
	}

	// config map exists, check if the cleanup key is found
	if _, ok := adminConfig.Data[cleanupKey]; ok {
		log.Info("Orphan bindings cleanup has already run, skipping", "cleanupKey", cleanupKey)
		return nil
	}

	// run cleanup
	if cleanupKey == orphanBindingsCleanupKey {
		err = clean.OrphanBindings(&scaledContext.RESTConfig)
	}
	if err != nil {
		log.Warn("Error during orphan binding cleanup", "cleanupKey", cleanupKey, "error", err)
		return err
	}

	// update configmap
	reloadedConfig, err := wContext.K8s.CoreV1().ConfigMaps(cattleNamespace).Get(scaledContext.RunContext, bootstrapAdminConfig, v1.GetOptions{})
	if err != nil {
		log.Warn("Unable to get configmap", "cleanupKey", cleanupKey, "configmap", bootstrapAdminConfig, "error", err)
		return err
	}

	adminConfigCopy := reloadedConfig.DeepCopy()
	if adminConfigCopy.Data == nil {
		adminConfigCopy.Data = make(map[string]string)
	}
	adminConfigCopy.Data[cleanupKey] = "yes"

	_, err = wContext.K8s.CoreV1().ConfigMaps(cattleNamespace).Update(scaledContext.RunContext, adminConfigCopy, v1.UpdateOptions{})
	if err != nil {
		log.Warn("Error while updating configmap, unable to record completion of orphan binding cleanup", "cleanupKey", cleanupKey, "error", err, "configmap", bootstrapAdminConfig)
	}

	log.Info("Successfully cleaned up orphan bindings", "cleanupKey", cleanupKey)
	return nil
}
