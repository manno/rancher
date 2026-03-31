package management

import (
	"fmt"

	"github.com/rancher/rancher/pkg/log"
	"github.com/rancher/rancher/pkg/namespace"
	"github.com/rancher/rancher/pkg/settings"
	"github.com/rancher/rancher/pkg/types/config"

	corev1 "k8s.io/api/core/v1"
	k8serrors "k8s.io/apimachinery/pkg/api/errors"
)

func addCattleGlobalNamespaces(management *config.ManagementContext) error {
	if err := createNamespace(namespace.GlobalNamespace, management); err != nil {
		return err
	}

	log.Debug("Calling sync for driver metadata", "operation", "add_namespaces")
	management.Management.Settings("").Controller().Enqueue("", settings.RkeMetadataConfig.Name)

	return nil
}

func createNamespace(namespace string, management *config.ManagementContext) error {
	lister := management.Core.Namespaces("").Controller().Lister()

	_, err := lister.Get("", namespace)
	if k8serrors.IsNotFound(err) {
		ns := &corev1.Namespace{}
		ns.Name = namespace
		if _, err := management.Core.Namespaces("").Create(ns); err != nil {
			return fmt.Errorf("error creating %v namespace: %v", namespace, err)
		}
		log.Info("Created namespace", "operation", "add_namespaces", "namespace", namespace)
	} else if err != nil {
		return fmt.Errorf("error getting %v namespace: %v", namespace, err)
	}

	return nil
}
