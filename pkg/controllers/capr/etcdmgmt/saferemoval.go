package etcdmgmt

import (
	"context"

	"github.com/rancher/rancher/pkg/log"
	apierror "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"
	"k8s.io/client-go/util/retry"
)

func SafelyRemoved(restConfig *rest.Config, runtime, nodeName string) (bool, error) {
	removeAnnotation := "etcd." + runtime + ".cattle.io/remove"
	removedNodeNameAnnotation := "etcd." + runtime + ".cattle.io/removed-node-name"

	clientset, err := kubernetes.NewForConfig(restConfig)
	if err != nil {
		return false, err
	}

	log.Debug("Retrieving node from k8s", "operation", "safely_removed", "node", nodeName)

	node, err := clientset.CoreV1().Nodes().Get(context.TODO(), nodeName, metav1.GetOptions{})
	if err != nil {
		if apierror.IsNotFound(err) {
			log.Debug("Node not found, proceeding with deletion", "operation", "safely_removed", "node", nodeName)
			return true, nil
		}
		return false, err
	}

	if node.Annotations[removeAnnotation] == "true" {
		// check val to see if it's true, if not, continue
		// check the status of the removal
		log.Debug("Etcd member removal in progress", "operation", "safely_removed", "annotation", removeAnnotation)
		return node.Annotations[removedNodeNameAnnotation] != "", nil
	}
	// The remove annotation has not been set to true, so we'll go ahead and set it on the node.
	return false, retry.RetryOnConflict(retry.DefaultRetry,
		func() error {
			node, err = clientset.CoreV1().Nodes().Get(context.TODO(), nodeName, metav1.GetOptions{})
			if err != nil {
				return err
			}
			node.Annotations[removeAnnotation] = "true"
			_, err = clientset.CoreV1().Nodes().Update(context.TODO(), node, metav1.UpdateOptions{})
			return err
		})
}
