package wrangler

import (
	"context"
	"fmt"
	"sync/atomic"

	ext "github.com/rancher/rancher/pkg/generated/controllers/ext.cattle.io"
	extv1 "github.com/rancher/rancher/pkg/generated/controllers/ext.cattle.io/v1"
	"github.com/rancher/rancher/pkg/log"
	wapiregv1 "github.com/rancher/wrangler/v3/pkg/generated/controllers/apiregistration.k8s.io/v1"
	"github.com/rancher/wrangler/v3/pkg/generic"
	"k8s.io/apimachinery/pkg/api/errors"
	apiregv1 "k8s.io/kube-aggregator/pkg/apis/apiregistration/v1"
)

// EXTAPIContext is a scoped context which wraps the larger Wrangler context.
// It includes EXT clients and factories which are initialized after the EXT
// api-service is detected and established
type EXTAPIContext struct {
	*Context
	Client  extv1.Interface
	factory *ext.Factory
}

// DeferredEXTAPIInitializer implements the DeferredInitializer interface and
// monitors api-services until the expected EXT api-service resource is created
// and established.
type DeferredEXTAPIInitializer struct {
	context *Context
}

func NewEXTAPIInitializer(client *Context) *DeferredEXTAPIInitializer {
	return &DeferredEXTAPIInitializer{
		context: client,
	}
}

// WaitForClient creates and returns an initialized ext api context. It spawns
// an internal waiter for the availability of the EXT api-service. On success it
// initializes and returns the context, enabling the controlling manager to run
// the associated deferred functions.
func (d *DeferredEXTAPIInitializer) WaitForClient(ctx context.Context) (*EXTAPIContext, error) {
	var done atomic.Bool
	ready := make(chan struct{})

	log.Info("Starting waiter for EXT api-service availability", "operation", "deferred_extapi_wait_for_client")

	d.context.API.APIService().OnChange(ctx, "extapi-deferred-registration", func(key string, api *apiregv1.APIService) (*apiregv1.APIService, error) {
		if done.Load() {
			return api, nil
		}

		if !extReady(d.context.API.APIService().Cache()) {
			return api, nil
		}

		if !done.CompareAndSwap(false, true) {
			return api, nil
		}

		close(ready)
		return api, nil
	})

	select {
	case <-ready:
	case <-ctx.Done():
		return nil, ctx.Err()
	}

	log.Debug("Creating ext factory", "operation", "deferred_extapi_wait_for_client")

	ext, err := ext.NewFactoryFromConfigWithOptions(d.context.RESTConfig, &generic.FactoryOptions{
		SharedControllerFactory: d.context.ControllerFactory,
	})
	if err != nil {
		return nil, fmt.Errorf("Unexpected error while creating ext factory: %w", err)
	}

	return &EXTAPIContext{
		Context: d.context,
		factory: ext,
		Client:  ext.Ext().V1(),
	}, nil
}

// extReady checks that all required api services are available and established
func extReady(apiServiceCache wapiregv1.APIServiceCache) bool {
	requiredAPIServices := []string{
		"v1.ext.cattle.io",
	}

	log.Debug("Checking EXT api-service availability and establishment status", "operation", "ext_ready")

	for _, apiServiceName := range requiredAPIServices {
		apiService, err := apiServiceCache.Get(apiServiceName)
		if err != nil {
			if errors.IsNotFound(err) {
				log.Debug("Api-service not found, continuing to wait", "operation", "ext_ready", "api_service", apiServiceName)
				return false
			}
			log.Debug("Api-service error during check", "operation", "ext_ready", "api_service", apiServiceName, "error", err)
			return false
		}

		established := false
		for _, condition := range apiService.Status.Conditions {
			if condition.Type == apiregv1.Available && condition.Status == apiregv1.ConditionTrue {
				established = true
				break
			}
		}

		if !established {
			log.Debug("Api-service exists but not yet established, continuing to wait", "operation", "ext_ready", "api_service", apiServiceName)
			return false
		}

		log.Debug("Api-service is available and established", "operation", "ext_ready", "api_service", apiServiceName)
	}

	return true
}
