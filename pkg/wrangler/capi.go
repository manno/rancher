package wrangler

import (
	"context"
	"sync/atomic"

	v1 "github.com/rancher/rancher/pkg/apis/catalog.cattle.io/v1"
	"github.com/rancher/rancher/pkg/controllers/dashboard/chart"
	"github.com/rancher/rancher/pkg/features"
	capi "github.com/rancher/rancher/pkg/generated/controllers/cluster.x-k8s.io"
	capicontrollers "github.com/rancher/rancher/pkg/generated/controllers/cluster.x-k8s.io/v1beta2"
	log "github.com/rancher/rancher/pkg/log"
	"github.com/rancher/rancher/pkg/namespace"
	"github.com/rancher/rancher/pkg/settings"
	wapiextv1 "github.com/rancher/wrangler/v3/pkg/generated/controllers/apiextensions.k8s.io/v1"
	"github.com/rancher/wrangler/v3/pkg/generic"
	apiextv1 "k8s.io/apiextensions-apiserver/pkg/apis/apiextensions/v1"
	k8serr "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

// CAPIContext is a scoped context which wraps the larger Wrangler context.
// It includes CAPI clients and factories which are initialized after CAPI
// CRDs are detected.
type CAPIContext struct {
	*Context
	CAPI        capicontrollers.Interface
	CAPIFactory *capi.Factory
	Client      client.Client
}

// DeferredCAPIInitializer implements the DeferredInitializer interface
// and monitors CRDs until all expected CAPI resources have been created.
type DeferredCAPIInitializer struct {
	context *Context
}

func NewCAPIInitializer(clients *Context) *DeferredCAPIInitializer {
	return &DeferredCAPIInitializer{
		context: clients,
	}
}

func (d *DeferredCAPIInitializer) WaitForClient(ctx context.Context) (*CAPIContext, error) {
	var done atomic.Bool
	ready := make(chan struct{})
	log.Info("Waiting for CAPI CRDs to be established", "operation", "deferred_capi_wait_for_client")
	d.context.CRD.CustomResourceDefinition().OnChange(ctx, "capi-deferred-registration", func(key string, crd *apiextv1.CustomResourceDefinition) (*apiextv1.CustomResourceDefinition, error) {
		if done.Load() {
			return crd, nil
		}

		if !capiCRDsReady(d.context.CRD.CustomResourceDefinition().Cache()) {
			return crd, nil
		}

		if !done.CompareAndSwap(false, true) {
			return crd, nil
		}
		close(ready)
		return crd, nil
	})

	select {
	case <-ready:
	case <-ctx.Done():
		return nil, ctx.Err()
	}

	log.Info("waiting for CAPI catalog App to be ready with correct version", "operation", "deferred_capi_wait_for_client")

	if err := waitForCAPIAppVersion(ctx, d.context); err != nil {
		return nil, err
	}

	log.Info("CAPI catalog App version is correct, initializing CAPI factory", "operation", "deferred_capi_wait_for_client")


	opts := &generic.FactoryOptions{
		SharedControllerFactory: d.context.ControllerFactory,
	}

	capiFactory, err := capi.NewFactoryFromConfigWithOptions(d.context.RESTConfig, opts)
	if err != nil {
		log.Fatal("Encountered unexpected error while creating capi factory", "operation", "deferred_capi_wait_for_client", "error", err)
	}

	// Create controller-runtime client for CAPI operations using the wrangler Scheme.
	// This ensures the client can properly decode all CAPI and related resource types.
	//
	// Note: This client is not cached because controller-runtime's cache system is separate
	// from wrangler/lasso's SharedControllerFactory cache. While we could create a separate
	// controller-runtime cache, that would duplicate the existing wrangler cache infrastructure.
	// This client is primarily used by external.GetObjectFromContractVersionedRef() from the
	// CAPI library, which requires a controller-runtime client interface.
	//
	// The CAPI factory (created above) uses the wrangler SharedControllerFactory cache for
	// most operations, so API calls from this client are limited to the specific CAPI
	// contract reference lookups.
	c, err := client.New(d.context.RESTConfig, client.Options{
		Scheme: Scheme,
	})
	if err != nil {
		log.Fatal("Encountered unexpected error while creating controller-runtime client", "error", err)
	}

	return &CAPIContext{
		Context:     d.context,
		CAPIFactory: capiFactory,
		CAPI:        capiFactory.Cluster().V1beta2(),
		Client:      c,
	}, nil
}

func capiCRDsReady(crdCache wapiextv1.CustomResourceDefinitionCache) bool {
	requiredCRDs := []string{
		"clusters.cluster.x-k8s.io",
		"machines.cluster.x-k8s.io",
		"machinesets.cluster.x-k8s.io",
		"machinedeployments.cluster.x-k8s.io",
		"machinehealthchecks.cluster.x-k8s.io",
	}

	log.Trace("Checking CAPI CRDs availability and establishment status", "operation", "capi_crds_ready")
	allCRDsReady := true
	for _, crdName := range requiredCRDs {
		crd, err := crdCache.Get(crdName)
		if err != nil {
			if k8serr.IsNotFound(err) {
				log.Trace("CAPI CRD not found, continuing to wait", "operation", "capi_crds_ready", "crd", crdName)
				allCRDsReady = false
				break
			}
			log.Error("Error checking for CAPI CRD", "operation", "capi_crds_ready", "crd", crdName, "error", err)
			allCRDsReady = false
			break
		}

		established := false
		for _, condition := range crd.Status.Conditions {
			if condition.Type == "Established" && condition.Status == "True" {
				established = true
				break
			}
		}

		if !established {
			log.Trace("CAPI CRD exists but not yet established, continuing to wait", "operation", "capi_crds_ready", "crd", crdName)
			allCRDsReady = false
			break
		}

		log.Trace("CAPI CRD is available and established", "operation", "capi_crds_ready", "crd", crdName)
	}

	return allCRDsReady
}

// waitForCAPIAppVersion waits for the catalog App (rancher-provisioning-capi or rancher-turtles)
// to be running with the correct version specified in settings.
// It also enqueues the rancher-charts ClusterRepo on version mismatches or errors to trigger a refresh.
func waitForCAPIAppVersion(ctx context.Context, wContext *Context) error {
	log.Info("Checking CAPI catalog App version", "operation", "deferred_capi_wait_for_app_version")

	var (
		name    string
		ns      string
		version string
	)

	if features.Turtles.Enabled() {
		name = chart.TurtlesChartName
		ns = namespace.TurtlesNamespace
		version = settings.RancherTurtlesVersion.Get()
	}

	if name == "" || ns == "" || version == "" {
		log.Debug("Turtles feature is disabled, skipping CAPI App version wait", "operation", "deferred_capi_wait_for_app_version")
		return nil
	}

	check := func() bool {
		app, err := wContext.Catalog.App().Get(ns, name, metav1.GetOptions{})
		if err != nil {
			if k8serr.IsNotFound(err) {
				wContext.Catalog.ClusterRepo().Enqueue("rancher-charts")
				log.Trace("CAPI App not found, continuing to wait", "operation", "deferred_capi_wait_for_app_version", "namespace", ns, "name", name)
			} else {
				log.Warn("Error getting CAPI App", "operation", "deferred_capi_wait_for_app_version", "namespace", ns, "name", name, "error", err)
			}
			return false
		}

		if app.Spec.Chart == nil || app.Spec.Chart.Metadata == nil {
			wContext.Catalog.ClusterRepo().Enqueue("rancher-charts")
			log.Trace("CAPI App has no chart metadata, continuing to wait", "operation", "deferred_capi_wait_for_app_version", "namespace", ns, "name", name)
			return false
		}

		currentVersion := app.Spec.Chart.Metadata.Version
		if currentVersion != version {
			wContext.Catalog.ClusterRepo().Enqueue("rancher-charts")
			log.Trace("CAPI App version mismatch, continuing to wait", "operation", "deferred_capi_wait_for_app_version", "namespace", ns, "name", name, "current", currentVersion, "expected", version)
			return false
		}

		if app.Status.Summary.State != string(v1.StatusDeployed) {
			log.Trace("CAPI App not yet deployed, continuing to wait", "operation", "deferred_capi_wait_for_app_version", "namespace", ns, "name", name, "state", app.Status.Summary.State)
			return false
		}

		return true
	}

	// Initial check before setting up the watch
	if check() {
		return nil
	}

	ready := make(chan struct{})
	var done atomic.Bool

	wContext.Catalog.App().OnChange(ctx, "deferred-capi-app-version", func(_ string, app *v1.App) (*v1.App, error) {
		if done.Load() {
			return app, nil
		}
		if check() && done.CompareAndSwap(false, true) {
			close(ready)
		}
		return app, nil
	})

	select {
	case <-ready:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}
