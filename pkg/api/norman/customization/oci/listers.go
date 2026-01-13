package oci

import (
	"context"
	"encoding/json"
	"net/http"
	"strings"

	"github.com/oracle/oci-go-sdk/common"
	"github.com/oracle/oci-go-sdk/containerengine"
	"github.com/oracle/oci-go-sdk/core"
	"github.com/oracle/oci-go-sdk/identity"
	"github.com/rancher/norman/httperror"
	"github.com/rancher/rancher/pkg/log"
)

func processVcns(provider common.ConfigurationProvider, compartment string) ([]byte, int, error) {
	log.Debug("Oci-handler: listing vcns in compartment", "operation", "process_vcns", "compartment", compartment)
	virtualNetworkClient, err := core.NewVirtualNetworkClientWithConfigurationProvider(provider)
	if err != nil {
		log.Debug("Oci-handler: error creating virtual network client", "operation", "process_vcns", "error", err)
		return nil, httperror.ServerError.Status, err
	}
	vcnRequest := core.ListVcnsRequest{
		CompartmentId: &compartment,
	}
	vcnResponse, err := virtualNetworkClient.ListVcns(context.Background(), vcnRequest)
	if err != nil {
		httpErr := httperror.ErrorCode{}
		if vcnResponse.RawResponse != nil {
			httpErr.Status = vcnResponse.RawResponse.StatusCode
		} else {
			httpErr.Status = httperror.ServerError.Status
		}
		log.Debug("Oci-handler: error listing vcns with virtual network client", "operation", "process_vcns", "error", err)
		return nil, httpErr.Status, err
	}

	var vcnDisplayNames []string
	for _, item := range vcnResponse.Items {
		vcnDisplayNames = append(vcnDisplayNames, *(item.DisplayName))
	}

	data, err := json.Marshal(vcnDisplayNames)
	if err != nil {
		return data, httperror.ServerError.Status, err
	}

	return data, http.StatusOK, err
}

func processOkeVersions(provider common.ConfigurationProvider) ([]byte, int, error) {
	log.Debug("Oci-handler: listing oke versions", "operation", "process_oke_versions")
	containerClient, err := containerengine.NewContainerEngineClientWithConfigurationProvider(provider)
	if err != nil {
		log.Debug("Oci-handler: error creating containerengine client", "operation", "process_oke_versions", "error", err)
		return nil, httperror.ServerError.Status, err
	}
	getClusterOptionsReq := containerengine.GetClusterOptionsRequest{
		ClusterOptionId: common.String("all"),
	}
	getClusterOptionsResp, err := containerClient.GetClusterOptions(context.Background(), getClusterOptionsReq)
	if err != nil {
		httpErr := httperror.ErrorCode{}
		if getClusterOptionsResp.RawResponse != nil {
			httpErr.Status = getClusterOptionsResp.RawResponse.StatusCode
		} else {
			httpErr.Status = httperror.ServerError.Status
		}
		log.Debug("Oci-handler: error getting cluster options with containerengine client", "operation", "process_oke_versions", "error", err)
		return nil, httpErr.Status, err
	}

	data, err := json.Marshal(getClusterOptionsResp.KubernetesVersions)
	if err != nil {
		return data, httperror.ServerError.Status, err
	}

	return data, http.StatusOK, err
}

func processAvailabilityDomains(provider common.ConfigurationProvider, compartment string) ([]byte, int, error) {
	log.Debug("Oci-handler: listing availability domains in compartment", "operation", "process_availability_domains", "compartment", compartment)
	identityClient, err := identity.NewIdentityClientWithConfigurationProvider(provider)
	if err != nil {
		log.Debug("Oci-handler: error creating identity client", "operation", "process_availability_domains", "error", err)
		return nil, httperror.ServerError.Status, err
	}
	request := identity.ListAvailabilityDomainsRequest{
		CompartmentId: &compartment,
	}
	availabilityDomains, err := identityClient.ListAvailabilityDomains(context.Background(), request)
	if err != nil {
		log.Debug("Oci-handler: error listing availability domains with identity client", "operation", "process_availability_domains", "error", err)
		return nil, getErrorCode(availabilityDomains.RawResponse), err
	}

	var adNames []string
	for _, item := range availabilityDomains.Items {
		adNames = append(adNames, *(item.Name))
	}

	data, err := json.Marshal(adNames)
	if err != nil {
		return data, httperror.ServerError.Status, err
	}

	return data, http.StatusOK, err
}

func processRegions(provider common.ConfigurationProvider, tenancy string) ([]byte, int, error) {
	log.Debug("Oci-handler: listing regions in tenancy", "operation", "process_regions", "tenancy", tenancy)
	identityClient, err := identity.NewIdentityClientWithConfigurationProvider(provider)
	if err != nil {
		log.Debug("Oci-handler: error creating identity client", "operation", "process_regions", "error", err)
		return nil, httperror.ServerError.Status, err
	}
	request := identity.ListRegionSubscriptionsRequest{
		TenancyId: &tenancy,
	}
	allRegions, err := identityClient.ListRegionSubscriptions(context.Background(), request)
	if err != nil {
		log.Debug("Oci-handler: error listing regions with identity client", "operation", "process_regions", "error", err)
		return nil, getErrorCode(allRegions.RawResponse), err
	}

	var regionNames []string
	for _, item := range allRegions.Items {
		regionNames = append(regionNames, *(item.RegionName))
	}

	data, err := json.Marshal(regionNames)
	if err != nil {
		return data, httperror.ServerError.Status, err
	}

	return data, http.StatusOK, err
}

func processNodeShapes(provider common.ConfigurationProvider, compartment string) ([]byte, int, error) {
	log.Debug("Oci-handler: listing shapes in compartment", "operation", "process_node_shapes", "compartment", compartment)
	computeClient, err := core.NewComputeClientWithConfigurationProvider(provider)
	if err != nil {
		log.Debug("Oci-handler: error creating compute client", "operation", "process_node_shapes", "error", err)
		return nil, httperror.ServerError.Status, err
	}
	shapeRequest := core.ListShapesRequest{
		CompartmentId: &compartment,
	}
	shapeResponse, err := computeClient.ListShapes(context.Background(), shapeRequest)
	if err != nil {
		log.Debug("Oci-handler: error listing shapes with compute client", "operation", "process_node_shapes", "error", err)
		return nil, getErrorCode(shapeResponse.RawResponse), err
	}

	var nodeShapes []string
	for _, item := range shapeResponse.Items {
		if !listContains(nodeShapes, *(item.Shape)) {
			nodeShapes = append(nodeShapes, *(item.Shape))
		}
	}

	data, err := json.Marshal(nodeShapes)
	if err != nil {
		return data, httperror.ServerError.Status, err
	}

	return data, http.StatusOK, err
}

func processImages(provider common.ConfigurationProvider, compartment string) ([]byte, int, error) {
	log.Debug("Oci-handler: listing images in compartment", "operation", "process_images", "compartment", compartment)
	computeClient, err := core.NewComputeClientWithConfigurationProvider(provider)
	if err != nil {
		log.Debug("Oci-handler: error creating compute client", "operation", "process_images", "error", err)
		return nil, httperror.ServerError.Status, err
	}
	imageRequest := core.ListImagesRequest{
		CompartmentId: &compartment,
	}
	shapeResponse, err := computeClient.ListImages(context.Background(), imageRequest)
	if err != nil {
		log.Debug("Oci-handler: error listing images with compute client", "operation", "process_images", "error", err)
		return nil, getErrorCode(shapeResponse.RawResponse), err
	}

	var nodeImages []string
	for _, item := range shapeResponse.Items {
		if !strings.Contains(*item.DisplayName, "GPU") &&
			!strings.Contains(*item.DisplayName, "Oracle-Linux-6") &&
			strings.Contains(*item.DisplayName, "Oracle-Linux") &&
			!listContains(nodeImages, *(item.DisplayName)) {
			nodeImages = append(nodeImages, *(item.DisplayName))
		}
	}

	data, err := json.Marshal(nodeImages)
	if err != nil {
		return data, httperror.ServerError.Status, err
	}

	return data, http.StatusOK, err
}

func processNodeOkeImages(provider common.ConfigurationProvider) ([]byte, int, error) {
	log.Debug("Oci-handler: listing node oke images", "operation", "process_node_oke_images")
	containerClient, err := containerengine.NewContainerEngineClientWithConfigurationProvider(provider)
	if err != nil {
		log.Debug("Oci-handler: error creating containerengine client", "operation", "process_node_oke_images", "error", err)
		return nil, httperror.ServerError.Status, err
	}
	nodePoolOptionsReq := containerengine.GetNodePoolOptionsRequest{
		NodePoolOptionId: common.String("all"),
	}
	nodePoolOptionsResp, err := containerClient.GetNodePoolOptions(context.Background(), nodePoolOptionsReq)
	if err != nil {
		log.Debug("Oci-handler: error getting node pool options with containerengine client", "operation", "process_node_oke_images", "error", err)
		return nil, getErrorCode(nodePoolOptionsResp.RawResponse), err
	}

	var nodeSources []string
	for _, item := range nodePoolOptionsResp.Sources {

		sourceName := *(item.GetSourceName())

		if !listContains(nodeSources, sourceName) {
			nodeSources = append(nodeSources, sourceName)
		}
	}

	data, err := json.Marshal(nodeSources)
	if err != nil {
		return data, httperror.ServerError.Status, err
	}

	return data, http.StatusOK, err
}

func listContains(list []string, entry string) bool {
	for _, x := range list {
		if x == entry {
			return true
		}
	}
	return false
}

func getErrorCode(response *http.Response) int {
	if response == nil {
		return httperror.ServerError.Status
	}
	return response.StatusCode
}
