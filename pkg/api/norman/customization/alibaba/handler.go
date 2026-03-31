package alibaba

import (
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"

	cs "github.com/rancher/muchang/cs/client"
	openapi "github.com/rancher/muchang/darabonba-openapi/client"
	ecs "github.com/rancher/muchang/ecs/client"
	resourcemanager "github.com/rancher/muchang/resourcemanager/client"
	vpc "github.com/rancher/muchang/vpc/client"
	client "github.com/rancher/rancher/pkg/client/generated/management/v3"
	schema "github.com/rancher/rancher/pkg/schemas/management.cattle.io/v3"

	"github.com/gorilla/mux"
	credential "github.com/rancher/muchang/credentials"
	"github.com/rancher/muchang/utils/tea"
	"github.com/rancher/muchang/utils/tea/dara"
	"github.com/rancher/norman/api/access"
	"github.com/rancher/norman/httperror"
	"github.com/rancher/norman/types"
	"github.com/rancher/rancher/pkg/auth/util"
	v1 "github.com/rancher/rancher/pkg/generated/norman/core/v1"
	"github.com/rancher/rancher/pkg/log"
	"github.com/rancher/rancher/pkg/namespace"
	"github.com/rancher/rancher/pkg/ref"
	"github.com/rancher/rancher/pkg/types/config"
	corev1 "k8s.io/api/core/v1"
)

const (
	credentialsType = "access_key"
	defaultRegion   = "us-east-1"
	defaultProtocol = "https"
)

type Capabilities struct {
	AccessKeyID     string `json:"accessKeyId"`
	AccessKeySecret string `json:"accessKeySecret"`
	RegionID        string `json:"regionId"`
	AcceptLanguage  string `json:"acceptLanguage"`
}

type handler struct {
	schemas       *types.Schemas
	secretsLister v1.SecretLister
}

func NewAlibabaHandler(scaledContext *config.ScaledContext) http.Handler {
	return &handler{
		schemas:       scaledContext.Schemas,
		secretsLister: scaledContext.Core.Secrets(namespace.GlobalNamespace).Controller().Lister(),
	}
}

func (h *handler) ServeHTTP(writer http.ResponseWriter, req *http.Request) {
	writer.Header().Set("Content-Type", "application/json")

	resourceType := mux.Vars(req)["resource"]

	if resourceType == "alibabaCheckCredentials" {
		if req.Method != http.MethodPost {
			handleErr(writer, http.StatusMethodNotAllowed, fmt.Errorf("use POST for this endpoint"))
			return
		}
		if errCode, err := h.checkCredentials(req); err != nil {
			handleErr(writer, errCode, err)
			return
		}
		return
	}

	capabilities := &Capabilities{}

	if credID := req.URL.Query().Get("cloudCredentialId"); credID != "" {
		cc, statusCode, err := h.getCloudCredential(req, credID)
		if err != nil {
			log.Debug("Alibaba-handler: error accessing cloud credential", "operation", "handle", "cred_id", credID, "error", err)
			handleErr(writer, statusCode, fmt.Errorf("error accessing cloud credential %s", credID))
			return
		}
		capabilities.AccessKeyID = string(cc.Data["alibabacredentialConfig-accessKeyId"])
		capabilities.AccessKeySecret = string(cc.Data["alibabacredentialConfig-accessKeySecret"])
	} else {
		handleErr(writer, http.StatusBadRequest, fmt.Errorf("cloud credential ID not found"))
		return
	}

	var (
		serialized []byte
		errCode    int
		err        error
	)

	regionID := req.URL.Query().Get("regionId")
	// regionID required for all other operations except alibabaRegions
	if regionID == "" && resourceType != "alibabaRegions" {
		util.ReturnHTTPError(writer, req, http.StatusBadRequest, "regionId not set")
		return
	}
	capabilities.RegionID = regionID

	switch resourceType {
	case "alibabaRegions":
		if serialized, errCode, err = describeRegions(capabilities, req); err != nil {
			log.Debug("Alibaba-handler: error call describeRegions", "operation", "handle", "error", err)
			util.ReturnHTTPError(writer, req, errCode, err.Error())
			return
		}
		writer.Write(serialized)
	case "alibabaZones":
		if serialized, errCode, err = describeZones(capabilities, req); err != nil {
			log.Debug("Alibaba-handler: error call describeZones", "operation", "handle", "error", err)
			util.ReturnHTTPError(writer, req, errCode, err.Error())
			return
		}
		writer.Write(serialized)
	case "alibabaClusters":
		if serialized, errCode, err = describeClusters(capabilities, req); err != nil {
			log.Debug("Alibaba-handler: error call describeClusters", "operation", "handle", "error", err)
			util.ReturnHTTPError(writer, req, errCode, err.Error())
			return
		}
		writer.Write(serialized)
	case "alibabaResourceGroups":
		if serialized, errCode, err = describeResourceGroups(capabilities, req); err != nil {
			log.Debug("Alibaba-handler: error call describeResourceGroups", "operation", "handle", "error", err)
			util.ReturnHTTPError(writer, req, errCode, err.Error())
			return
		}
		writer.Write(serialized)
	case "alibabaInstanceTypes":
		if serialized, errCode, err = describeInstanceTypes(capabilities, req); err != nil {
			log.Debug("Alibaba-handler: error call describeInstanceTypes", "operation", "handle", "error", err)
			util.ReturnHTTPError(writer, req, errCode, err.Error())
			return
		}
		writer.Write(serialized)
	case "alibabaKeyPairs":
		if serialized, errCode, err = describeKeyPairs(capabilities, req); err != nil {
			log.Debug("Alibaba-handler: error call describeKeyPairs", "operation", "handle", "error", err)
			util.ReturnHTTPError(writer, req, errCode, err.Error())
			return
		}
		writer.Write(serialized)
	case "alibabaVpcs":
		if serialized, errCode, err = describeVpcs(capabilities, req); err != nil {
			log.Debug("Alibaba-handler: error call describeVpcs", "operation", "handle", "error", err)
			util.ReturnHTTPError(writer, req, errCode, err.Error())
			return
		}
		writer.Write(serialized)
	case "alibabaVSwitches":
		if serialized, errCode, err = describeVSwitches(capabilities, req); err != nil {
			log.Debug("Alibaba-handler: error call describeVSwitches", "operation", "handle", "error", err)
			util.ReturnHTTPError(writer, req, errCode, err.Error())
			return
		}
		writer.Write(serialized)
	case "alibabaAvailableResources":
		if serialized, errCode, err = describeAvailableResource(capabilities, req); err != nil {
			log.Debug("Alibaba-handler: error call describeAvailableResource", "operation", "handle", "error", err)
			util.ReturnHTTPError(writer, req, errCode, err.Error())
			return
		}
		writer.Write(serialized)
	case "alibabaKubernetesVersions":
		if serialized, errCode, err = describeKubernetesMetadata(capabilities, req); err != nil {
			log.Debug("Alibaba-handler: error call describeKubernetesMetadata", "operation", "handle", "error", err)
			util.ReturnHTTPError(writer, req, errCode, err.Error())
			return
		}
		writer.Write(serialized)
	case "alibabaImageSupportedInstanceTypes":
		if serialized, errCode, err = describeImageSupportedInstanceTypes(capabilities, req); err != nil {
			log.Debug("Alibaba-handler: error call describeImageSupportedInstanceTypes", "operation", "handle", "error", err)
			util.ReturnHTTPError(writer, req, errCode, err.Error())
			return
		}
		writer.Write(serialized)
	default:
		handleErr(writer, httperror.NotFound.Status, fmt.Errorf("invalid endpoint %v", resourceType))
	}
}

func (h *handler) checkCredentials(req *http.Request) (int, error) {
	cred := &Capabilities{}
	raw, err := io.ReadAll(req.Body)
	if err != nil {
		return http.StatusBadRequest, fmt.Errorf("cannot read request body: %v", err)
	}

	if err = json.Unmarshal(raw, &cred); err != nil {
		return http.StatusBadRequest, fmt.Errorf("cannot parse request body: %v", err)
	}

	if cred.RegionID == "" {
		cred.RegionID = defaultRegion
	}
	if cred.AccessKeyID == "" {
		return http.StatusBadRequest, fmt.Errorf("must provide access key ID")
	}
	if cred.AccessKeySecret == "" {
		return http.StatusBadRequest, fmt.Errorf("must provide access key secret")
	}

	client, err := CreateECSClient(cred.AccessKeyID, cred.AccessKeySecret, cred.RegionID)
	if err != nil {
		return http.StatusInternalServerError, err
	}

	request := &ecs.DescribeRegionsRequest{}
	if cred.AcceptLanguage != "" {
		request.AcceptLanguage = &cred.AcceptLanguage
	}

	_, err = client.DescribeRegionsWithContext(req.Context(), request, &dara.RuntimeOptions{})
	if err != nil {

		log.Debug("Alibaba-handler: error call describeRegions", "operation", "handle", "error", err)
		return handleSDKError(err)
	}

	return http.StatusOK, nil
}

func CreateCSClient(ak, sk, regionID string) (*cs.Client, error) {
	if regionID == "" {
		return nil, errors.New("regionId can not be empty")
	}

	credentials, err := getCredentials(ak, sk)
	if err != nil {
		return nil, err
	}

	protocol := defaultProtocol
	openAPICfg := &openapi.Config{
		Credential: credentials,
		Protocol:   &protocol,
	}

	openAPICfg.Endpoint = tea.String("cs." + regionID + ".aliyuncs.com")
	return cs.NewClient(openAPICfg)
}

func CreateECSClient(ak, sk, regionID string) (*ecs.Client, error) {
	if regionID == "" {
		return nil, errors.New("regionId can not be empty")
	}

	credentials, err := getCredentials(ak, sk)
	if err != nil {
		return nil, err
	}

	protocol := defaultProtocol
	openAPICfg := &openapi.Config{
		Credential: credentials,
		Protocol:   &protocol,
	}

	openAPICfg.Endpoint = tea.String("ecs." + regionID + ".aliyuncs.com")
	return ecs.NewClient(openAPICfg)
}

func CreateResourceManagerClient(ak, sk, regionID string) (*resourcemanager.Client, error) {
	if regionID == "" {
		return nil, errors.New("regionId can not be empty")
	}

	credentials, err := getCredentials(ak, sk)
	if err != nil {
		return nil, err
	}

	protocol := defaultProtocol
	openAPICfg := &openapi.Config{
		Credential: credentials,
		Protocol:   &protocol,
	}

	openAPICfg.Endpoint = tea.String("resourcemanager." + regionID + ".aliyuncs.com")
	return resourcemanager.NewClient(openAPICfg)
}

func CreateVpcClient(ak, sk, regionID string) (*vpc.Client, error) {
	if regionID == "" {
		return nil, errors.New("regionId can not be empty")
	}

	credentials, err := getCredentials(ak, sk)
	if err != nil {
		return nil, err
	}

	protocol := defaultProtocol
	openAPICfg := &openapi.Config{
		Credential: credentials,
		Protocol:   &protocol,
	}

	openAPICfg.Endpoint = tea.String("vpc." + regionID + ".aliyuncs.com")
	return vpc.NewClient(openAPICfg)
}

func getCredentials(ak, sk string) (credential.Credential, error) {
	configCredsType := credentialsType

	config := &credential.Config{
		AccessKeyId:     &ak,
		AccessKeySecret: &sk,
		Type:            &configCredsType,
	}

	return credential.NewCredential(config)
}

func handleErr(writer http.ResponseWriter, errorCode int, originalErr error) {
	writer.WriteHeader(errorCode)

	payload := make(map[string]string)
	payload["error"] = originalErr.Error()
	payloadJSON, err := json.Marshal(payload)
	if err != nil { // This should not happen given fixed types on the payload - https://stackoverflow.com/a/33964549
		log.Error("Alibaba-handler: failed to write payload JSON", "operation", "encode_payload", "error", err)
		return
	}
	writer.Write(payloadJSON)
}

func (h *handler) generateAPIContext(req *http.Request) *types.APIContext {
	return &types.APIContext{
		Method:  req.Method,
		Request: req,
		Schemas: h.schemas,
		Query:   map[string][]string{},
	}
}

func (h *handler) getCloudCredential(req *http.Request, credID string) (*corev1.Secret, int, error) {
	ns, name := ref.Parse(credID)
	if ns == "" || name == "" {
		log.Error("Alibaba-handler: invalid cloud credential ID", "operation", "get_credentials", "cred_id", credID)
		return nil, http.StatusBadRequest, fmt.Errorf("invalid cloud credential ID %s", credID)
	}

	var accessCred client.CloudCredential //var to check access
	if err := access.ByID(h.generateAPIContext(req), &schema.Version, client.CloudCredentialType, credID, &accessCred); err != nil {
		apiError, ok := err.(*httperror.APIError)
		if !ok {
			return nil, httperror.NotFound.Status, err
		}
		if apiError.Code.Status == httperror.NotFound.Status {
			return nil, httperror.InvalidBodyContent.Status, fmt.Errorf("cloud credential not found")
		}
		if apiError.Code.Status != httperror.PermissionDenied.Status {
			return nil, httperror.InvalidBodyContent.Status, err
		}
	}

	cc, err := h.secretsLister.Get(ns, name)
	if err != nil {
		log.Error("Alibaba-handler: error accessing cloud credential", "operation", "get_credentials", "cred_id", credID)
		return nil, httperror.InvalidBodyContent.Status, fmt.Errorf("error accessing cloud credential %s", credID)
	}

	return cc, http.StatusOK, nil
}
