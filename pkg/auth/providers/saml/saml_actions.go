package saml

import (
	"encoding/json"
	"fmt"
	"net/http"

	"github.com/rancher/norman/httperror"
	"github.com/rancher/norman/types"
	apiv3 "github.com/rancher/rancher/pkg/apis/management.cattle.io/v3"
	"github.com/rancher/rancher/pkg/auth/providers/common"
	"github.com/rancher/rancher/pkg/log"
)

func (s *Provider) formatter(apiContext *types.APIContext, resource *types.RawResource) {
	common.AddCommonActions(apiContext, resource)
	resource.AddAction(apiContext, "testAndEnable")
}

func (s *Provider) actionHandler(actionName string, action *types.Action, request *types.APIContext) error {
	handled, err := common.HandleCommonAction(actionName, action, request, s.name, s.authConfigs)
	if err != nil {
		return err
	}
	if handled {
		return nil
	}

	switch actionName {
	case "testAndEnable":
		return s.testAndEnable(request)
	default:
		return httperror.NewAPIError(httperror.ActionNotAvailable, "")
	}
}

func (s *Provider) testAndEnable(request *types.APIContext) error {
	samlLogin := &apiv3.SamlConfigTestInput{}
	if err := json.NewDecoder(request.Request.Body).Decode(samlLogin); err != nil {
		return httperror.NewAPIError(httperror.InvalidBodyContent,
			fmt.Sprintf("SAML: Failed to parse body: %v", err))
	}

	samlConfig, err := s.getSamlConfig()
	if err != nil {
		return err
	}

	log.Debug("Initializing SAML service provider", "provider", s.name, "operation", "test_and_enable")
	err = InitializeSamlServiceProvider(samlConfig, s.name)
	if err != nil {
		return err
	}

	provider, ok := SamlProviders[s.name]
	if !ok {
		return fmt.Errorf("SAML [testAndEnable]: Provider %v not configured", s.name)
	}

	log.Debug("Setting clientState for SAML service provider", "provider", s.name, "operation", "test_and_enable")

	finalRedirectURL := samlLogin.FinalRedirectURL
	log.Debug("Final redirect will be set", "provider", s.name, "operation", "test_and_enable", "redirect_url", finalRedirectURL)

	provider.clientState.SetPath(provider.serviceProvider.AcsURL.Path)
	provider.clientState.SetState(request.Response, request.Request, "Rancher_FinalRedirectURL", finalRedirectURL)
	provider.clientState.SetState(request.Response, request.Request, "Rancher_Action", testAndEnableAction)

	idpRedirectURL, err := provider.HandleSamlLogin(request.Response, request.Request, provider.userMGR.GetUser(request.Request))
	if err != nil {
		return err
	}
	log.Debug("Redirecting to identity provider login page", "provider", s.name, "operation", "test_and_enable", "idp_redirect_url", idpRedirectURL)
	data := map[string]any{
		"idpRedirectUrl": idpRedirectURL,
		"type":           "samlConfigTestOutput",
	}

	request.WriteResponse(http.StatusOK, data)
	return nil
}
