package saml

import (
	"crypto/x509"
	"encoding/base64"
	"encoding/xml"
	"fmt"
	"net/http"
	"strings"

	"github.com/crewjam/saml"
	"github.com/golang-jwt/jwt/v5"
	log "github.com/rancher/rancher/pkg/log"
)

const rancherUserID = "rancherUserID"

// ServeHTTP is the handler for /saml/metadata and /saml/acs endpoints
func (s *Provider) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	serviceProvider := s.serviceProvider

	log.Debug("Saml received request", "method", r.Method, "url", r.URL)

	r.ParseForm()

	if r.URL.Path == serviceProvider.MetadataURL.Path {
		buf, _ := xml.MarshalIndent(serviceProvider.Metadata(), "", "  ")
		w.Header().Set("Content-Type", "application/samlmetadata+xml")
		w.Write(buf)

		log.Debug("Saml returned metadata")
		return
	}

	if r.URL.Path == serviceProvider.AcsURL.Path {
		log.Debug("Saml assertion processing started")

		r.ParseForm()
		assertion, err := serviceProvider.ParseResponse(r, s.getPossibleRequestIDs(r))

		if err != nil {
			log.Debug("Saml assertion validation failed", "error", err)

			if parseErr, ok := err.(*saml.InvalidResponseError); ok {
				// Note: If access to the response itself is needed (debugging)
				// just add `parseErr.Response` to the log statement.

				log.Debug("Saml assertion validation details",
					"now", parseErr.Now, "private_error", parseErr.PrivateErr)
			}

			redirectURL := r.URL.Host + "/login?errorCode=403"
			http.Redirect(w, r, redirectURL, http.StatusFound)
			return
		}

		log.Debug("Saml assertions validated")

		s.HandleSamlAssertion(w, r, assertion)

		log.Debug("Saml assertion processing completed")
		return
	}

	if r.URL.Path == serviceProvider.SloURL.Path {
		log.Debug("Saml logout response processing started")

		s.FinalizeSamlLogout(w, r)

		log.Debug("Saml logout response processing completed")
		return
	}

	log.Debug("Saml failed to handle request", "method", r.Method, "url", r.URL)

	http.NotFoundHandler().ServeHTTP(w, r)
}

func (s *Provider) getPossibleRequestIDs(r *http.Request) []string {
	rv := []string{}
	serviceProvider := s.serviceProvider

	for name, value := range s.clientState.GetStates(r) {
		if strings.HasPrefix(name, "Rancher_") {
			continue
		}
		jwtParser := newJWTParser()
		token, err := jwtParser.Parse(value, func(t *jwt.Token) (any, error) {
			secretBlock := x509.MarshalPKCS1PrivateKey(serviceProvider.Key)
			return secretBlock, nil
		})
		if err != nil || !token.Valid {
			log.Debug("Saml invalid token", "error", err)
			continue
		}
		claims := token.Claims.(jwt.MapClaims)
		rv = append(rv, claims["id"].(string))
	}
	return rv
}

// HandleSamlLogin is the endpoint for /saml/login endpoint
func (s *Provider) HandleSamlLogin(w http.ResponseWriter, r *http.Request, userID string) (string, error) {
	serviceProvider := s.serviceProvider
	if r.URL.Path == serviceProvider.AcsURL.Path {
		return "", fmt.Errorf("don't wrap Middleware with RequireAccount")
	}

	binding := saml.HTTPRedirectBinding
	bindingLocation := serviceProvider.GetSSOBindingLocation(binding)

	req, err := serviceProvider.MakeAuthenticationRequest(bindingLocation, binding, saml.HTTPPostBinding)
	if err != nil {
		return "", err
	}

	// relayState is limited to 80 bytes but also must be integrity protected.
	// this means that we cannot use a JWT because it is way too long. Instead
	// we set a cookie that corresponds to the state
	relayState := base64.URLEncoding.EncodeToString(randomBytes(42))

	secretBlock := x509.MarshalPKCS1PrivateKey(serviceProvider.Key)
	state := jwt.New(jwt.SigningMethodHS256)
	claims := state.Claims.(jwt.MapClaims)
	claims["id"] = req.ID
	claims["uri"] = r.URL.String()
	if userID != "" {
		claims[rancherUserID] = userID
	}

	signedState, err := state.SignedString(secretBlock)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return "", err
	}

	s.clientState.SetState(w, r, relayState, signedState)

	redirectURL, err := req.Redirect(relayState, serviceProvider)
	if err != nil {
		return "", err
	}

	return redirectURL.String(), nil
}

// HandleSamlLogout is the final handler for the logoutAll action
func (s *Provider) HandleSamlLogout(name string, w http.ResponseWriter, r *http.Request) (string, error) {
	serviceProvider := s.serviceProvider
	if r.URL.Path == serviceProvider.AcsURL.Path {
		return "", fmt.Errorf("don't wrap Middleware with RequireAccount")
	}

	binding := saml.HTTPRedirectBinding
	bindingLocation := serviceProvider.GetSLOBindingLocation(binding)

	req, err := serviceProvider.MakeLogoutRequest(bindingLocation, name)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return "", err
	}
	// relayState is limited to 80 bytes but also must be integrity protected.
	// this means that we cannot use a JWT because it is way too long. Instead
	// we set a cookie that corresponds to the state
	relayState := base64.URLEncoding.EncodeToString(randomBytes(42))

	secretBlock := x509.MarshalPKCS1PrivateKey(serviceProvider.Key)
	state := jwt.New(jwt.SigningMethodHS256)
	claims := state.Claims.(jwt.MapClaims)
	claims["id"] = req.ID
	claims["uri"] = r.URL.String()

	signedState, err := state.SignedString(secretBlock)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return "", err
	}

	s.clientState.SetState(w, r, relayState, signedState)

	redirectURL, err := req.Redirect(relayState, serviceProvider)
	if err != nil {
		return "", err
	}

	return redirectURL.String(), nil
}

func randomBytes(n int) []byte {
	rv := make([]byte, n)
	if _, err := saml.RandReader.Read(rv); err != nil {
		panic(err)
	}
	return rv
}
