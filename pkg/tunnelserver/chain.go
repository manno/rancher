package tunnelserver

import (
	"fmt"
	"net/http"

	"github.com/rancher/rancher/pkg/log"
	"github.com/rancher/remotedialer"
)

type Authorizers struct {
	chain []remotedialer.Authorizer
}

func ErrorWriter(rw http.ResponseWriter, req *http.Request, code int, err error) {
	fullAddress := req.RemoteAddr
	forwardedFor := req.Header.Get("X-Forwarded-For")
	if forwardedFor != "" {
		fullAddress = fmt.Sprintf("%s (X-Forwarded-For: %s)", req.RemoteAddr, forwardedFor)
	}
	log.Error("Failed to handle tunnel request", "operation", "error_writer", "remote_addr", fullAddress, "response_code", code, "error", err)
	log.Trace("Error writer response", "operation", "error_writer", "response_code", code, "request", req)
	remotedialer.DefaultErrorWriter(rw, req, code, err)
}

func (a *Authorizers) Authorize(req *http.Request) (clientKey string, authed bool, err error) {
	var (
		firstErr error
	)

	for _, auth := range a.chain {
		key, authed, err := auth(req)
		if err != nil || !authed {
			if firstErr == nil {
				firstErr = err
			}
			continue
		}
		return key, authed, err
	}

	return "", false, firstErr
}

func (a *Authorizers) Add(authorizer remotedialer.Authorizer) {
	a.chain = append(a.chain, authorizer)
}
