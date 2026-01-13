package helm

import (
	"helm.sh/helm/v3/pkg/action"
	"helm.sh/helm/v3/pkg/release"
	"k8s.io/cli-runtime/pkg/genericclioptions"
)

type Client struct {
	actRun           func(*action.List) ([]*release.Release, error)
	newList          func(*action.Configuration) *action.List
	restClientGetter genericclioptions.RESTClientGetter
}

func NewClient(restClientGetter genericclioptions.RESTClientGetter) *Client {
	return &Client{restClientGetter: restClientGetter, actRun: runAction, newList: action.NewList}
}

func (c *Client) ListReleases(namespace, name string, stateMask action.ListStates) ([]*release.Release, error) {
	helmCfg := &action.Configuration{}
	logFunc := func(format string, v ...interface{}) {
		// Helm expects a printf-style logger, but we're using structured logging
		// So we just ignore helm's internal logs for now
	}
	if err := helmCfg.Init(c.restClientGetter, namespace, "", logFunc); err != nil {
		return nil, err
	}
	l := c.newList(helmCfg)
	l.Filter = "^" + name + "$"
	l.StateMask = stateMask
	return c.actRun(l)
}

func runAction(l *action.List) ([]*release.Release, error) {
	return l.Run()
}
