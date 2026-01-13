package tokens

import (
	"context"
	"time"

	"github.com/rancher/norman/clientbase"
	v3 "github.com/rancher/rancher/pkg/generated/norman/management.cattle.io/v3"
	"github.com/rancher/rancher/pkg/log"
	"github.com/rancher/rancher/pkg/namespace"
	"github.com/rancher/rancher/pkg/types/config"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/apimachinery/pkg/util/wait"
)

const intervalSeconds int64 = 3600

func StartPurgeDaemon(ctx context.Context, mgmt *config.ManagementContext) {
	p := &purger{
		tokenLister:      mgmt.Management.Tokens("").Controller().Lister(),
		tokens:           mgmt.Management.Tokens(""),
		samlTokensLister: mgmt.Management.SamlTokens("").Controller().Lister(),
		samlTokens:       mgmt.Management.SamlTokens(""),
	}
	go wait.JitterUntil(p.purge, time.Duration(intervalSeconds)*time.Second, .1, true, ctx.Done())
}

type purger struct {
	tokenLister      v3.TokenLister
	tokens           v3.TokenInterface
	samlTokens       v3.SamlTokenInterface
	samlTokensLister v3.SamlTokenLister
}

func (p *purger) purge() {
	allTokens, err := p.tokenLister.List("", labels.Everything())
	if err != nil {
		log.Error("Error listing tokens during purge", "operation", "purge", "error", err)
	}

	var count int
	for _, token := range allTokens {
		if IsExpired(token) {
			err = p.tokens.Delete(token.ObjectMeta.Name, &metav1.DeleteOptions{})
			if err != nil && !clientbase.IsNotFound(err) {
				log.Error("Error while deleting expired token", "operation", "purge", "token_name", token.ObjectMeta.Name, "error", err)
				continue
			}
			count++
		}
	}
	if count > 0 {
		log.Info("Purged expired tokens", "operation", "purge", "count", count)
	}

	// saml tokens store encrypted token for login request from rancher cli
	samlTokens, err := p.samlTokensLister.List(namespace.GlobalNamespace, labels.Everything())
	if err != nil {
		return
	}

	count = 0
	for _, token := range samlTokens {
		// avoid delete immediately after creation, login request might be pending
		if token.CreationTimestamp.Add(15 * time.Minute).Before(time.Now()) {
			err = p.samlTokens.Delete(token.ObjectMeta.Name, &metav1.DeleteOptions{})
			if err != nil && !clientbase.IsNotFound(err) {
				log.Error("Error while deleting expired token", "operation", "purge", "token_name", token.Name, "error", err)
				continue
			}
			count++
		}
	}
	if count > 0 {
		log.Info("Purged saml tokens", "operation", "purge", "count", count)
	}
}
