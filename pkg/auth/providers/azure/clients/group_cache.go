package clients

import (
	"time"

	lru "github.com/hashicorp/golang-lru"
	v3 "github.com/rancher/rancher/pkg/apis/management.cattle.io/v3"
	"github.com/rancher/rancher/pkg/log"
	"golang.org/x/sync/errgroup"
)

// GroupCache is an in-memory cache of group principals.
var GroupCache *lru.Cache

type userPrincipalsClient interface {
	GetGroup(id string) (v3.Principal, error)
}

// UserGroupsToPrincipals attempts to convert a value representing a collection of groups to a slice of principal values.
// It also stores group values in an in-memory cache for faster subsequent access.
func UserGroupsToPrincipals(azureClient userPrincipalsClient, groupNames []string) ([]v3.Principal, error) {
	var tasksManager errgroup.Group
	groupPrincipals := make([]v3.Principal, len(groupNames))

	start := time.Now()
	log.Debug("Started gathering users groups", "provider", "azure", "operation", "user_groups_to_principals")

	for i, id := range groupNames {
		if id == "" {
			continue
		}

		j := i
		groupID := id

		if principal, ok := GroupCache.Get(groupID); ok {
			p, ok := principal.(v3.Principal)
			if !ok {
				log.Error("Failed to convert cached group to principal", "provider", "azure", "group_id", groupID)
				continue
			}
			groupPrincipals[j] = p
			continue
		}

		tasksManager.Go(func() error {
			// This is inefficient for a collection of msgraph.Group. This is temporary - until support for Azure AD Graph is removed.
			// The SDK for Microsoft Graph returns actual groups when queried for a user's group memberships.
			// The SDK for Azure AD Graph returns group names as strings.
			// The common interface that abstracts the Graph operations returns group names as strings.
			// So Microsoft Graph groups are effectively fetched twice. But this happens only once - before the groups are added to the cache.
			groupObj, err := azureClient.GetGroup(groupID)
			if err != nil {
				log.Error("Error getting group", "provider", "azure", "group_id", groupID, "error", err)
				return err
			}
			groupObj.MemberOf = true

			GroupCache.Add(groupID, groupObj)
			groupPrincipals[j] = groupObj
			return nil
		})
	}
	if err := tasksManager.Wait(); err != nil {
		return nil, err
	}
	log.Debug("Completed gathering users groups", "provider", "azure", "operation", "user_groups_to_principals", "duration", time.Since(start), "cache_size", GroupCache.Len())
	return groupPrincipals, nil
}
