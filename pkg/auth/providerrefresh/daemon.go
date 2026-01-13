package providerrefresh

import (
	"fmt"
	"time"

	"github.com/pkg/errors"
	apiv3 "github.com/rancher/rancher/pkg/apis/management.cattle.io/v3"
	"github.com/rancher/rancher/pkg/auth/settings"
	"github.com/rancher/rancher/pkg/auth/tokens"
	exttokenstore "github.com/rancher/rancher/pkg/ext/stores/tokens"
	"github.com/rancher/rancher/pkg/log"
	"github.com/rancher/rancher/pkg/types/config"
	"github.com/robfig/cron"
)

var (
	ref *refresher
	c   = cron.New()
)

func StartRefreshDaemon(scaledContext *config.ScaledContext, mgmtContext *config.ManagementContext) {
	extTokenStore := exttokenstore.NewSystemFromWrangler(scaledContext.Wrangler)
	refreshCronTime := settings.AuthUserInfoResyncCron.Get()
	maxAge := settings.AuthUserInfoMaxAgeSeconds.Get()
	ref = &refresher{
		tokenLister:               mgmtContext.Management.Tokens("").Controller().Lister(),
		tokens:                    mgmtContext.Management.Tokens(""),
		userLister:                mgmtContext.Management.Users("").Controller().Lister(),
		tokenMGR:                  tokens.NewManager(scaledContext.Wrangler),
		userAttributes:            mgmtContext.Management.UserAttributes(""),
		userAttributeLister:       mgmtContext.Management.UserAttributes("").Controller().Lister(),
		extTokenStore:             extTokenStore,
		ensureAndGetUserAttribute: scaledContext.UserManager.EnsureAndGetUserAttribute,
	}

	UpdateRefreshMaxAge(maxAge)
	UpdateRefreshCronTime(refreshCronTime)

}

func UpdateRefreshCronTime(refreshCronTime string) {
	if ref == nil || refreshCronTime == "" {
		return
	}

	parsed, err := ParseCron(refreshCronTime)
	if err != nil {
		log.Error("Error parsing cron", "operation", "update_refresh_cron_time", "error", err)
		return
	}

	c.Stop()
	c = cron.New()

	if parsed != nil {
		job := cron.FuncJob(RefreshAllForCron)
		c.Schedule(parsed, job)
		c.Start()
	}
}

func UpdateRefreshMaxAge(maxAge string) {
	if ref == nil {
		return
	}

	ref.ensureMaxAgeUpToDate(maxAge)
}

func RefreshAllForCron() {
	if ref == nil {
		return
	}

	log.Debug("Triggering auth refresh cron", "operation", "refresh_all_for_cron")
	ref.refreshAll(false)
}

func RefreshAttributes(attribs *apiv3.UserAttribute) (*apiv3.UserAttribute, error) {
	if ref == nil {
		return nil, errors.Errorf("refresh daemon not yet initialized")
	}

	log.Debug("Starting refresh process", "operation", "refresh_attributes", "user_attribute", attribs.Name)
	modified, err := ref.refreshAttributes(attribs)
	if err != nil {
		return nil, fmt.Errorf("error refreshing userattribute %s: %w", attribs.Name, err)
	}
	log.Debug("Finished refresh process", "operation", "refresh_attributes", "user_attribute", attribs.Name)
	modified.LastRefresh = time.Now().UTC().Format(time.RFC3339)
	modified.NeedsRefresh = false
	return modified, nil
}

func ParseMaxAge(setting string) (time.Duration, error) {
	durString := fmt.Sprintf("%vs", setting)
	dur, err := time.ParseDuration(durString)
	if err != nil {
		return 0, fmt.Errorf("error parsing auth refresh max age: %v", err)
	}
	return dur, nil
}

func ParseCron(setting string) (cron.Schedule, error) {
	if setting == "" {
		return nil, nil
	}
	schedule, err := cron.ParseStandard(setting)
	if err != nil {
		return nil, fmt.Errorf("error parsing auth refresh cron: %v", err)
	}
	return schedule, nil
}
