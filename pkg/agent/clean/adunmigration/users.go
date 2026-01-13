package adunmigration

import (
	"fmt"
	"strings"

	"github.com/pkg/errors"
	"github.com/rancher/rancher/pkg/log"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	v3 "github.com/rancher/rancher/pkg/apis/management.cattle.io/v3"
	"github.com/rancher/rancher/pkg/types/config"
)

func describePlannedChanges(workunit migrateUserWorkUnit) {
	log.Debug("DRY RUN: changes to user have NOT been saved",
		"operation", migrateAdUserOperation,
		"user", workunit.originalUser.Name)
	if len(workunit.duplicateUsers) > 0 {
		log.Info("DRY RUN: duplicate users were identified", "operation", migrateAdUserOperation)
		for _, duplicateUser := range workunit.duplicateUsers {
			log.Info("DRY RUN: would DELETE user",
				"operation", migrateAdUserOperation,
				"user", duplicateUser.Name)
		}
	}
}

func deleteDuplicateUsers(workunit migrateUserWorkUnit, sc *config.ScaledContext) error {
	for _, duplicateUser := range workunit.duplicateUsers {
		err := sc.Management.Users("").Delete(duplicateUser.Name, &metav1.DeleteOptions{})
		if err != nil {
			log.Error("Failed to delete duplicate user",
				"operation", migrateAdUserOperation,
				"user", duplicateUser.Name,
				"error", err)
			// If the duplicate deletion has failed for some reason, it is NOT safe to save the modified user, as
			// this may result in a duplicate AD principal ID. Notify and skip.

			log.Error("Cannot safely save modifications to user, skipping",
				"operation", migrateAdUserOperation,
				"user", workunit.originalUser.Name)
			return errors.Errorf("failed to delete duplicate users")
		}
		log.Info("Deleted duplicate user",
			"operation", migrateAdUserOperation,
			"user", duplicateUser.Name)
	}
	return nil
}

func updateModifiedUser(workunit migrateUserWorkUnit, sc *config.ScaledContext) {
	workunit.originalUser.Annotations[adGUIDMigrationAnnotation] = workunit.guid
	workunit.originalUser.Labels[adGUIDMigrationLabel] = migratedLabelValue
	_, err := sc.Management.Users("").Update(workunit.originalUser)
	if err != nil {
		log.Error("Failed to save modified user",
			"operation", migrateAdUserOperation,
			"user", workunit.originalUser.Name,
			"error", err)
	}
	log.Info("User was successfully migrated",
		"operation", migrateAdUserOperation,
		"user", workunit.originalUser.Name)
}

func replaceGUIDPrincipalWithDn(user *v3.User, dn string, guid string, dryRun bool) {
	// It's weird for a single user to have more than just an AD and a Local principal ID, but it *can* happen
	// if Rancher has used more than one auth provider over its history. Here we'll keep all principal IDs
	// that are unrelated to AD
	var principalIDs []string
	for _, principalID := range user.PrincipalIDs {
		if !strings.HasPrefix(principalID, activeDirectoryPrefix) {
			principalIDs = append(principalIDs, principalID)
		}
	}
	principalIDs = append(principalIDs, activeDirectoryPrefix+dn)

	if dryRun {
		// In dry run mode we will merely print the computed list and leave the original user object alone
		log.Info("DRY RUN: user with GUID would have new principals",
			"operation", migrateAdUserOperation,
			"user", user.Name,
			"guid", guid)
		for _, principalID := range principalIDs {
			log.Info("DRY RUN: principal",
				"operation", migrateAdUserOperation,
				"principal_id", principalID)
		}
	} else {
		user.PrincipalIDs = principalIDs
		log.Debug("User with GUID will have new principals",
			"operation", migrateAdUserOperation,
			"user", user.Name,
			"guid", guid)
		for _, principalID := range user.PrincipalIDs {
			log.Debug("Principal",
				"operation", migrateAdUserOperation,
				"principal_id", principalID)
		}
	}
}

func isAdUser(user *v3.User) bool {
	for _, principalID := range user.PrincipalIDs {
		if strings.HasPrefix(principalID, activeDirectoryPrefix) {
			return true
		}
	}
	return false
}

func adPrincipalID(user *v3.User) string {
	for _, principalID := range user.PrincipalIDs {
		if strings.HasPrefix(principalID, activeDirectoryPrefix) {
			return principalID
		}
	}
	return ""
}

func getExternalID(principalID string) (string, error) {
	parts := strings.Split(principalID, "://")
	if len(parts) != 2 {
		return "", fmt.Errorf("[%v] failed to parse invalid principalID: %v", identifyAdUserOperation, principalID)
	}
	return parts[1], nil
}

func getScope(principalID string) (string, error) {
	parts := strings.Split(principalID, "://")
	if len(parts) != 2 {
		return "", fmt.Errorf("[%v] failed to parse invalid principalID: %v", identifyAdUserOperation, principalID)
	}
	return parts[0], nil
}
