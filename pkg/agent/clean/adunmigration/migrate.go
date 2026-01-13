/*
Look for any active directory users with a GUID type principal.
Convert these users to a distinguished name instead.
*/

package adunmigration

import (
	"context"
	"fmt"
	"strconv"
	"strings"
	"time"

	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"

	"github.com/pkg/errors"
	"github.com/rancher/rancher/pkg/log"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	restclient "k8s.io/client-go/rest"

	v3 "github.com/rancher/rancher/pkg/apis/management.cattle.io/v3"
	"github.com/rancher/rancher/pkg/auth/providers/activedirectory"
	"github.com/rancher/rancher/pkg/types/config"
)

const (
	migrateAdUserOperation    = "migrate-ad-user"
	identifyAdUserOperation   = "identify-ad-users"
	migrateTokensOperation    = "migrate-ad-tokens"
	migrateCrtbsOperation     = "migrate-ad-crtbs"
	migratePrtbsOperation     = "migrate-ad-prtbs"
	migrateGrbsOperation      = "migrate-ad-grbs"
	activeDirecotryName       = "activedirectory"
	activeDirectoryScope      = "activedirectory_user"
	activeDirectoryPrefix     = "activedirectory_user://"
	localPrefix               = "local://"
	adGUIDMigrationLabel      = "ad-guid-migration"
	adGUIDMigrationAnnotation = "ad-guid-migration-data"
	adGUIDMigrationPrefix     = "migration-"
	migratedLabelValue        = "migrated"
	migrationPreviousName     = "ad-guid-previous-name"
	AttributeObjectClass      = "objectClass"
	AttributeObjectGUID       = "objectGUID"
	migrateStatusSkipped      = "skippedUsers"
	migrateStatusMissing      = "missingUsers"
	migrateStatusCountSuffix  = "Count"
	migrationStatusPercentage = "percentDone"
	migrationStatusLastUpdate = "statusLastUpdated"
)

type migrateUserWorkUnit struct {
	distinguishedName string
	guid              string
	originalUser      *v3.User
	duplicateUsers    []*v3.User
	principal         *v3.Principal

	activeDirectoryCRTBs []v3.ClusterRoleTemplateBinding
	duplicateLocalCRTBs  []v3.ClusterRoleTemplateBinding

	activeDirectoryPRTBs []v3.ProjectRoleTemplateBinding
	duplicateLocalPRTBs  []v3.ProjectRoleTemplateBinding

	duplicateLocalGRBs []v3.GlobalRoleBinding

	activeDirectoryTokens []v3.Token
	duplicateLocalTokens  []v3.Token
}

type missingUserWorkUnit struct {
	guid           string
	originalUser   *v3.User
	duplicateUsers []*v3.User
}

type skippedUserWorkUnit struct {
	guid         string
	originalUser *v3.User
}

func scaledContext(restConfig *restclient.Config) (*config.ScaledContext, error) {
	sc, err := config.NewScaledContext(*restConfig, nil)
	if err != nil {
		log.Error("Failed to create scaledContext",
			"operation", migrateAdUserOperation,
			"error", err)
		return nil, err
	}

	ctx := context.Background()
	err = sc.Start(ctx)
	if err != nil {
		log.Error("Failed to start scaled context",
			"operation", migrateAdUserOperation,
			"error", err)
		return nil, err
	}

	return sc, nil
}

// UnmigrateAdGUIDUsersOnce will ensure that the migration script will run only once.  cycle through all users, ctrb, ptrb, tokens and migrate them to an
// appropriate DN-based PrincipalID.
func UnmigrateAdGUIDUsersOnce(sc *config.ScaledContext) error {
	migrationConfigMap, err := sc.Core.ConfigMaps(activedirectory.StatusConfigMapNamespace).GetNamespaced(activedirectory.StatusConfigMapNamespace, activedirectory.StatusConfigMapName, metav1.GetOptions{})
	if err != nil && !apierrors.IsNotFound(err) {
		log.Error("Unable to check unmigration configmap",
			"operation", migrateAdUserOperation,
			"error", err)
		log.Error("Cannot determine if it is safe to proceed, refusing to run",
			"operation", migrateAdUserOperation)
		return nil
	}
	if migrationConfigMap != nil {
		migrationStatus := migrationConfigMap.Data[activedirectory.StatusMigrationField]
		switch migrationStatus {
		case activedirectory.StatusMigrationFinished:
			log.Debug("Ad-guid migration has already been completed, refusing to run again at startup", "operation", migrateAdUserOperation)
			return nil
		case activedirectory.StatusMigrationFinishedWithMissing:
			log.Info("Ad-guid migration has already been completed. To clean-up missing users, you can run the utility manually", "operation", migrateAdUserOperation)
			return nil
		case activedirectory.StatusMigrationFinishedWithSkipped:
			log.Info("Ad-guid migration has already been completed. To try and resolve skipped users, you can run the utility manually", "operation", migrateAdUserOperation)
			return nil
		}

	}
	return UnmigrateAdGUIDUsers(&sc.RESTConfig, false, false)
}

// UnmigrateAdGUIDUsers will cycle through all users, ctrb, ptrb, tokens and migrate them to an
// appropriate DN-based PrincipalID.
func UnmigrateAdGUIDUsers(clientConfig *restclient.Config, dryRun bool, deleteMissingUsers bool) error {
	if dryRun {
		log.Info("DryRun is true, no objects will be deleted/modified", "operation", migrateAdUserOperation)
		deleteMissingUsers = false
	} else if deleteMissingUsers {
		log.Info("DeleteMissingUsers is true, GUID-based users not present in Active Directory will be deleted", "operation", migrateAdUserOperation)
	}

	sc, adConfig, err := prepareClientContexts(clientConfig)
	if err != nil {
		return err
	}

	migrationConfigMap, err := sc.Core.ConfigMaps(activedirectory.StatusConfigMapNamespace).GetNamespaced(activedirectory.StatusConfigMapNamespace, activedirectory.StatusConfigMapName, metav1.GetOptions{})
	if err != nil && !apierrors.IsNotFound(err) {
		log.Error("Unable to check unmigration configmap",
			"operation", migrateAdUserOperation,
			"error", err)
		log.Error("Cannot determine if it is safe to proceed, refusing to run",
			"operation", migrateAdUserOperation)
		return nil
	}
	if migrationConfigMap != nil {
		migrationStatus := migrationConfigMap.Data[activedirectory.StatusMigrationField]
		switch migrationStatus {
		case activedirectory.StatusMigrationRunning:
			log.Info("Ad-guid migration is currently running, refusing to run again concurrently", "operation", migrateAdUserOperation)
			return nil
		}
	}

	finalStatus := activedirectory.StatusMigrationFinished

	// set the status to running and reset the unmigrated fields
	if !dryRun {
		err = updateMigrationStatus(sc, activedirectory.StatusMigrationField, activedirectory.StatusMigrationRunning)
		if err != nil {
			log.Error("Unable to update migration status configmap",
				"operation", migrateAdUserOperation,
				"error", err)
			return err
		}
		updateUnmigratedUsers("", migrateStatusSkipped, true, sc)
		updateUnmigratedUsers("", migrateStatusMissing, true, sc)
		// If we return past this point, no matter how we got there, make sure we update the configmap to clear the
		// status away from "running." If we fail to do this, we block AD-based logins indefinitely.
		defer func(sc *config.ScaledContext, status string) {
			err := updateMigrationStatus(sc, status, finalStatus)
			if err != nil {
				log.Error("Unable to update migration status configmap",
					"operation", migrateAdUserOperation,
					"error", err)
			}
		}(sc, activedirectory.StatusMigrationField)

		// Early bail: if the AD configuration is disabled, then we're done! Update the configmap right now and exit.
		if !adConfig.Enabled {
			log.Info("During unmigration, found that Active Directory is not enabled. nothing to do", "operation", migrateAdUserOperation)
			finalStatus = activedirectory.StatusMigrationFinished
			return nil
		}
	}

	log.Info("Beginning ad-guid unmigration", "operation", migrateAdUserOperation)

	users, err := sc.Management.Users("").List(metav1.ListOptions{})
	if err != nil {
		log.Error("Unable to fetch user list",
			"operation", migrateAdUserOperation,
			"error", err)
		return err
	}

	lConn := sharedLdapConnection{adConfig: adConfig}
	usersToMigrate, missingUsers, skippedUsers := identifyMigrationWorkUnits(users, lConn)
	// If any of the below functions fail, there is either a permissions problem or a more serious issue with the
	// Rancher API. We should bail in this case and not attempt to process users.

	tokenInterface := sc.Management.Tokens("")
	tokenList, err := tokenInterface.List(metav1.ListOptions{})
	if err != nil {
		finalStatus = activedirectory.StatusMigrationFailed
		log.Error("Unable to fetch token objects",
			"operation", migrateAdUserOperation,
			"error", err)
		return err
	}
	identifyTokens(&usersToMigrate, tokenList)

	crtbInterface := sc.Management.ClusterRoleTemplateBindings("")
	crtbList, err := crtbInterface.List(metav1.ListOptions{})
	if err != nil {
		finalStatus = activedirectory.StatusMigrationFailed
		log.Error("Unable to fetch CRTB objects",
			"operation", migrateAdUserOperation,
			"error", err)
		return err
	}
	identifyCRTBs(&usersToMigrate, crtbList)

	prtbInterface := sc.Management.ProjectRoleTemplateBindings("")
	prtbList, err := prtbInterface.List(metav1.ListOptions{})
	if err != nil {
		finalStatus = activedirectory.StatusMigrationFailed
		log.Error("Unable to fetch PRTB objects",
			"operation", migrateAdUserOperation,
			"error", err)
		return err
	}
	identifyPRTBs(&usersToMigrate, prtbList)

	grbInterface := sc.Management.GlobalRoleBindings("")
	grbList, err := grbInterface.List(metav1.ListOptions{})
	if err != nil {
		finalStatus = activedirectory.StatusMigrationFailed
		log.Error("Unable to fetch GRB objects",
			"operation", migrateAdUserOperation,
			"error", err)
		return err
	}
	identifyGRBs(&usersToMigrate, grbList)

	if len(missingUsers) > 0 {
		finalStatus = activedirectory.StatusMigrationFinishedWithMissing
	}
	if len(skippedUsers) > 0 {
		finalStatus = activedirectory.StatusMigrationFinishedWithSkipped
	}

	for _, user := range skippedUsers {
		log.Error("Unable to migrate user due to a connection failure, this user will be skipped",
			"operation", migrateAdUserOperation,
			"user", user.originalUser.Name)
		if !dryRun {
			updateUnmigratedUsers(user.originalUser.Name, migrateStatusSkipped, false, sc)
		}
	}
	for _, missingUser := range missingUsers {
		if deleteMissingUsers && !dryRun {
			log.Info("User with GUID does not seem to exist in Active Directory, proceeding to delete this user permanently",
				"operation", migrateAdUserOperation,
				"user", missingUser.originalUser.Name,
				"guid", missingUser.guid)
			updateUnmigratedUsers(missingUser.originalUser.Name, migrateStatusMissing, false, sc)
			err = sc.Management.Users("").Delete(missingUser.originalUser.Name, &metav1.DeleteOptions{})
			if err != nil {
				log.Error("Failed to delete missing user",
					"operation", migrateAdUserOperation,
					"user", missingUser.originalUser.Name,
					"error", err)
			}
		} else {
			log.Info("User with GUID does not seem to exist in Active Directory, this user will be skipped",
				"operation", migrateAdUserOperation,
				"user", missingUser.originalUser.Name,
				"guid", missingUser.guid)
			if !dryRun {
				updateUnmigratedUsers(missingUser.originalUser.Name, migrateStatusMissing, false, sc)
			}
		}
	}

	for i, userToMigrate := range usersToMigrate {
		// Note: some resources may fail to migrate due to webhook constraints; this applies especially to bindings
		// that refer to disabled templates, as rancher won't allow us to create the replacements. We'll log these
		// errors, but do not consider them to be serious enough to stop processing the remainder of each user's work.
		migrateCRTBs(&userToMigrate, sc, dryRun)
		migratePRTBs(&userToMigrate, sc, dryRun)
		migrateGRBs(&userToMigrate, sc, dryRun)
		migrateTokens(&userToMigrate, sc, dryRun)
		replaceGUIDPrincipalWithDn(userToMigrate.originalUser, userToMigrate.distinguishedName, userToMigrate.guid, dryRun)

		if dryRun {
			describePlannedChanges(userToMigrate)
		} else {
			err = deleteDuplicateUsers(userToMigrate, sc)
			if err == nil {
				updateModifiedUser(userToMigrate, sc)
			}
			percentDone := float64(i+1) / float64(len(usersToMigrate)) * 100
			progress := fmt.Sprintf("%.0f%%", percentDone)
			err = updateMigrationStatus(sc, migrationStatusPercentage, progress)
			if err != nil {
				log.Error("Unable to update migration status",
					"operation", migrateAdUserOperation,
					"error", err)
			}
		}
	}

	err = migrateAllowedUserPrincipals(&usersToMigrate, &missingUsers, sc, dryRun, deleteMissingUsers)
	if err != nil {
		log.Error("Unable to migrate allowed users",
			"operation", migrateAdUserOperation,
			"error", err)
		finalStatus = activedirectory.StatusMigrationFailed
		return err
	}

	if dryRun {
		log.Info("End of ad-guid unmigration", "operation", migrateAdUserOperation)
	} else {
		log.Info("End of ad-guid unmigration, results saved to configmap",
			"operation", migrateAdUserOperation,
			"configmap", activedirectory.StatusConfigMapName)
	}

	return nil
}

// identifyMigrationWorkUnits locates ActiveDirectory users with GUID and DN based principal IDs and sorts them
// into work units based on whether those users can be located in the upstream Active Directory provider. Specifically:
//
//	usersToMigrate contains GUID-based original users and any duplicates (GUID or DN based) that we wish to merge
//	missingUsers contains GUID-based users who could not be found in Active Directory
//	skippedUsers contains GUID-based users that could not be processed, usually due to an LDAP connection failure
func identifyMigrationWorkUnits(users *v3.UserList, lConn retryableLdapConnection) (
	[]migrateUserWorkUnit, []missingUserWorkUnit, []skippedUserWorkUnit) {
	// Note: we *could* make the ldap connection on the spot here, but we're accepting it as a parameter specifically
	// so that this function is easier to test. This setup allows us to mock the ldap connection and thus more easily
	// test unusual Active Directory responses to our searches.

	var usersToMigrate []migrateUserWorkUnit
	var missingUsers []missingUserWorkUnit
	var skippedUsers []skippedUserWorkUnit

	// These assist with quickly identifying duplicates, so we don't have to scan the whole structure each time.
	// We key on guid/dn, and the value is the index of that work unit in the associated table
	knownGUIDWorkUnits := map[string]int{}
	knownGUIDMissingUnits := map[string]int{}
	knownDnWorkUnits := map[string]int{}

	// Now we'll make two passes over the list of all users. First we need to identify any GUID based users, and
	// sort them into "found" and "not found" lists. At this stage we might have GUID-based duplicates, and we'll
	// detect and sort those accordingly
	ldapPermanentlyFailed := false
	log.Debug("Locating GUID-based Active Directory users", "operation", identifyAdUserOperation)
	for _, user := range users.Items {
		if !isAdUser(&user) {
			log.Debug("User has no AD principals, skipping",
				"operation", identifyAdUserOperation,
				"user", user.Name)
			continue
		}
		principalID := adPrincipalID(&user)
		log.Debug("Processing AD user",
			"operation", identifyAdUserOperation,
			"user", user.Name,
			"principal_id", principalID)
		if !isGUID(principalID) {
			log.Debug("Does not appear to be a GUID-based principal ID, taking no action",
				"operation", identifyAdUserOperation,
				"principal_id", principalID)
			continue
		}
		guid, err := getExternalID(principalID)

		if err != nil {
			// This really shouldn't be possible to hit, since isGuid will fail to parse anything that would
			// cause getExternalID to choke on the input, but for maximum safety we'll handle it anyway.
			log.Error("Failed to extract GUID from principal, cannot process user",
				"operation", identifyAdUserOperation,
				"principal_id", principalID,
				"user", user.Name,
				"error", err)
			continue
		}
		// If our LDAP connection has gone sour, we still need to log this user for reporting
		userCopy := user.DeepCopy()
		if ldapPermanentlyFailed {
			skippedUsers = append(skippedUsers, skippedUserWorkUnit{guid: guid, originalUser: userCopy})
		} else {
			// Check for guid-based duplicates here. If we find one, we don't need to perform an other LDAP lookup.
			if i, exists := knownGUIDWorkUnits[guid]; exists {
				log.Debug("User is GUID-based and a duplicate",
					"operation", identifyAdUserOperation,
					"user", user.Name,
					"guid", guid,
					"duplicate_of", usersToMigrate[i].originalUser.Name)
				// Make sure the oldest duplicate user is selected as the original
				if usersToMigrate[i].originalUser.CreationTimestamp.Time.After(user.CreationTimestamp.Time) {
					usersToMigrate[i].duplicateUsers = append(usersToMigrate[i].duplicateUsers, usersToMigrate[i].originalUser)
					usersToMigrate[i].originalUser = userCopy
				} else {
					usersToMigrate[i].duplicateUsers = append(usersToMigrate[i].duplicateUsers, userCopy)
				}
				continue
			}
			if i, exists := knownGUIDMissingUnits[guid]; exists {
				log.Debug("User is GUID-based and a duplicate of user which is known to be missing",
					"operation", identifyAdUserOperation,
					"user", user.Name,
					"guid", guid,
					"duplicate_of", missingUsers[i].originalUser.Name)
				// We're less picky about the age of the oldest user here, because we aren't going to deduplicate these
				missingUsers[i].duplicateUsers = append(missingUsers[i].duplicateUsers, userCopy)
				continue
			}
			dn, principal, err := lConn.findLdapUserWithRetries(guid)
			if errors.Is(err, LdapFoundDuplicateGUID{}) {
				log.Error("LDAP returned multiple users with GUID, this should not be possible and may indicate a configuration error, this user will be skipped",
					"operation", identifyAdUserOperation,
					"guid", guid)
				skippedUsers = append(skippedUsers, skippedUserWorkUnit{guid: guid, originalUser: userCopy})
			} else if errors.Is(err, LdapErrorNotFound{}) {
				log.Debug("User is GUID-based and the Active Directory server doesn't know about it, marking as missing",
					"operation", identifyAdUserOperation,
					"user", user.Name,
					"guid", guid)
				knownGUIDMissingUnits[guid] = len(missingUsers)
				missingUsers = append(missingUsers, missingUserWorkUnit{guid: guid, originalUser: userCopy})
			} else if err != nil {
				log.Warn("LDAP connection has permanently failed! will continue to migrate previously identified users", "operation", identifyAdUserOperation)
				skippedUsers = append(skippedUsers, skippedUserWorkUnit{guid: guid, originalUser: userCopy})
				ldapPermanentlyFailed = true
			} else {
				log.Debug("User is GUID-based and the Active Directory server knows it",
					"operation", identifyAdUserOperation,
					"user", user.Name,
					"guid", guid,
					"distinguished_name", dn)
				knownGUIDWorkUnits[guid] = len(usersToMigrate)
				knownDnWorkUnits[dn] = len(usersToMigrate)
				var emptyDuplicateList []*v3.User
				usersToMigrate = append(usersToMigrate, migrateUserWorkUnit{guid: guid, distinguishedName: dn, principal: principal, originalUser: userCopy, duplicateUsers: emptyDuplicateList})
			}
		}
	}

	if len(usersToMigrate) == 0 {
		log.Debug("Found 0 users in need of migration, exiting without checking for DN-based duplicates", "operation", identifyAdUserOperation)
		return usersToMigrate, missingUsers, skippedUsers
	}

	// Now for the second pass, we need to identify DN-based users, and see if they are duplicates of any of the GUID
	// users that we found in the first pass. We'll prefer the oldest user as the originalUser object, this will be
	// the one we keep when we resolve duplicates later.
	log.Debug("Locating any DN-based Active Directory users", "operation", identifyAdUserOperation)
	for _, user := range users.Items {
		if !isAdUser(&user) {
			log.Debug("User has no AD principals, skipping",
				"operation", identifyAdUserOperation,
				"user", user.Name)
			continue
		}
		principalID := adPrincipalID(&user)
		log.Debug("Processing AD user",
			"operation", identifyAdUserOperation,
			"user", user.Name,
			"principal_id", principalID)
		if isGUID(principalID) {
			log.Debug("Does not appear to be a DN-based principal ID, taking no action",
				"operation", identifyAdUserOperation,
				"principal_id", principalID)
			continue
		}
		dn, err := getExternalID(principalID)
		if err != nil {
			log.Error("Failed to extract DN from principal, cannot process user",
				"operation", identifyAdUserOperation,
				"principal_id", principalID,
				"user", user.Name,
				"error", err)
			continue
		}
		if i, exists := knownDnWorkUnits[dn]; exists {
			log.Debug("User is DN-based and a duplicate",
				"operation", identifyAdUserOperation,
				"user", user.Name,
				"distinguished_name", dn,
				"duplicate_of", usersToMigrate[i].originalUser.Name)
			// Make sure the oldest duplicate user is selected as the original
			userCopy := user.DeepCopy()
			if usersToMigrate[i].originalUser.CreationTimestamp.Time.After(user.CreationTimestamp.Time) {
				usersToMigrate[i].duplicateUsers = append(usersToMigrate[i].duplicateUsers, usersToMigrate[i].originalUser)
				usersToMigrate[i].originalUser = userCopy
			} else {
				usersToMigrate[i].duplicateUsers = append(usersToMigrate[i].duplicateUsers, userCopy)
			}
		}
	}

	return usersToMigrate, missingUsers, skippedUsers
}

func workUnitContainsName(workunit *migrateUserWorkUnit, name string) bool {
	if workunit.originalUser.Name == name {
		return true
	}
	for _, duplicateLocalUser := range workunit.duplicateUsers {
		if duplicateLocalUser.Name == name {
			return true
		}
	}
	return false
}

func updateMigrationStatus(sc *config.ScaledContext, status string, value string) error {
	cm, err := sc.Core.ConfigMaps(activedirectory.StatusConfigMapNamespace).Get(activedirectory.StatusConfigMapName, metav1.GetOptions{})
	if err != nil {
		// Create a new ConfigMap if it doesn't exist
		if !apierrors.IsNotFound(err) {
			return err
		}
		cm = &corev1.ConfigMap{
			ObjectMeta: metav1.ObjectMeta{
				Name:      activedirectory.StatusConfigMapName,
				Namespace: activedirectory.StatusConfigMapNamespace,
			},
		}
	}
	if cm.Data == nil {
		cm.Data = map[string]string{}
	}
	cm.Data[status] = value
	cm.Data[migrationStatusLastUpdate] = metav1.Now().Format(time.RFC3339)

	if _, err := sc.Core.ConfigMaps(activedirectory.StatusConfigMapNamespace).Update(cm); err != nil {
		// If the ConfigMap does not exist, create it
		if apierrors.IsNotFound(err) {
			_, err = sc.Core.ConfigMaps(activedirectory.StatusConfigMapNamespace).Create(cm)
			if err != nil {
				return fmt.Errorf("[%v] unable to create migration status configmap: %v", migrateAdUserOperation, err)
			}
		}
	}
	err = updateADConfigMigrationStatus(cm.Data, sc)
	if err != nil {
		return fmt.Errorf("unable to update AuthConfig status: %v", err)
	}
	return nil
}

// updateUnmigratedUsers will add a user to the list for the specified migration status in the migration status configmap.
// If reset is set to true, it will empty the list.
func updateUnmigratedUsers(user string, status string, reset bool, sc *config.ScaledContext) {
	cm, err := sc.Core.ConfigMaps(activedirectory.StatusConfigMapNamespace).Get(activedirectory.StatusConfigMapName, metav1.GetOptions{})
	if err != nil {
		log.Error("Unable to fetch configmap to update users",
			"operation", migrateAdUserOperation,
			"status", status,
			"error", err)
	}
	var currentList string
	if reset {
		delete(cm.Data, status)
		delete(cm.Data, status+migrateStatusCountSuffix)
	} else {
		currentList = cm.Data[status]
		if currentList == "" {
			currentList = currentList + user
		} else {
			currentList = currentList + "," + user
		}
		count := strconv.Itoa(len(strings.Split(currentList, ",")))
		cm.Data[status+migrateStatusCountSuffix] = count
		cm.Data[status] = currentList
	}

	cm.Data[migrationStatusLastUpdate] = metav1.Now().Format(time.RFC3339)
	if _, err := sc.Core.ConfigMaps(activedirectory.StatusConfigMapNamespace).Update(cm); err != nil {
		if err != nil {
			log.Error("Unable to update migration status configmap",
				"operation", migrateAdUserOperation,
				"error", err)
		}
	}
	err = updateADConfigMigrationStatus(cm.Data, sc)
	if err != nil {
		log.Error("Unable to update AuthConfig status",
			"operation", migrateAdUserOperation,
			"error", err)
	}
}
