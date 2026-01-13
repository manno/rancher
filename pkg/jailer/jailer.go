package jailer

import (
	"context"
	"fmt"
	"os"
	"os/exec"
	"os/user"
	"path"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/pkg/errors"
	"github.com/rancher/rancher/pkg/log"
	"github.com/rancher/rancher/pkg/settings"
)

const BaseJailPath = "/opt/jail"

// JailUser is the non-root user's username used for executing commands within the Linux jail.
// This user should already exist, having been created during the image build process
// (refer to "useradd rancher" in the package/Dockerfile).
const JailUser = "rancher"

// JailGroup is the name of the user group that should have access to all files and directories
// within the jail directory.
const JailGroup = "jail-accessors"

var lock = sync.Mutex{}

// CreateJail sets up the named directory for use with chroot
func CreateJail(name string) error {
	if os.Getenv("CATTLE_DEV_MODE") != "" {
		return os.MkdirAll(path.Join(BaseJailPath, name), 0700)
	}

	log.Debug("Createjail called", "operation", "create_jail", "name", name)
	lock.Lock()
	defer lock.Unlock()

	jailPath := path.Join(BaseJailPath, name)

	log.Debug("Createjail jailpath determined", "operation", "create_jail", "jail_path", jailPath)
	// Check for the done file, if that exists the jail is ready to be used
	_, err := os.Stat(path.Join(jailPath, "done"))
	if err == nil {
		log.Debug("Createjail done file found, jail is ready", "operation", "create_jail", "done_file_path", path.Join(jailPath, "done"))
		return nil
	}

	// If the base dir exists without the done file rebuild the directory
	_, err = os.Stat(jailPath)
	if err == nil {
		log.Debug("Createjail basedir for jail exists but no done file found, removing jailpath", "operation", "create_jail", "jail_path", jailPath)
		if err := os.RemoveAll(jailPath); err != nil {
			return err
		}

	}

	t := settings.JailerTimeout.Get()
	timeout, err := strconv.Atoi(t)
	if err != nil {
		timeout = 60
		log.Warn("Error converting jailer-timeout setting to int, using default of 60 seconds", "operation", "create_jail", "error", err)
	}

	log.Debug("Createjail running script to create jail", "operation", "create_jail", "name", name)

	ctx, cancel := context.WithTimeout(context.Background(), time.Duration(timeout)*time.Second)
	defer cancel()

	cmd := exec.CommandContext(ctx, "/usr/bin/jailer.sh", name)
	out, err := cmd.CombinedOutput()
	if len(out) > 0 {
		log.Trace("Createjail output from jail script", "operation", "create_jail", "name", name, "output", string(out))
	} else {
		log.Trace("Createjail no output from jail script", "operation", "create_jail", "name", name)
	}
	if err != nil {
		if strings.HasSuffix(err.Error(), "signal: killed") {
			return errors.WithMessage(err, "error running the jail command: timed out waiting for the script to complete")
		}
		return errors.WithMessage(err, "error running the jail command")
	}
	return nil
}

func getWhitelistedEnvVars(envvars []string) []string {
	settings.IterateWhitelistedEnvVars(func(name, value string) {
		envvars = append(envvars, name+"="+value)
	})
	return envvars
}

// SetJailOwnership will ensure that the file/dir at `path` is owned by jailed user and group.
func SetJailOwnership(path string) error {
	uid, err := getUserID(JailUser)
	if err != nil {
		return fmt.Errorf("error finding UID for user %s: %w", JailUser, err)
	}

	gid, err := getGroupID(JailGroup)
	if err != nil {
		return fmt.Errorf("error finding GID for group %s: %w", JailGroup, err)
	}

	if err = os.Chown(path, uid, gid); err != nil {
		return fmt.Errorf("error changing ownership of %s: %w", path, err)
	}

	return nil
}

// getUserID returns the user ID of the given username.
func getUserID(userName string) (int, error) {
	u, err := user.Lookup(userName)
	if err != nil {
		return 0, fmt.Errorf("error getting user %s: %w", userName, err)
	}

	uid, err := strconv.Atoi(u.Uid)
	if err != nil {
		return 0, fmt.Errorf("error parsing user ID: %w", err)
	}

	return uid, nil
}

// getGroupID returns the group ID of the given group name.
func getGroupID(groupName string) (int, error) {
	group, err := user.LookupGroup(groupName)
	if err != nil {
		return 0, fmt.Errorf("error getting gid for group %s: %w", groupName, err)
	}

	gid, err := strconv.Atoi(group.Gid)
	if err != nil {
		return 0, fmt.Errorf("error parsing group ID %s: %w", group.Gid, err)
	}

	return gid, nil
}
