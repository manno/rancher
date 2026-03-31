package clusterdeploy

import (
	"bytes"
	"context"
	"fmt"
	"reflect"
	"regexp"
	"strings"
	"sync"
	"time"

	"github.com/rancher/rancher/pkg/capr"
	"github.com/rancher/rancher/pkg/namespace"

	"github.com/pkg/errors"
	"github.com/rancher/norman/types"
	apimgmtv3 "github.com/rancher/rancher/pkg/apis/management.cattle.io/v3"
	"github.com/rancher/rancher/pkg/auth/tokens"
	util "github.com/rancher/rancher/pkg/cluster"
	"github.com/rancher/rancher/pkg/clustermanager"
	"github.com/rancher/rancher/pkg/controllers/managementuser/healthsyncer"
	rancherFeatures "github.com/rancher/rancher/pkg/features"
	v1 "github.com/rancher/rancher/pkg/generated/norman/core/v1"
	v3 "github.com/rancher/rancher/pkg/generated/norman/management.cattle.io/v3"
	"github.com/rancher/rancher/pkg/image"
	"github.com/rancher/rancher/pkg/kubectl"
	"github.com/rancher/rancher/pkg/log"
	"github.com/rancher/rancher/pkg/settings"
	"github.com/rancher/rancher/pkg/systemaccount"
	"github.com/rancher/rancher/pkg/systemtemplate"
	"github.com/rancher/rancher/pkg/taints"
	"github.com/rancher/rancher/pkg/types/config"
	"github.com/rancher/rancher/pkg/user"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/apimachinery/pkg/runtime"
	clientcmdapi "k8s.io/client-go/tools/clientcmd/api"
)

const (
	AgentForceDeployAnn = "io.cattle.agent.force.deploy"
	clusterImage        = "clusterImage"
)

var ErrCantConnectToAPI = errors.New("cannot connect to the cluster's Kubernetes API")

var (
	agentImagesMutex sync.RWMutex
	agentImages      = map[string]map[string]string{
		clusterImage: {},
	}
	controlPlaneTaintsMutex sync.RWMutex
	controlPlaneTaints      = make(map[string][]corev1.Taint)
	controlPlaneLabels      = map[string]string{
		"node-role.kubernetes.io/controlplane":  "true",
		"node-role.kubernetes.io/control-plane": "true",
	}
)

func Register(ctx context.Context, management *config.ManagementContext, clusterManager *clustermanager.Manager) {
	c := &clusterDeploy{
		mgmt:                 management,
		systemAccountManager: systemaccount.NewManager(management),
		userManager:          management.UserManager,
		clusters:             management.Management.Clusters(""),
		nodeLister:           management.Management.Nodes("").Controller().Lister(),
		clusterManager:       clusterManager,
		secretLister:         management.Core.Secrets("").Controller().Lister(),
		ctx:                  ctx,
	}

	management.Management.Clusters("").AddHandler(ctx, "cluster-deploy", c.sync)
}

type clusterDeploy struct {
	systemAccountManager *systemaccount.Manager
	userManager          user.Manager
	clusters             v3.ClusterInterface
	clusterManager       *clustermanager.Manager
	mgmt                 *config.ManagementContext
	nodeLister           v3.NodeLister
	secretLister         v1.SecretLister
	ctx                  context.Context
}

func (cd *clusterDeploy) sync(key string, cluster *apimgmtv3.Cluster) (runtime.Object, error) {
	log.Trace("ClusterDeploy: sync called", "key", key)
	var (
		err, updateErr error
	)

	if cluster == nil || cluster.DeletionTimestamp != nil {
		// remove the system account user created for this cluster
		if err := cd.systemAccountManager.RemoveSystemAccount(key); err != nil {
			return nil, err
		}
		return nil, nil
	}

	original := cluster
	cluster = original.DeepCopy()

	err = cd.doSync(cluster)
	if cluster != nil && !reflect.DeepEqual(cluster, original) {
		log.Trace("ClusterDeploy: sync: cluster changed, calling Update", "cluster", cluster.Name)
		_, updateErr = cd.clusters.Update(cluster)
	}

	if err != nil {
		return nil, err
	}
	return nil, updateErr
}

func (cd *clusterDeploy) doSync(cluster *apimgmtv3.Cluster) error {
	log.Trace("ClusterDeploy: doSync called", "cluster", cluster.Name)

	if !apimgmtv3.ClusterConditionProvisioned.IsTrue(cluster) {
		log.Trace("ClusterDeploy: doSync: cluster is not yet provisioned", "cluster", cluster.Name)
		return nil
	}

	// Skip further work if the cluster's API is not reachable according to HealthSyncer criteria
	// Note that we don't check the cluster's ClusterConditionReady status here, as HealthSyncer is not running
	// prior deployment of the cluster agent
	uc, err := cd.clusterManager.UserContextNoControllersReconnecting(cluster.Name, false)
	if err != nil {
		return err
	}
	if err := healthsyncer.IsAPIUp(cd.ctx, uc.K8sClient.CoreV1().Namespaces()); err != nil {
		log.Trace("ClusterDeploy: doSync: cannot connect to API for cluster", "cluster", cluster.Name)
		return ErrCantConnectToAPI
	}

	nodes, err := cd.nodeLister.List(cluster.Name, labels.Everything())
	if err != nil {
		return err
	}
	log.Trace("ClusterDeploy: doSync: found nodes for cluster", "count", len(nodes), "cluster", cluster.Name)

	if len(nodes) == 0 {
		return nil
	}

	_, err = apimgmtv3.ClusterConditionSystemAccountCreated.DoUntilTrue(cluster, func() (runtime.Object, error) {
		log.Trace("ClusterDeploy: doSync: Creating SystemAccount for cluster", "cluster", cluster.Name)
		return cluster, cd.systemAccountManager.CreateSystemAccount(cluster)
	})
	if err != nil {
		return err
	}

	if cluster.Status.AgentImage != "" && !agentImagesCached(cluster.Name) {
		if err := cd.cacheAgentImages(cluster.Name); err != nil {
			return err
		}
	}

	if !controlPlaneTaintsCached(cluster.Name) {
		if err := cd.cacheControlPlaneTaints(cluster.Name); err != nil {
			return err
		}
	}

	err = cd.managePodDisruptionBudget(cluster)
	if err != nil {
		return err
	}

	err = cd.deployAgent(cluster)
	if err != nil {
		return err
	}

	return nil
}

// agentFeaturesChanged will treat a missing key as false. This means we only detect changes
// when we set a feature to true so we can't reliably set a feature to false that is enabled by default.
// This behavior makes adding new def false features not cause the agent to redeploy.
func agentFeaturesChanged(desired, actual map[string]bool) bool {
	for k, v := range desired {
		if actual[k] != v {
			return true
		}
	}

	for k, v := range actual {
		if desired[k] != v {
			return true
		}
	}

	return false
}

func redeployAgent(cluster *apimgmtv3.Cluster, desiredAgent, desiredAuth string, desiredFeatures map[string]bool, desiredTaints []corev1.Taint) bool {
	log.Trace("ClusterDeploy: redeployAgent called for cluster", "cluster", cluster.Name)
	if !apimgmtv3.ClusterConditionAgentDeployed.IsTrue(cluster) {
		return true
	}
	forceDeploy := cluster.Annotations[AgentForceDeployAnn] == "true"
	imageChange := cluster.Status.AgentImage != desiredAgent || cluster.Status.AuthImage != desiredAuth
	agentFeaturesChanged := agentFeaturesChanged(desiredFeatures, cluster.Status.AgentFeatures)

	if forceDeploy || imageChange || agentFeaturesChanged {
		log.Info("Redeploying Rancher agents",
			"cluster_name", cluster.Name,
			"force_deploy", forceDeploy,
			"image_changed", imageChange,
			"features_changed", agentFeaturesChanged)
		log.Trace("ClusterDeploy: redeployAgent",
			"currentAgentImage", cluster.Status.AgentImage, "desiredAgent", desiredAgent,
			"currentAuthImage", cluster.Status.AuthImage, "desiredAuth", desiredAuth,
			"currentAgentFeatures", cluster.Status.AgentFeatures, "desiredFeatures", desiredFeatures)
		return true
	}

	ca := getAgentImages(cluster.Name)
	if cluster.Status.AgentImage != ca {
		// downstream agent does not match, kick a redeploy with settings agent
		log.Info("Redeploying agents due to image mismatch",
			"cluster_name", cluster.Name,
			"old_value", cluster.Status.AgentImage,
			"new_value", image.ResolveWithCluster(settings.AgentImage.Get(), cluster),
			"reason", "agent_image_mismatch")
		clearAgentImages(cluster.Name)
		return true
	}

	// Taints/tolerations
	// Current control plane taints are cached for comparison
	currentTaints := getCachedControlPlaneTaints(cluster.Name)
	log.Trace("ClusterDeploy: redeployAgent taints", "cluster", cluster.Name, "currentTaints", currentTaints, "desiredTaints", desiredTaints)
	toAdd, toDelete := taints.GetToDiffTaints(currentTaints, desiredTaints)
	// Any change to current triggers redeploy
	if len(toAdd) > 0 || len(toDelete) > 0 {
		log.Info("Redeploying agents due to toleration mismatch",
			"cluster_name", cluster.Name,
			"old_value", currentTaints,
			"new_value", desiredTaints,
			"reason", "toleration_mismatch")
		// Clear cache to refresh
		clearControlPlaneTaints(cluster.Name)
		return true
	}

	if !reflect.DeepEqual(append(settings.DefaultAgentSettingsAsEnvVars(), cluster.Spec.AgentEnvVars...), cluster.Status.AppliedAgentEnvVars) {
		log.Info("Redeploying agents due to env vars mismatch",
			"cluster_name", cluster.Name,
			"old_value", cluster.Status.AppliedAgentEnvVars,
			"new_value", cluster.Spec.AgentEnvVars,
			"reason", "env_vars_mismatch")
		return true
	}

	if pdbChanged, _ := util.AgentSchedulingPodDisruptionBudgetChanged(cluster); pdbChanged {
		log.Info("Redeploying agent due to configuration change",
			"cluster_name", cluster.Name,
			"reason", "pdb_changed")
		return true
	}

	if util.AgentDeploymentCustomizationChanged(cluster) {
		log.Info("Redeploying agent due to configuration change",
			"cluster_name", cluster.Name,
			"reason", "deployment_customization_changed")
		return true
	}

	log.Trace("ClusterDeploy: redeployAgent: returning false for redeployAgent")

	return false
}

// managePodDisruptionBudget compares the apimgmtv3.Cluster status and spec to determine if the pod disruption budget
// configuration has been removed. If the configuration has been removed, a kubeConfig will be generated and used to
// delete the downstream object. The updated pod disruption budget will be recreated when applying the agent manifest in deployAgent.
// This is required to ensure that unwanted pod disruption budgets are not left behind in downstream clusters, as simply omitting
// the object from the agent manifest does not guarantee that the object will be removed.
func (cd *clusterDeploy) managePodDisruptionBudget(cluster *apimgmtv3.Cluster) error {
	_, pdbDeleted := util.AgentSchedulingPodDisruptionBudgetChanged(cluster)
	if !pdbDeleted {
		return nil
	}

	log.Debug("ClusterDeploy: Removing Pod Disruption Budget", "cluster", cluster.Name)

	pdbYaml, err := systemtemplate.PodDisruptionBudgetTemplate(cluster)
	if err != nil {
		return err
	}

	kubeConfig, tokenName, err := cd.getKubeConfig(cluster)
	if err != nil {
		return err
	}

	defer func() {
		if err := cd.mgmt.SystemTokens.DeleteToken(tokenName); err != nil {
			log.Error("Cleanup for clusterdeploy token failed, will not retry", "tokenName", tokenName, "error", err)
		}
	}()

	// Once the cluster agent manifest has been applied to an imported cluster
	// any resources later omitted from that manifest must be manually removed.
	o, err := kubectl.Delete(pdbYaml, kubeConfig)
	if err != nil {
		if !strings.Contains(string(o), fmt.Sprintf("\"%s\" not found", util.PodDisruptionBudgetName)) {
			return fmt.Errorf("could not delete existing pod disruption budget: %w", err)
		}
	}

	return nil
}

// managePriorityClass compares the apimgmtv3.Cluster status and spec to determine if the priority class configuration has changed.
// If the priority class configuration has not been updated, the function will exit early and return false booleans and a nil error. If a change is detected,
// a kubeConfig will be generated, the downstream priority class will be deleted and recreated, and a boolean indicating the type of update will be returned.
// If the priority class is being created or deleted, the agent deployment manifest must update the priorityClassName field accordingly.
func (cd *clusterDeploy) managePriorityClass(cluster *apimgmtv3.Cluster) (bool, bool, bool, error) {
	pcChanged, pcCreated, pcRemovedFromCluster := util.AgentSchedulingPriorityClassChanged(cluster)
	if !pcChanged && !pcRemovedFromCluster && !pcCreated {
		return pcChanged, pcCreated, pcRemovedFromCluster, nil
	}

	log.Debug("ClusterDeploy: Updating Priority Class", "cluster", cluster.Name, "pcChanged", pcChanged, "pcCreated", pcCreated, "pcRemoved", pcRemovedFromCluster)

	pcYaml, err := systemtemplate.PriorityClassTemplate(cluster)
	if err != nil {
		return pcChanged, pcCreated, pcRemovedFromCluster, fmt.Errorf("could not create priority class yaml: %w", err)
	}

	kubeConfig, tokenName, err := cd.getKubeConfig(cluster)
	if err != nil {
		return pcChanged, pcCreated, pcRemovedFromCluster, err
	}

	defer func() {
		if err := cd.mgmt.SystemTokens.DeleteToken(tokenName); err != nil {
			log.Error("Cleanup for clusterdeploy token failed, will not retry", "tokenName", tokenName, "error", err)
		}
	}()

	// priority classes are immutable, we need to completely delete and recreate
	// the object to update it
	o, err := kubectl.Delete(pcYaml, kubeConfig)
	if err != nil {
		if !strings.Contains(string(o), fmt.Sprintf("\"%s\" not found", util.PriorityClassName)) {
			return pcChanged, pcCreated, pcRemovedFromCluster, fmt.Errorf("could not delete existing priority class : %w", err)
		}
	}

	// if the feature has been disabled we should allow for the deletion of the PC, but not the recreation.
	if !pcRemovedFromCluster && rancherFeatures.ClusterAgentSchedulingCustomization.Enabled() {
		_, err = kubectl.Apply(pcYaml, kubeConfig)
		if err != nil {
			return pcChanged, pcCreated, pcRemovedFromCluster, fmt.Errorf("failed to create updated priority class: %w", err)
		}
	}

	return pcChanged, pcCreated, pcRemovedFromCluster, nil
}

// ensurePriorityClass inspects the apimgmtv3.Cluster object to determine if a priority class should exist in the downstream cluster.
// If the priority class has been configured on the cluster object, the provided kubeConfig will be used to query the object in the downstream cluster.
// If the downstream object is missing, it will be recreated using the priority class configuration set on the apimgmtv3.Cluster object.
// A boolean is returned to indicate if the priority class exists on the downstream cluster, as well as any errors encountered.
func (cd *clusterDeploy) ensurePriorityClass(cluster *apimgmtv3.Cluster, kubeConfig *clientcmdapi.Config) (bool, error) {
	pcEnabled, _ := util.AgentSchedulingCustomizationEnabled(cluster)
	if !pcEnabled {
		return false, nil
	}

	schedulingStatus := util.GetAgentSchedulingCustomizationStatus(cluster)
	if schedulingStatus == nil || schedulingStatus.PriorityClass == nil {
		return false, nil
	}

	log.Debug("ClusterDeploy: deployAgent: ensuring that downstream priority class exists", "cluster", cluster.Name)

	out, err := kubectl.GetNonNamespacedResource(kubeConfig, util.PriorityClassKind, util.PriorityClassName)
	if err == nil {
		return true, nil
	}

	if strings.Contains(string(out), "Error from server (NotFound)") {
		if rancherFeatures.ClusterAgentSchedulingCustomization.Enabled() {
			log.Debug("ClusterDeploy: deployAgent: recreating downstream priority class", "cluster", cluster.Name)
			pcYaml, err := systemtemplate.PriorityClassTemplate(cluster)
			if err != nil {
				return false, fmt.Errorf("clusterDeploy: could not create priority class yaml: %w", err)
			}
			_, err = kubectl.Apply(pcYaml, kubeConfig)
			if err != nil {
				return false, fmt.Errorf("clusterDeploy: failed to recreate priority class: %w", err)
			}
		}

		return true, nil
	}

	return false, fmt.Errorf("clusterDeploy: error encountered querying downstream priority class: %w", err)
}

func (cd *clusterDeploy) deployAgent(cluster *apimgmtv3.Cluster) error {
	if cluster.Spec.Internal {
		return nil
	}

	desiredAgent := systemtemplate.GetDesiredAgentImage(cluster)
	desiredAuth := systemtemplate.GetDesiredAuthImage(cluster)
	desiredFeatures := systemtemplate.GetDesiredFeatures(cluster)

	log.Trace("ClusterDeploy: deployAgent: desired features for cluster", "desiredFeatures", desiredFeatures, "cluster", cluster.Name)

	desiredTaints, err := cd.getControlPlaneTaints(cluster.Name)
	if err != nil {
		return err
	}
	log.Trace("ClusterDeploy: deployAgent: desired taints for cluster", "desiredTaints", desiredTaints, "cluster", cluster.Name)

	pcChanged, pcCreated, pcDeleted, err := cd.managePriorityClass(cluster)
	if err != nil {
		return err
	}

	shouldRedeployAgent := redeployAgent(cluster, desiredAgent, desiredAuth, desiredFeatures, desiredTaints)
	agentManifestChanged := shouldRedeployAgent || pcDeleted || pcCreated
	if !agentManifestChanged && !pcChanged {
		return nil
	}

	kubeConfig, tokenName, err := cd.getKubeConfig(cluster)
	if err != nil {
		return err
	}
	defer func() {
		if err := cd.mgmt.SystemTokens.DeleteToken(tokenName); err != nil {
			log.Error("Cleanup for clusterdeploy token failed, will not retry", "tokenName", tokenName, "error", err)
		}
	}()

	// if a user has only changed the priority class definition then the resulting cluster agent manifest
	// will not be different (as it only contains a reference to the priority class name, which would not change).
	// Due to this, kubectl.Apply will not roll out new instances of the agent with the adjusted priority value.
	// In this case we need to manually roll out the deployment. The only exception to this is if the PC is
	// being created for the first time or removed, at which point the reference needs to be updated.
	if !agentManifestChanged {
		log.Debug("ClusterDeploy: deployAgent: restarting rollout of cattle-cluster-agent deployment to apply updated priority class value")
		output, err := kubectl.RestartRolloutWithNamespace("cattle-system", "deployment/cattle-cluster-agent", kubeConfig)
		if err != nil {
			log.Error("ClusterDeploy: failed to rollout cattle-cluster-agent deployment", "output", string(output), "error", err)
			return err
		}
		util.UpdateAppliedAgentDeploymentCustomization(cluster)
		return nil
	}

	log.Trace("ClusterDeploy: deployAgent: detected a change in desired cluster agent manifest")

	// pcChanged and pcCreated indicate that 'managePriorityClass' function has recently recreated
	// the downstream PC, so there is no need to check a second time.
	pcExists := pcChanged || pcCreated
	if !pcExists {
		pcExists, err = cd.ensurePriorityClass(cluster, kubeConfig)
		if err != nil {
			log.Error("ClusterDeploy: deployAgent: failed to ensure priority class exists for cluster", "cluster", cluster.Name, "error", err)
			return err
		}
	}

	if _, err = apimgmtv3.ClusterConditionAgentDeployed.Do(cluster, func() (runtime.Object, error) {
		yaml, err := cd.getYAML(cluster, desiredAgent, desiredAuth, desiredFeatures, desiredTaints, pcExists)
		if err != nil {
			return cluster, err
		}
		log.Trace("ClusterDeploy: deployAgent: agent YAML", "yaml", string(yaml))
		var output []byte
		for i := 0; i < 5; i++ {
			// This will fail almost always the first time because when we create the namespace in the file it won't have privileges.
			// This allows for 5*5 seconds for the cluster to be ready to apply the agent YAML before erroring out
			log.Trace("ClusterDeploy: deployAgent: applying agent YAML for cluster", "cluster", cluster.Name, "try", i+1, "output", string(output))
			output, err = kubectl.Apply(yaml, kubeConfig)
			if err == nil {
				log.Debug("ClusterDeploy: deployAgent: successfully applied agent YAML for cluster", "cluster", cluster.Name, "try", i+1)
				break
			}
			log.Debug("ClusterDeploy: deployAgent: error while applying agent YAML for cluster", "cluster", cluster.Name, "try", i+1)
			time.Sleep(5 * time.Second)
		}
		if err != nil {
			return cluster, errors.WithMessage(types.NewErrors(err, errors.New(formatKubectlApplyOutput(string(output)))), "Error while applying agent YAML, it will be retried automatically")
		}
		apimgmtv3.ClusterConditionAgentDeployed.Message(cluster, string(output))
		if !cluster.Spec.LocalClusterAuthEndpoint.Enabled && cluster.Status.AppliedSpec.LocalClusterAuthEndpoint.Enabled && cluster.Status.AuthImage != "" {
			output, err = kubectl.Delete([]byte(systemtemplate.AuthDaemonSet), kubeConfig)
		}
		if err != nil {
			log.Trace("Output from kubectl delete kube-api-auth DaemonSet", "output", string(output), "error", err)
			// Ignore if the resource does not exist and it returns 'daemonsets.apps "kube-api-auth" not found'
			dsNotFoundError := "daemonsets.apps \"kube-api-auth\" not found"
			if !strings.Contains(string(output), dsNotFoundError) {
				return cluster, errors.WithMessage(types.NewErrors(err, errors.New(string(output))), "kubectl delete failed")
			}
			log.Debug("Ignored error during delete kube-api-auth DaemonSet", "error", dsNotFoundError)
		}
		apimgmtv3.ClusterConditionAgentDeployed.Message(cluster, string(output))
		return cluster, nil
	}); err != nil {
		return err
	}

	if err = cd.cacheAgentImages(cluster.Name); err != nil {
		return err
	}

	cluster.Status.AgentImage = desiredAgent
	cluster.Status.AgentFeatures = desiredFeatures
	if cluster.Spec.DesiredAgentImage == "fixed" {
		cluster.Spec.DesiredAgentImage = desiredAgent
	}
	cluster.Status.AuthImage = desiredAuth
	if cluster.Spec.DesiredAuthImage == "fixed" {
		cluster.Spec.DesiredAuthImage = desiredAuth
	}
	if cluster.Annotations[AgentForceDeployAnn] == "true" {
		cluster.Annotations[AgentForceDeployAnn] = "false"
	}

	cluster.Status.AppliedAgentEnvVars = append(settings.DefaultAgentSettingsAsEnvVars(), cluster.Spec.AgentEnvVars...)

	util.UpdateAppliedAgentDeploymentCustomization(cluster)

	return nil
}

func (cd *clusterDeploy) getKubeConfig(cluster *apimgmtv3.Cluster) (*clientcmdapi.Config, string, error) {
	log.Trace("ClusterDeploy: getKubeConfig called for cluster", "cluster", cluster.Name)
	systemUser, err := cd.systemAccountManager.GetSystemUser(cluster.Name)
	if err != nil {
		return nil, "", err
	}

	tokenPrefix := fmt.Sprintf("%s-%s", "agent", systemUser.Name)
	token, err := cd.mgmt.SystemTokens.EnsureSystemToken(tokenPrefix, "token for agent deployment", "agent", systemUser.Name, nil, true)
	if err != nil {
		return nil, "", err
	}

	tokenName, _ := tokens.SplitTokenParts(token)
	return cd.clusterManager.KubeConfig(cluster.Name, token), tokenName, nil
}

func (cd *clusterDeploy) getYAML(cluster *apimgmtv3.Cluster, agentImage, authImage string, features map[string]bool, taints []corev1.Taint, priorityClassExists bool) ([]byte, error) {
	log.Trace("ClusterDeploy: getYAML: Desired agent configuration for cluster",
		"agentImage", agentImage, "authImage", authImage, "features", features, "taints", taints, "cluster", cluster.Name)

	token, err := cd.systemAccountManager.GetOrCreateSystemClusterToken(cluster.Name)
	if err != nil {
		return nil, err
	}

	url := settings.ServerURL.Get()
	if url == "" {
		cd.clusters.Controller().EnqueueAfter("", cluster.Name, time.Second)
		return nil, fmt.Errorf("waiting for server-url setting to be set")
	}

	buf := &bytes.Buffer{}
	err = systemtemplate.SystemTemplate(buf, agentImage, authImage, cluster.Name,
		token, url, capr.PreBootstrap(cluster), cluster, features,
		taints, cd.secretLister, priorityClassExists, namespace.GetMutator())

	return buf.Bytes(), err
}

func (cd *clusterDeploy) getClusterAgentImage(name string) (string, error) {
	uc, err := cd.clusterManager.UserContextNoControllers(name)
	if err != nil {
		return "", err
	}

	d, err := uc.Apps.Deployments("cattle-system").Get("cattle-cluster-agent", metav1.GetOptions{})
	if err != nil {
		if !apierrors.IsNotFound(err) {
			return "", err
		}
		return "", nil
	}

	for _, c := range d.Spec.Template.Spec.Containers {
		if c.Name == "cluster-register" {
			return c.Image, nil
		}
	}

	return "", nil
}

func (cd *clusterDeploy) cacheAgentImages(name string) error {
	ca, err := cd.getClusterAgentImage(name)
	if err != nil {
		return err
	}

	agentImagesMutex.Lock()
	defer agentImagesMutex.Unlock()
	agentImages[clusterImage][name] = ca
	return nil
}

func agentImagesCached(name string) bool {
	ca := getAgentImages(name)
	return ca != ""
}

func controlPlaneTaintsCached(name string) bool {
	controlPlaneTaintsMutex.RLock()
	defer controlPlaneTaintsMutex.RUnlock()
	if _, ok := controlPlaneTaints[name]; ok {
		return true
	}
	return false
}

func getAgentImages(name string) string {
	agentImagesMutex.RLock()
	defer agentImagesMutex.RUnlock()
	return agentImages[clusterImage][name]
}

func getCachedControlPlaneTaints(name string) []corev1.Taint {
	controlPlaneTaintsMutex.RLock()
	defer controlPlaneTaintsMutex.RUnlock()
	if _, ok := controlPlaneTaints[name]; ok {
		return controlPlaneTaints[name]
	}
	return nil
}

func clearAgentImages(name string) {
	log.Trace("ClusterDeploy: clearAgentImages called for", "name", name)
	agentImagesMutex.Lock()
	defer agentImagesMutex.Unlock()
	delete(agentImages[clusterImage], name)
}

func clearControlPlaneTaints(name string) {
	log.Trace("ClusterDeploy: clearControlPlaneTaints called for", "name", name)
	controlPlaneTaintsMutex.Lock()
	defer controlPlaneTaintsMutex.Unlock()
	delete(controlPlaneTaints, name)
}

func (cd *clusterDeploy) cacheControlPlaneTaints(name string) error {
	taints, err := cd.getControlPlaneTaints(name)
	if err != nil {
		return err
	}

	controlPlaneTaintsMutex.Lock()
	defer controlPlaneTaintsMutex.Unlock()
	controlPlaneTaints[name] = taints
	return nil
}

func (cd *clusterDeploy) getControlPlaneTaints(name string) ([]corev1.Taint, error) {
	var allTaints []corev1.Taint
	var controlPlaneLabelFound bool
	nodes, err := cd.nodeLister.List(name, labels.Everything())
	if err != nil {
		return nil, err
	}
	log.Debug("ClusterDeploy: getControlPlaneTaints: Length of nodes for cluster", "cluster", name, "count", len(nodes))

	for _, node := range nodes {
		controlPlaneLabelFound = false
		// Filtering nodes for controlplane nodes based on labels
		for controlPlaneLabelKey, controlPlaneLabelValue := range controlPlaneLabels {
			if labelValue, ok := node.Status.NodeLabels[controlPlaneLabelKey]; ok {
				log.Trace("ClusterDeploy: getControlPlaneTaints: node has label key", "node", node.Status.NodeName, "labelKey", controlPlaneLabelKey)
				if labelValue == controlPlaneLabelValue {
					log.Trace("ClusterDeploy: getControlPlaneTaints: node has label key and value", "node", node.Status.NodeName, "labelKey", controlPlaneLabelKey, "labelValue", controlPlaneLabelValue)
					controlPlaneLabelFound = true
					break
				}
			}
		}
		if controlPlaneLabelFound {
			toAdd, _ := taints.GetToDiffTaints(allTaints, node.Spec.InternalNodeSpec.Taints)
			for _, taintStr := range toAdd {
				if !strings.HasPrefix(taintStr.Key, "node.kubernetes.io") {
					log.Debug("ClusterDeploy: getControlPlaneTaints: toAdd", "taints", toAdd)
					allTaints = append(allTaints, taintStr)
					continue
				}
				log.Trace("ClusterDeploy: getControlPlaneTaints: skipping k8s internal taint", "taint", taintStr)
			}
		}
	}
	log.Debug("ClusterDeploy: getControlPlaneTaints: allTaints", "taints", allTaints)

	return allTaints, nil
}

func formatKubectlApplyOutput(log string) string {
	// Strip newlines to compact output
	log = strings.Replace(log, "\n", " ", -1)
	// Strip token from output
	tokenRegex := regexp.MustCompile(`^.*?\"token\":\"(.*?)\"`)
	token := tokenRegex.FindStringSubmatch(log)
	if len(token) == 2 {
		log = strings.Replace(log, token[1], "REDACTED", 1)
	}
	return log
}
