# Logging Field Naming Conventions

This document establishes consistent field names for structured logging across Rancher Manager.

## General Principles

1. Use snake_case for field names
2. Use consistent names across the codebase
3. Keep names short but descriptive
4. Avoid abbreviations unless widely understood

## Standard Field Names

### Resources
- `cluster_name` - Name of a cluster
- `cluster_id` - ID of a cluster
- `namespace` - Kubernetes namespace
- `resource_name` - Generic resource name
- `resource_type` - Type of Kubernetes resource
- `node_name` - Name of a node
- `pod_name` - Name of a pod
- `deployment_name` - Name of a deployment

### Identifiers
- `user_id` - User identifier
- `username` - User name
- `token_name` - Token identifier
- `request_id` - Request trace ID
- `correlation_id` - Correlation ID for distributed tracing

### Operations
- `operation` - Operation being performed (e.g., "create", "update", "delete")
- `component` - Component name (e.g., "clusterdeploy", "auth")
- `controller` - Controller name
- `handler` - Handler name
- `attempt` - Attempt number (for retries)

### Values
- `error` - Error message
- `reason` - Reason for an action
- `changed` - Boolean indicating if something changed
- `count` - Count of items
- `duration_ms` - Duration in milliseconds

### Configuration
- `image` - Container image
- `version` - Version string
- `old_value` - Previous value (for updates)
- `new_value` - New value (for updates)

## Examples

```go
// Before (logrus)
logrus.Infof("Cluster %s deployment started", clusterName)

// After (slog) - Simple message
log.Info("cluster deployment started", "cluster_name", clusterName)

// Before (logrus)
logrus.Debugf("clusterDeploy: deployAgent: successfully applied agent YAML for cluster [%s], try #%d", cluster.Name, i+1)

// After (slog) - Structured
log.Debug("agent YAML applied successfully",
    "cluster_name", cluster.Name,
    "attempt", i+1)

// Before (logrus)
logrus.Infof("clusterDeploy: redeployAgent: redeploy Rancher agents due to downstream agent image mismatch for [%s]: was [%s] and will be [%s]",
    cluster.Name, oldImage, newImage)

// After (slog) - Structured
log.Info("redeploying agents due to image mismatch",
    "cluster_name", cluster.Name,
    "old_value", oldImage,
    "new_value", newImage,
    "reason", "image_mismatch")
```
