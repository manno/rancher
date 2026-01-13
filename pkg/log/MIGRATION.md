# Rancher Manager: Structured Logging Migration

Migration from logrus to log/slog with a clean, minimal API.

## Important: Audit Logger Exception

**DO NOT convert the audit logger itself** (`pkg/auth/audit/writer.go` and audit log entries). The audit logger writes structured audit trail logs to files and is a separate logging system from operational logging. 

Only convert operational logging calls (errors, debug messages) in files like `pkg/auth/audit/handler.go` and `pkg/auth/audit/middleware.go`. These are for troubleshooting the audit middleware, not for the audit trail itself.

## Design: Clean API with Minimal Wrapper

**What the package provides:**
- `log.Info()`, `log.Error()`, `log.Debug()`, `log.Warn()` - Structured logging
- `log.Trace()` - Trace level (not in slog)
- `log.Fatal()` - Fatal level (logs and exits)
- `log.L()` - Get `*slog.Logger` for advanced usage (e.g., `With()`)
- `log.Init()`, `log.SetLevel()`, `log.GetLevel()` - Configuration

**What it does NOT provide:**
- ❌ No `Infof()`, `Debugf()` - Forces structured logging
- ❌ No `WithFields(map)` - Use `log.L().With()` instead
- ❌ No printf-style APIs - Must use structured logging

## Migration Pattern

### Old (Logrus)

```go
import "github.com/rancher/rancher/pkg/log"

logrus.Infof("Cluster %s is ready", name)
logrus.Errorf("Failed to deploy: %v", err)

logrus.WithFields(logrus.Fields{
    "cluster": name,
    "count": 5,
}).Info("deployed")
```

### New (Clean API)

```go
import "github.com/rancher/rancher/pkg/log"

// Direct structured logging
log.Info("cluster is ready", "cluster_name", name)
log.Error("failed to deploy", "error", err)

// Context logger for related operations
logger := log.L().With("cluster_name", name, "count", 5)
logger.Info("deployed")

// Special cases
log.Trace("detailed debug", "data", debugInfo)
log.Fatal("unrecoverable error", "reason", reason)
```

## Systematic Conversion Process

For each package with logrus imports:

1. **Replace import**
   ```go
   // Old
   import "github.com/rancher/rancher/pkg/log"

   // New
   import "github.com/rancher/rancher/pkg/log"
   ```

2. **Convert log statements**
   ```bash
   # These need manual conversion to structured format:
   logrus.Infof(fmt, args...)  → log.Info(msg, "key", val, ...)
   logrus.Debugf(fmt, args...) → log.Debug(msg, "key", val, ...)
   logrus.Errorf(fmt, args...) → log.Error(msg, "key", val, ...)
   logrus.Warnf(fmt, args...)  → log.Warn(msg, "key", val, ...)

   # Special cases:
   logrus.Tracef(fmt, args...) → log.Trace(msg, "key", val, ...)
   logrus.Fatal(args...)       → log.Fatal(msg, "key", val, ...)
   ```

3. **Convert to structured logging**
   ```go
   // Before: printf-style
   logrus.Infof("Deploying cluster %s to namespace %s", cluster, ns)

   // After: structured
   log.Info("deploying cluster",
       "cluster_name", cluster,
       "namespace", ns)
   ```

4. **Build and test**
   ```bash
   go build ./pkg/[package]/...
   go test ./pkg/[package]/...
   ```

## Field Naming Standards

Use consistent field names (see `CONVENTIONS.md`):

- `cluster_name`, `cluster_id` - Cluster identifiers
- `namespace` - Kubernetes namespace
- `operation` - Operation being performed
- `error` - Error messages
- `old_value`, `new_value` - For comparisons
- `reason` - Why an action was taken
- `count`, `attempt` - Counters
- `changed` - Boolean flags

## Examples from Actual Code

### Example 1: Simple Info Log

```go
// Before
logrus.Infof("Rancher version %s is starting", version)

// After
log.Info("Rancher is starting", "version", version)
```

### Example 2: Error with Context

```go
// Before
logrus.Errorf("Could not connect to %s: %v", server, err)

// After
log.Error("could not connect to server",
    "server", server,
    "error", err)
```

### Example 3: Complex Operation

```go
// Before
logrus.Infof("Redeploy agents for %s: image changed from %s to %s",
    cluster.Name, oldImage, newImage)

// After
log.Info("redeploying agents due to image mismatch",
    "cluster_name", cluster.Name,
    "old_value", oldImage,
    "new_value", newImage,
    "reason", "image_mismatch")
```

### Example 4: Context Logger

```go
// Before (repeated cluster name)
logrus.Infof("Starting deployment for %s", cluster.Name)
logrus.Debugf("Checking prerequisites for %s", cluster.Name)
logrus.Infof("Deployment complete for %s", cluster.Name)

// After (cluster name in context)
logger := log.L().With("cluster_name", cluster.Name)
logger.Info("starting deployment")
logger.Debug("checking prerequisites")
logger.Info("deployment complete")
```

## Testing After Migration

1. **Unit tests**
   ```bash
   go test ./pkg/...
   ```

2. **Build verification**
   ```bash
   go build -o /dev/null ./pkg/...
   go build -o /dev/null ./cmd/...
   ```

3. **Log format verification**
   ```bash
   # Text format
   ./rancher --log-format=text --debug

   # JSON format
   ./rancher --log-format=json
   ```

4. **Dynamic level changes**
   ```bash
   curl --unix-socket /tmp/log.sock http://localhost/v1/loglevel
   curl --unix-socket /tmp/log.sock -X POST -d "level=debug" http://localhost/v1/loglevel
   ```

## Benefits of This Approach

1. **Clean, simple API** - Just `log.Info()`, no `.L()` needed
2. **Forces structured logging** - No printf escape hatch
3. **Minimal abstraction** - ~150 lines in log.go
4. **Easy to understand** - No hidden magic
5. **Zero overhead** - Direct function calls, no locks

## Common Pitfalls to Avoid

### ❌ Don't use printf-style formatting

```go
// Wrong - loses structure
log.Info(fmt.Sprintf("Deployed %s to %s", cluster, ns))

// Right - structured data
log.Info("deployed cluster",
    "cluster_name", cluster,
    "namespace", ns)
```

### ❌ Don't put structured data in the message

```go
// Wrong - hard to parse
log.Info("cluster=test-cluster operation=deploy")

// Right - separate fields
log.Info("operation started",
    "cluster_name", "test-cluster",
    "operation", "deploy")
```

### ✅ Do use context loggers for related operations

```go
// Good - common fields in context
logger := log.L().With(
    "cluster_name", cluster.Name,
    "operation", "deploy")

logger.Info("started")
logger.Debug("checking prerequisites")
logger.Info("completed")
```
