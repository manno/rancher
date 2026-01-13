# Rancher Logging Package

A minimal wrapper around Go's `log/slog` package providing structured logging with a clean API.

## Design Philosophy

This package provides the **minimum necessary** over `log/slog`:

1. **Clean API** - Direct functions: `log.Info()`, `log.Error()`, etc.
2. **Forces structured logging** - No printf-style APIs
3. **Only adds what slog lacks** - `Trace` and `Fatal` levels
4. **Standard slog underneath** - Use `log.L()` when you need the logger directly

## Usage

### Standard Logging

```go
import "github.com/rancher/rancher/pkg/log"

// Simple structured logging
log.Info("cluster deployed",
    "cluster_name", cluster.Name,
    "namespace", namespace)

// With error
log.Error("deployment failed",
    "cluster_name", cluster.Name,
    "error", err)

// Different levels
log.Debug("checking prerequisites", "count", len(items))
log.Warn("deprecated feature used", "feature", featureName)
```

### Typed Attributes (Recommended)

Use `slog` typed attributes for better performance and type safety:

```go
import "log/slog"

log.Info("operation completed",
    slog.String("cluster_name", cluster.Name),
    slog.Int("count", 42),
    slog.Bool("success", true),
    slog.Duration("elapsed", elapsed))
```

### Context Loggers

Create a logger with common fields for related operations:

```go
// Create logger with cluster context
clusterLogger := log.L().With(
    "cluster_name", cluster.Name,
    "cluster_id", cluster.ClusterName)

// All logs from this logger include cluster fields
clusterLogger.Info("starting deployment")
clusterLogger.Debug("checking prerequisites")
clusterLogger.Error("deployment failed", "error", err)
```

### Special Levels

**Trace** - Very detailed debugging (not in standard slog):
```go
log.Trace("detailed debug info",
    "step", "validate",
    "data", debugData)
```

**Fatal** - Unrecoverable errors (logs and exits):
```go
if cfg.Required == "" {
    log.Fatal("required configuration missing",
        "config_key", "required")
}
// Program exits with code 1
```

## API Reference

### Direct Logging Functions

- `log.Debug(msg, args...)` - Debug level
- `log.Info(msg, args...)` - Info level
- `log.Warn(msg, args...)` - Warning level
- `log.Error(msg, args...)` - Error level
- `log.Trace(msg, args...)` - Trace level (custom)
- `log.Fatal(msg, args...)` - Fatal level (logs and exits)

### Advanced Usage

- `log.L()` - Get the underlying `*slog.Logger` for advanced operations like `With()`

## Configuration

### Initialization

Called once at startup:

```go
log.Init(format, level, output)
```

**Parameters:**
- `format`: `"simple"`, `"text"`, or `"json"`
- `level`: `"trace"`, `"debug"`, `"info"`, `"warn"`, `"error"`
- `output`: `io.Writer` (usually `os.Stdout`)

**Environment Variables (Agent):**
- `CATTLE_TRACE` / `RANCHER_TRACE` - Enable trace level
- `CATTLE_DEBUG` / `RANCHER_DEBUG` - Enable debug level

**CLI Flags (Manager):**
- `--log-format` - Set output format
- `--debug` - Enable debug level
- `--trace` - Enable trace level

### Dynamic Level Changes

Runtime level changes via Unix socket:

```bash
# Get current level
curl --unix-socket /tmp/log.sock http://localhost/v1/loglevel

# Set level to debug
curl --unix-socket /tmp/log.sock -X POST -d "level=debug" http://localhost/v1/loglevel
```

Or programmatically:

```go
log.SetLevel("debug")
level := log.GetLevel() // Returns "debug"
```

## Field Naming Conventions

Use consistent field names (see `CONVENTIONS.md`):

| Field Name | Usage |
|------------|-------|
| `cluster_name` | Cluster name |
| `namespace` | Kubernetes namespace |
| `operation` | Operation type |
| `error` | Error message |
| `old_value` | Previous value |
| `new_value` | New value |
| `reason` | Reason for action |

## Examples

### Simple Operation

```go
log.Info("starting server", "port", 8080)
```

### Operation with Error

```go
if err := deploy(cluster); err != nil {
    log.Error("deployment failed",
        "cluster_name", cluster.Name,
        "error", err)
    return err
}
```

### Complex Operation with Context

```go
logger := log.L().With(
    "cluster_name", cluster.Name,
    "operation", "agent_redeploy")

logger.Info("checking if redeploy needed",
    "force_deploy", forceDeploy,
    "image_changed", imageChanged)

if needsRedeploy {
    logger.Info("redeploying agents",
        "old_value", oldImage,
        "new_value", newImage,
        "reason", "image_mismatch")
}

logger.Info("redeploy complete", "success", true)
```

### Trace Debugging

```go
log.Trace("detailed state",
    "cluster_status", cluster.Status,
    "desired_state", desiredState)
```

## What This Package Provides

✅ **Clean API** - `log.Info()`, `log.Error()`, etc.
✅ **Trace level** - Not in standard slog
✅ **Fatal level** - Logs and exits
✅ **Configuration helpers** - `Init()`, `SetLevel()`, `GetLevel()`
✅ **Direct logger access** - `log.L()` for advanced usage

## What This Package Does NOT Provide

❌ Printf-style logging (`Infof`, `Debugf`) - Use structured logging
❌ `WithFields` map API - Use `log.L().With()` directly
❌ Non-standard slog patterns - Keeps you on the standard path

## Benefits

1. **Clean syntax** - No `.L()` needed for most calls
2. **Minimal abstraction** - Thin layer over slog
3. **Future-proof** - Easy to remove wrapper if desired
4. **No overhead** - Direct function calls to underlying logger
