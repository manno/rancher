package log

import (
	"context"
	"io"
	"log/slog"
	"os"
)

// Custom log levels
const (
	LevelTrace = slog.Level(-8) // Below Debug (-4)
	LevelFatal = slog.Level(12) // Above Error (8)
)

var (
	// logger is the global logger instance
	logger *slog.Logger
	// level tracks the current log level for dynamic changes
	// LevelVar is safe for concurrent use
	level = new(slog.LevelVar)
)

func init() {
	// Initialize with a default logger
	level.Set(slog.LevelInfo)
	logger = slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{
		Level: level,
		ReplaceAttr: replaceAttr,
	}))
}

// Init initializes the logger with the specified format and options
// This should be called once at startup before concurrent logging begins
func Init(format string, logLevel string, output io.Writer) {
	// Parse and set level
	parseAndSetLevel(logLevel)

	var handler slog.Handler
	opts := &slog.HandlerOptions{
		Level:       level,
		ReplaceAttr: replaceAttr,
	}

	switch format {
	case "json":
		handler = slog.NewJSONHandler(output, opts)
	case "text":
		handler = slog.NewTextHandler(output, opts)
	case "simple":
		handler = newSimpleHandler(output, opts)
	default:
		handler = newSimpleHandler(output, opts)
	}

	logger = slog.New(handler)
}

// replaceAttr maps custom levels to readable names
func replaceAttr(groups []string, a slog.Attr) slog.Attr {
	if a.Key == slog.LevelKey {
		lvl := a.Value.Any().(slog.Level)
		switch lvl {
		case LevelTrace:
			a.Value = slog.StringValue("TRACE")
		case LevelFatal:
			a.Value = slog.StringValue("FATAL")
		}
	}
	return a
}

// parseAndSetLevel sets the current log level from a string
func parseAndSetLevel(logLevel string) {
	switch logLevel {
	case "trace":
		level.Set(LevelTrace)
	case "debug":
		level.Set(slog.LevelDebug)
	case "info":
		level.Set(slog.LevelInfo)
	case "warn", "warning":
		level.Set(slog.LevelWarn)
	case "error":
		level.Set(slog.LevelError)
	default:
		level.Set(slog.LevelInfo)
	}
}

// SetLevel dynamically changes the log level
// Safe for concurrent use - LevelVar is thread-safe
func SetLevel(logLevel string) {
	parseAndSetLevel(logLevel)
}

// GetLevel returns the current log level as a string
func GetLevel() string {
	lvl := level.Level()
	switch lvl {
	case LevelTrace:
		return "trace"
	case slog.LevelDebug:
		return "debug"
	case slog.LevelInfo:
		return "info"
	case slog.LevelWarn:
		return "warn"
	case slog.LevelError:
		return "error"
	default:
		return "info"
	}
}

// L returns the global logger for advanced slog usage
// Use this when you need the logger instance directly (e.g., logger.With())
func L() *slog.Logger {
	return logger
}

// Debug logs a debug message with structured fields
func Debug(msg string, args ...any) {
	logger.Debug(msg, args...)
}

// Info logs an info message with structured fields
func Info(msg string, args ...any) {
	logger.Info(msg, args...)
}

// Warn logs a warning message with structured fields
func Warn(msg string, args ...any) {
	logger.Warn(msg, args...)
}

// Error logs an error message with structured fields
func Error(msg string, args ...any) {
	logger.Error(msg, args...)
}

// Trace logs a trace message (not available in standard slog)
// Use for very detailed debugging information
func Trace(msg string, args ...any) {
	logger.Log(context.Background(), LevelTrace, msg, args...)
}

// Fatal logs an error message and exits the program
// Use for unrecoverable errors only
func Fatal(msg string, args ...any) {
	logger.Log(context.Background(), LevelFatal, msg, args...)
	os.Exit(1)
}
