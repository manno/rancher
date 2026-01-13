package log

import (
	"io"
	"log/slog"
	"testing"
)

// BenchmarkInfo measures the performance of Info logging
func BenchmarkInfo(b *testing.B) {
	Init("text", "info", io.Discard)
	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			Info("benchmark message", "key", "value")
		}
	})
}

// BenchmarkTypedAttributes measures performance with typed attributes
func BenchmarkTypedAttributes(b *testing.B) {
	Init("text", "info", io.Discard)
	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			Info("benchmark message",
				slog.String("key", "value"),
				slog.Int("count", 42))
		}
	})
}

// BenchmarkDebugDisabled measures the performance when debug is disabled
func BenchmarkDebugDisabled(b *testing.B) {
	Init("text", "info", io.Discard)
	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			Debug("this message should not be logged", "key", "value")
		}
	})
}

// BenchmarkStructuredLogging measures structured logging performance
func BenchmarkStructuredLogging(b *testing.B) {
	Init("json", "info", io.Discard)
	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			Info("operation completed",
				"cluster_name", "test-cluster",
				"operation", "deploy",
				"count", 42,
				"success", true)
		}
	})
}

// BenchmarkContextLogger measures context logger performance
func BenchmarkContextLogger(b *testing.B) {
	Init("text", "info", io.Discard)
	contextLogger := L().With(
		"cluster_name", "test-cluster",
		"namespace", "default")
	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			contextLogger.Info("operation", "action", "deploy")
		}
	})
}

// BenchmarkTrace measures trace logging performance
func BenchmarkTrace(b *testing.B) {
	Init("text", "trace", io.Discard)
	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			Trace("trace message", "key", "value")
		}
	})
}
