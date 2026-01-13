package log

import (
	"bytes"
	"log/slog"
	"strings"
	"testing"
)

func TestLogLevels(t *testing.T) {
	var buf bytes.Buffer
	Init("text", "debug", &buf)

	Debug("debug message", "key", "value")
	output := buf.String()

	if !strings.Contains(output, "debug message") {
		t.Errorf("Expected debug message in output, got: %s", output)
	}
	if !strings.Contains(output, "key") {
		t.Errorf("Expected key in output, got: %s", output)
	}
}

func TestLogFormats(t *testing.T) {
	tests := []struct {
		format string
		name   string
	}{
		
		{"text", "text format"},
		{"json", "json format"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var buf bytes.Buffer
			Init(tt.format, "info", &buf)

			Info("test message", "key", "value")
			output := buf.String()

			if !strings.Contains(output, "test message") {
				t.Errorf("Expected message in %s output", tt.format)
			}
		})
	}
}

func TestSetLevel(t *testing.T) {
	var buf bytes.Buffer
	Init("text", "warn", &buf)

	// Debug should not appear at warn level
	Debug("should not appear")
	if buf.Len() > 0 {
		t.Errorf("Debug message appeared at warn level")
	}

	// Change to debug level
	SetLevel("debug")

	buf.Reset()
	Debug("should appear")
	if buf.Len() == 0 {
		t.Errorf("Debug message did not appear after level change")
	}
}

func TestGetLevel(t *testing.T) {
	Init("text", "debug", &bytes.Buffer{})

	lvl := GetLevel()
	if lvl != "debug" {
		t.Errorf("Expected level 'debug', got: %s", lvl)
	}

	SetLevel("info")
	lvl = GetLevel()
	if lvl != "info" {
		t.Errorf("Expected level 'info', got: %s", lvl)
	}
}

func TestStructuredLogging(t *testing.T) {
	var buf bytes.Buffer
	Init("text", "info", &buf)

	Info("test message",
		"cluster_name", "test-cluster",
		"operation", "deploy",
		"count", 5)

	output := buf.String()

	if !strings.Contains(output, "cluster_name=test-cluster") {
		t.Errorf("Expected cluster_name field in output: %s", output)
	}
	if !strings.Contains(output, "operation=deploy") {
		t.Errorf("Expected operation field in output: %s", output)
	}
	if !strings.Contains(output, "count=5") {
		t.Errorf("Expected count field in output: %s", output)
	}
}

func TestTraceLevel(t *testing.T) {
	var buf bytes.Buffer
	Init("text", "trace", &buf)

	Trace("trace message", "key", "value")
	output := buf.String()

	if !strings.Contains(output, "trace message") {
		t.Errorf("Expected trace message in output: %s", output)
	}
	if !strings.Contains(output, "TRACE") {
		t.Errorf("Expected TRACE level in output: %s", output)
	}
}

func TestTypedAttributes(t *testing.T) {
	var buf bytes.Buffer
	Init("text", "info", &buf)

	Info("typed attributes",
		slog.String("cluster_name", "test"),
		slog.Int("count", 42),
		slog.Bool("success", true))

	output := buf.String()

	if !strings.Contains(output, "cluster_name=test") {
		t.Errorf("Expected cluster_name in output: %s", output)
	}
	if !strings.Contains(output, "count=42") {
		t.Errorf("Expected count in output: %s", output)
	}
	if !strings.Contains(output, "success=true") {
		t.Errorf("Expected success in output: %s", output)
	}
}

func TestContextLogger(t *testing.T) {
	var buf bytes.Buffer
	Init("text", "info", &buf)

	// Create a context logger with common fields
	contextLogger := L().With(
		"cluster_name", "test-cluster",
		"namespace", "default")

	contextLogger.Info("operation started", "operation", "deploy")

	output := buf.String()

	if !strings.Contains(output, "cluster_name=test-cluster") {
		t.Errorf("Expected cluster_name in output: %s", output)
	}
	if !strings.Contains(output, "namespace=default") {
		t.Errorf("Expected namespace in output: %s", output)
	}
	if !strings.Contains(output, "operation=deploy") {
		t.Errorf("Expected operation in output: %s", output)
	}
}
