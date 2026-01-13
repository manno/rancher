package nodedriver

import (
	"bytes"
	"io"
	"strings"
	"testing"

	"github.com/rancher/rancher/pkg/log"
	"github.com/stretchr/testify/assert"
)

func TestParseKeyValueString(t *testing.T) {
	testCases := []struct {
		name               string
		input              string
		expectedResult     map[string]string
		expectedLog        string
		expectedLogEntries int
		expectedLogLevel   string // "debug" or "error"
	}{
		{
			name:  "valid key-value pair",
			input: "userdata:userdata,cloudConfig:cloud-config",
			expectedResult: map[string]string{
				"userdata":    "userdata",
				"cloudConfig": "cloud-config",
			},
		},
		{
			name:  "key-value pairs with extra space",
			input: "userdata: userdata, cloudConfig:cloud-config",
			expectedResult: map[string]string{
				"userdata":    "userdata",
				"cloudConfig": "cloud-config",
			},
		},
		{
			name:               "empty pair",
			input:              "",
			expectedResult:     map[string]string{},
			expectedLog:        "Empty input string",
			expectedLogEntries: 1,
			expectedLogLevel:   "debug",
		},
		{
			name:               "empty key",
			input:              ":cloud-config",
			expectedResult:     map[string]string{},
			expectedLog:        "failed to parse pair: \":cloud-config\" (expected key:value)",
			expectedLogEntries: 1,
			expectedLogLevel:   "error",
		},
		{
			name:               "invalid pair",
			input:              "userdata:cloudConfig:cloud-config",
			expectedResult:     map[string]string{},
			expectedLog:        "failed to parse pair: \"userdata:cloudConfig:cloud-config\" (expected key:value)",
			expectedLogEntries: 1,
			expectedLogLevel:   "error",
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			var buf bytes.Buffer
			originalLevel := log.GetLevel()
			// Set level to debug to capture both debug and error logs
			log.Init("text", "debug", &buf)
			defer log.Init("text", originalLevel, io.Discard)

			annotations := ParseKeyValueString(tc.input)
			assert.Equal(t, tc.expectedResult, annotations)

			output := buf.String()
			if tc.expectedLog != "" {
				assert.Contains(t, output, tc.expectedLog, "expected log message not found")
				// Check for log level in output
				levelUpper := strings.ToUpper(tc.expectedLogLevel)
				assert.True(t, strings.Contains(output, levelUpper),
					"expected log level %s not found in output: %s", levelUpper, output)
			} else {
				assert.Empty(t, output, "expected no log entries but got: %s", output)
			}
		})
	}
}
