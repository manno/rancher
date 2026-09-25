package git

import (
	"crypto/sha256"
	"encoding/hex"
	"io"
	"os"
	"path/filepath"
	"testing"

	"github.com/rancher/wrangler/v3/pkg/schemas/validation"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	chart "helm.sh/helm/v4/pkg/chart/v2"
	repo "helm.sh/helm/v4/pkg/repo/v1"
)

func Test_localChart(t *testing.T) {
	const gitURL = "https://git.rancher.io/charts"
	content := []byte("chart-bytes")
	sum := sha256.Sum256(content)
	digest := hex.EncodeToString(sum[:])

	dir := t.TempDir()
	require.NoError(t, os.MkdirAll(filepath.Join(dir, "assets", "fleet"), 0o755))
	require.NoError(t, os.WriteFile(filepath.Join(dir, "assets", "fleet", "fleet-108.0.0+up0.14.0.tgz"), content, 0o644))

	chartVersion := func(name, version, digest string) *repo.ChartVersion {
		return &repo.ChartVersion{
			Metadata: &chart.Metadata{Name: name, Version: version},
			Digest:   digest,
			URLs:     []string{"oci://registry.suse.com/rancher/charts/" + name + ":" + version},
		}
	}

	tests := []struct {
		name         string
		chartVersion *repo.ChartVersion
		wantErr      bool
		wantNotFound bool
	}{
		{name: "found without digest", chartVersion: chartVersion("fleet", "108.0.0+up0.14.0", "")},
		{name: "found with matching digest", chartVersion: chartVersion("fleet", "108.0.0+up0.14.0", digest)},
		{name: "digest mismatch", chartVersion: chartVersion("fleet", "108.0.0+up0.14.0", "deadbeef"), wantErr: true},
		{name: "missing version", chartVersion: chartVersion("fleet", "108.0.1+up0.14.1", ""), wantErr: true, wantNotFound: true},
		{name: "path traversal", chartVersion: chartVersion("../../etc", "1.0.0", ""), wantErr: true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			rc, err := localChart(dir, gitURL, tt.chartVersion)
			if tt.wantErr {
				assert.Error(t, err)
				if tt.wantNotFound {
					assert.ErrorIs(t, err, validation.NotFound)
				}
				return
			}
			require.NoError(t, err)
			defer rc.Close()
			got, err := io.ReadAll(rc)
			require.NoError(t, err)
			assert.Equal(t, content, got)
		})
	}
}
