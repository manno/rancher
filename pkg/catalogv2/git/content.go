package git

import (
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"os"
	"path"
	"path/filepath"
	"strings"

	"github.com/rancher/rancher/pkg/catalogv2/chart"
	"github.com/rancher/wrangler/v3/pkg/schemas/validation"
	repo "helm.sh/helm/v4/pkg/repo/v1"
)

// Icon will return the icon for a chartName version in a local repository by getting the relative path
func Icon(namespace, name, gitURL string, chartVersion *repo.ChartVersion) (io.ReadCloser, string, error) {
	if len(chartVersion.Icon) == 0 {
		return nil, "", fmt.Errorf("failed to find chartName %s version %s: %w", chartVersion.Name, chartVersion.Version, validation.NotFound)
	}

	dir := RepoDir(namespace, name, gitURL)
	icon := chartVersion.Icon

	file, err := relative(dir, gitURL, icon)
	if err != nil {
		return nil, "", err
	}

	f, err := os.Open(file)
	return f, path.Ext(file), err
}

func Chart(namespace, name, gitURL string, chartVersion *repo.ChartVersion) (io.ReadCloser, error) {
	dir := RepoDir(namespace, name, gitURL)

	if len(chartVersion.URLs) == 0 {
		return nil, fmt.Errorf("failed to find chartName %s version %s: %w", chartVersion.Name, chartVersion.Version, validation.NotFound)
	}

	file, err := relative(dir, gitURL, chartVersion.URLs[0])
	if err != nil {
		return nil, err
	}

	archive, ok, err := chart.LoadArchive(file)
	if err != nil {
		return nil, err
	} else if !ok {
		return nil, fmt.Errorf("failed to find chartName %s version %s: %w", chartVersion.Name, chartVersion.Version, validation.NotFound)
	}

	return archive.Open()
}

// LocalChart will return a chart version from a local repository when its index entry points at a
// remote URL (e.g. oci://). The remote URL can't be mapped to a file, so the chart is looked up at
// the conventional assets/<name>/<name>-<version>.tgz path instead. If the index entry has a digest,
// the local tarball must match it.
func LocalChart(namespace, name, gitURL string, chartVersion *repo.ChartVersion) (io.ReadCloser, error) {
	return localChart(RepoDir(namespace, name, gitURL), gitURL, chartVersion)
}

func localChart(dir, gitURL string, chartVersion *repo.ChartVersion) (io.ReadCloser, error) {
	assetPath := path.Join("assets", chartVersion.Name, fmt.Sprintf("%s-%s.tgz", chartVersion.Name, chartVersion.Version))
	file, err := relative(dir, gitURL, assetPath)
	if err != nil {
		return nil, err
	}

	f, err := os.Open(file)
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return nil, fmt.Errorf("failed to find chartName %s version %s: %w", chartVersion.Name, chartVersion.Version, validation.NotFound)
		}
		return nil, err
	}

	if chartVersion.Digest == "" {
		return f, nil
	}

	h := sha256.New()
	if _, err := io.Copy(h, f); err != nil {
		f.Close()
		return nil, err
	}
	if digest := hex.EncodeToString(h.Sum(nil)); digest != chartVersion.Digest {
		f.Close()
		return nil, fmt.Errorf("local chart %s has digest %s, index expects %s", file, digest, chartVersion.Digest)
	}
	if _, err := f.Seek(0, io.SeekStart); err != nil {
		f.Close()
		return nil, err
	}
	return f, nil
}

func relative(base, publicURL, path string) (string, error) {
	path = strings.TrimPrefix(path, publicURL)
	path = strings.TrimPrefix(path, "file://")

	baseAbs, err := filepath.Abs(base)
	if err != nil {
		return "", err
	}
	fullAbs, err := filepath.Abs(filepath.Join(base, path))
	if err != nil {
		return "", err
	}
	rel, err := filepath.Rel(baseAbs, fullAbs)
	if err != nil {
		return "", fmt.Errorf("failed to resolve relative path: %w", err)
	}
	if !filepath.IsLocal(rel) {
		return "", fmt.Errorf("invalid file path [%s]", path)
	}

	return fullAbs, nil
}
