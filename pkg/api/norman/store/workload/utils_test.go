package workload

import (
	"testing"

	"github.com/rancher/rancher/pkg/log"
)

func TestGetRegistryDomain(t *testing.T) {
	var d string
	var err error

	tests := [][]string{
		{"docker.io", "docker.io"},
		{"http://index.docker.io/v1/", "index.docker.io"},
		{"https://index.docker.io/v1/", "index.docker.io"},
		{"http://my.private.registry:5000/v1", "my.private.registry:5000"},
		{"https://my.private.registry/v1", "my.private.registry"},
	}

	for _, test := range tests {
		d, err = GetRegistryDomain(test[0])
		if err != nil {
			t.Fail()
		}
		if d != test[1] {
			log.Info("Registry domain mismatch", "domain", d)
			t.Fail()
		}
	}
}
