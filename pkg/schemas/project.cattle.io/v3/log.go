package schema

import (
	rlog "github.com/rancher/rancher/pkg/log"
	"log/slog"
)

var (
	log = rlog.L().With(slog.String("component", "types/mapper"))
)
