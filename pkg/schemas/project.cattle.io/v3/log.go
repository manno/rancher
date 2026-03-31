package schema

import (
	"log/slog"

	rlog "github.com/rancher/rancher/pkg/log"
)

var (
	log = rlog.L().With(slog.String("component", "types/mapper"))
)
