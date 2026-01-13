package log

import (
	"log/slog"

	rlog "github.com/rancher/rancher/pkg/log"
	"github.com/rancher/rancher/pkg/scc/consts"
)

type StructuredLogger = *slog.Logger

type Builder struct {
	Controller   string
	SubComponent string
}

func NewLog() StructuredLogger {
	baseLogger := rlog.L().With(slog.String("component", "scc-operator-deployer"))

	if consts.IsDevMode() {
		return baseLogger.With(slog.Bool("devMode", true))
	}

	return baseLogger
}

func NewControllerLogger(controllerName string) StructuredLogger {
	builder := &Builder{
		Controller: controllerName,
	}

	return builder.ToLogger()
}

func (lb *Builder) ToLogger() StructuredLogger {
	baseLogger := NewLog()

	if lb.Controller != "" {
		baseLogger = baseLogger.With(slog.String("controller", lb.Controller))
	}

	if lb.SubComponent != "" {
		baseLogger = baseLogger.With(slog.String("subcomponent", lb.SubComponent))
	}

	return baseLogger
}
