package log

import (
	"context"
	"fmt"
	"io"
	"log/slog"
	"sync"
	"time"
)

// simpleHandler is a simple text formatter similar to simplelog.StandardFormatter
type simpleHandler struct {
	opts  slog.HandlerOptions
	out   io.Writer
	mu    sync.Mutex
	attrs []slog.Attr
}

func newSimpleHandler(out io.Writer, opts *slog.HandlerOptions) *simpleHandler {
	if opts == nil {
		opts = &slog.HandlerOptions{}
	}
	return &simpleHandler{
		opts: *opts,
		out:  out,
	}
}

func (h *simpleHandler) Enabled(ctx context.Context, level slog.Level) bool {
	minLevel := slog.LevelInfo
	if h.opts.Level != nil {
		minLevel = h.opts.Level.Level()
	}
	return level >= minLevel
}

func (h *simpleHandler) Handle(ctx context.Context, r slog.Record) error {
	h.mu.Lock()
	defer h.mu.Unlock()

	// Format: time level message key=value ...
	buf := make([]byte, 0, 1024)

	// Time
	if !r.Time.IsZero() {
		buf = append(buf, r.Time.Format(time.RFC3339)...)
		buf = append(buf, ' ')
	}

	// Level
	level := r.Level
	if h.opts.ReplaceAttr != nil {
		attr := h.opts.ReplaceAttr(nil, slog.Attr{Key: slog.LevelKey, Value: slog.AnyValue(level)})
		buf = append(buf, fmt.Sprintf("[%-5s] ", attr.Value.String())...)
	} else {
		switch level {
		case LevelTrace:
			buf = append(buf, "[TRACE] "...)
		case slog.LevelDebug:
			buf = append(buf, "[DEBUG] "...)
		case slog.LevelInfo:
			buf = append(buf, "[INFO]  "...)
		case slog.LevelWarn:
			buf = append(buf, "[WARN]  "...)
		case slog.LevelError:
			buf = append(buf, "[ERROR] "...)
		case LevelFatal:
			buf = append(buf, "[FATAL] "...)
		default:
			buf = append(buf, fmt.Sprintf("[%-5d] ", level)...)
		}
	}

	// Message
	buf = append(buf, r.Message...)

	// Attributes from handler
	for _, attr := range h.attrs {
		buf = append(buf, ' ')
		buf = appendAttr(buf, attr)
	}

	// Attributes from record
	r.Attrs(func(a slog.Attr) bool {
		buf = append(buf, ' ')
		buf = appendAttr(buf, a)
		return true
	})

	buf = append(buf, '\n')
	_, err := h.out.Write(buf)
	return err
}

func (h *simpleHandler) WithAttrs(attrs []slog.Attr) slog.Handler {
	h2 := *h
	h2.attrs = append(h2.attrs, attrs...)
	return &h2
}

func (h *simpleHandler) WithGroup(name string) slog.Handler {
	// Simple handler doesn't support groups
	return h
}

func appendAttr(buf []byte, a slog.Attr) []byte {
	// Format: key=value
	buf = append(buf, a.Key...)
	buf = append(buf, '=')

	switch a.Value.Kind() {
	case slog.KindString:
		s := a.Value.String()
		// Quote strings with spaces
		if needsQuoting(s) {
			buf = append(buf, '"')
			buf = append(buf, s...)
			buf = append(buf, '"')
		} else {
			buf = append(buf, s...)
		}
	case slog.KindInt64:
		buf = append(buf, fmt.Sprintf("%d", a.Value.Int64())...)
	case slog.KindUint64:
		buf = append(buf, fmt.Sprintf("%d", a.Value.Uint64())...)
	case slog.KindFloat64:
		buf = append(buf, fmt.Sprintf("%g", a.Value.Float64())...)
	case slog.KindBool:
		buf = append(buf, fmt.Sprintf("%t", a.Value.Bool())...)
	case slog.KindDuration:
		buf = append(buf, a.Value.Duration().String()...)
	case slog.KindTime:
		buf = append(buf, a.Value.Time().Format(time.RFC3339)...)
	default:
		buf = append(buf, fmt.Sprintf("%v", a.Value.Any())...)
	}

	return buf
}

func needsQuoting(s string) bool {
	for _, r := range s {
		if r == ' ' || r == '\t' || r == '\n' || r == '"' {
			return true
		}
	}
	return false
}
