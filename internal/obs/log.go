package obs

import (
	"context"
	"os"

	"github.com/sirupsen/logrus"
	"go.opentelemetry.io/otel/trace"
)

// timestampFormat is RFC 3339 in UTC with milliseconds, always with the fractional part.
const timestampFormat = "2006-01-02T15:04:05.000Z07:00"

type ctxKey int

const requestIDKey ctxKey = 1

// WithRequestID returns ctx carrying the request id (the logs of this request show it).
func WithRequestID(ctx context.Context, id string) context.Context {
	if id == "" {
		return ctx
	}
	return context.WithValue(ctx, requestIDKey, id)
}

// RequestID returns the request id of ctx, or "".
func RequestID(ctx context.Context) string {
	id, _ := ctx.Value(requestIDKey).(string)
	return id
}

// Formatter renders one JSON object per line, the same fields in every Go service:
//
//	timestamp (RFC 3339, UTC), severity (debug|info|warning|error), message,
//	service, version, trace_id + span_id (only inside a valid span), request_id (when
//	the request has one), error (from WithError), plus the entry's own fields.
//
// The ids come from entry.Context, so call sites write log.WithContext(ctx).Infof(...).
// The entry itself is not modified (other hooks, e.g. the OTLP bridge, see the original).
type Formatter struct {
	Service string
	Version string
	inner   *logrus.JSONFormatter
}

// NewFormatter returns the formatter of a service.
func NewFormatter(service, version string) *Formatter {
	return &Formatter{Service: service, Version: version, inner: &logrus.JSONFormatter{
		TimestampFormat: timestampFormat,
		FieldMap: logrus.FieldMap{
			logrus.FieldKeyTime:  "timestamp",
			logrus.FieldKeyLevel: "severity",
			logrus.FieldKeyMsg:   "message",
		},
	}}
}

// Format implements logrus.Formatter.
func (f *Formatter) Format(e *logrus.Entry) ([]byte, error) {
	data := make(logrus.Fields, len(e.Data)+5)
	for k, v := range e.Data {
		data[k] = v
	}
	data["service"] = f.Service
	data["version"] = f.Version
	if e.Context != nil {
		if sc := trace.SpanContextFromContext(e.Context); sc.IsValid() {
			data["trace_id"] = sc.TraceID().String()
			data["span_id"] = sc.SpanID().String()
		}
		if id := RequestID(e.Context); id != "" {
			data["request_id"] = id
		}
	}
	c := *e
	c.Data = data
	c.Time = e.Time.UTC()
	switch c.Level { // the contract has four severities
	case logrus.FatalLevel, logrus.PanicLevel:
		c.Level = logrus.ErrorLevel
	case logrus.TraceLevel:
		c.Level = logrus.DebugLevel
	}
	return f.inner.Format(&c)
}

// ConfigureLogger makes l write the contract's JSON to stdout at the configured level.
func ConfigureLogger(l *logrus.Logger, s Settings) {
	l.SetFormatter(NewFormatter(s.Service, s.Version))
	l.SetOutput(os.Stdout)
	l.SetLevel(s.LogLevel)
}
