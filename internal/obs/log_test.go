package obs

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/sirupsen/logrus"
	"go.opentelemetry.io/otel/trace"
)

var fixedTime = time.Date(2026, 11, 13, 10, 11, 12, 123_456_789, time.FixedZone("CET", 3600))

func spanCtx(t *testing.T) context.Context {
	t.Helper()
	tid, _ := trace.TraceIDFromHex("4bf92f3577b34da6a3ce929d0e0e4736")
	sid, _ := trace.SpanIDFromHex("00f067aa0ba902b7")
	return trace.ContextWithSpanContext(context.Background(), trace.NewSpanContext(trace.SpanContextConfig{TraceID: tid, SpanID: sid, TraceFlags: trace.FlagsSampled}))
}

func format(t *testing.T, e *logrus.Entry) string {
	t.Helper()
	e.Time = fixedTime
	b, err := NewFormatter("checkout-service", "1.2.3").Format(e)
	if err != nil {
		t.Fatal(err)
	}
	if !strings.HasSuffix(string(b), "\n") || strings.Count(string(b), "\n") != 1 {
		t.Fatalf("not exactly one line: %q", b)
	}
	return strings.TrimSuffix(string(b), "\n")
}

// Golden lines: this is the log shape every service has to produce.
func TestGoldenShape(t *testing.T) {
	l := logrus.New()
	entry := func(ctx context.Context, lvl logrus.Level, msg string, kv ...any) *logrus.Entry {
		e := logrus.NewEntry(l).WithContext(ctx)
		for i := 0; i+1 < len(kv); i += 2 {
			if err, ok := kv[i+1].(error); ok {
				e = e.WithError(err)
			} else {
				e = e.WithField(kv[i].(string), kv[i+1])
			}
		}
		e.Level, e.Message = lvl, msg
		return e
	}

	t.Run("inside a span, with request id", func(t *testing.T) {
		ctx := WithRequestID(spanCtx(t), "req-1")
		got := format(t, entry(ctx, logrus.InfoLevel, "hello", "order_id", "o1"))
		want := `{"message":"hello","order_id":"o1","request_id":"req-1","service":"checkout-service","severity":"info","span_id":"00f067aa0ba902b7","timestamp":"2026-11-13T09:11:12.123Z","trace_id":"4bf92f3577b34da6a3ce929d0e0e4736","version":"1.2.3"}`
		if got != want {
			t.Fatalf("\n got %s\nwant %s", got, want)
		}
	})
	t.Run("outside a span the ids are absent", func(t *testing.T) {
		got := format(t, entry(context.Background(), logrus.WarnLevel, "no span"))
		want := `{"message":"no span","service":"checkout-service","severity":"warning","timestamp":"2026-11-13T09:11:12.123Z","version":"1.2.3"}`
		if got != want {
			t.Fatalf("\n got %s\nwant %s", got, want)
		}
	})
	t.Run("no context at all", func(t *testing.T) {
		e := logrus.NewEntry(l)
		e.Level, e.Message = logrus.InfoLevel, "plain"
		got := format(t, e)
		if strings.Contains(got, "trace_id") || strings.Contains(got, "span_id") || strings.Contains(got, "request_id") {
			t.Fatalf("ids without a context: %s", got)
		}
	})
	t.Run("an invalid (all zero) span context adds nothing", func(t *testing.T) {
		ctx := trace.ContextWithSpanContext(context.Background(), trace.NewSpanContext(trace.SpanContextConfig{}))
		got := format(t, entry(ctx, logrus.InfoLevel, "zero"))
		if strings.Contains(got, "trace_id") || strings.Contains(got, "span_id") || strings.Contains(got, "0000000") {
			t.Fatalf("zero ids logged: %s", got)
		}
	})
	t.Run("error is a string", func(t *testing.T) {
		got := format(t, entry(spanCtx(t), logrus.ErrorLevel, "boom", "error", errors.New("db down")))
		var m map[string]any
		if err := json.Unmarshal([]byte(got), &m); err != nil {
			t.Fatal(err)
		}
		if m["error"] != "db down" || m["severity"] != "error" {
			t.Fatalf("%v", m)
		}
	})
	t.Run("four severities", func(t *testing.T) {
		for lvl, want := range map[logrus.Level]string{
			logrus.DebugLevel: "debug", logrus.InfoLevel: "info", logrus.WarnLevel: "warning", logrus.ErrorLevel: "error",
			logrus.FatalLevel: "error", logrus.PanicLevel: "error", logrus.TraceLevel: "debug",
		} {
			var m map[string]any
			if err := json.Unmarshal([]byte(format(t, entry(context.Background(), lvl, "x"))), &m); err != nil {
				t.Fatal(err)
			}
			if m["severity"] != want {
				t.Errorf("%v -> %v, want %s", lvl, m["severity"], want)
			}
		}
	})
	t.Run("the entry is not modified", func(t *testing.T) {
		e := entry(spanCtx(t), logrus.FatalLevel, "x", "a", 1)
		_ = format(t, e)
		if _, ok := e.Data["trace_id"]; ok || e.Level != logrus.FatalLevel {
			t.Fatalf("entry changed: %v %v", e.Data, e.Level)
		}
	})
}

func TestConfigureLogger(t *testing.T) {
	var buf bytes.Buffer
	l := logrus.New()
	ConfigureLogger(l, LoadSettings("svc", env(map[string]string{"LOG_LEVEL": "warn"})))
	l.SetOutput(&buf)
	l.Info("dropped")
	l.WithContext(spanCtx(t)).Warn("kept")
	lines := strings.Split(strings.TrimSpace(buf.String()), "\n")
	if len(lines) != 1 {
		t.Fatalf("LOG_LEVEL=warn: %q", buf.String())
	}
	var m map[string]any
	if err := json.Unmarshal([]byte(lines[0]), &m); err != nil {
		t.Fatal(err)
	}
	if m["service"] != "svc" || m["trace_id"] != "4bf92f3577b34da6a3ce929d0e0e4736" || m["message"] != "kept" {
		t.Fatalf("%v", m)
	}
}
