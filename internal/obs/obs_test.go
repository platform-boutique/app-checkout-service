package obs

import (
	"context"
	"encoding/json"
	"io"
	"net"
	"net/http"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/collectors"
	"github.com/sirupsen/logrus"
	"go.opentelemetry.io/otel"
)

func newReg() *prometheus.Registry {
	r := prometheus.NewRegistry()
	r.MustRegister(collectors.NewGoCollector())
	return r
}

func get(t *testing.T, url string) (int, string) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	req, _ := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	b, _ := io.ReadAll(resp.Body)
	return resp.StatusCode, string(b)
}

// With nothing configured the process starts quietly: one "starting" line with version and
// commit, telemetry and profiling off, /metrics on the metrics address with build_info,
// and no pprof listener.
func TestStartDefaults(t *testing.T) {
	reg := newReg()
	x := Start(context.Background(), Options{
		Service: "checkout-service", Loggers: []*logrus.Logger{logrus.New()}, Registerer: reg, Gatherer: reg, MetricsAddr: "127.0.0.1:0",
		Getenv: env(map[string]string{"SERVICE_VERSION": "7.7.7", "HOSTNAME": "pod-9"}),
	})
	defer x.Shutdown()

	if x.Settings.TelemetryOn() || x.Settings.ProfilingOn() {
		t.Fatal("telemetry or profiling on without configuration")
	}
	if x.PprofAddr() != "" {
		t.Fatal("pprof listener without PPROF_ADDR")
	}
	code, body := get(t, "http://"+x.MetricsAddr()+"/metrics")
	if code != 200 {
		t.Fatalf("/metrics: %d", code)
	}
	if !strings.Contains(body, `build_info{commit="`) || !strings.Contains(body, `service="checkout-service"`) || !strings.Contains(body, `version="7.7.7"} 1`) {
		t.Fatalf("build_info missing:\n%s", firstLines(body, "build_info"))
	}
	if !strings.Contains(body, "go_goroutines") {
		t.Fatal("runtime metrics missing")
	}
	if code, _ := get(t, "http://"+x.MetricsAddr()+"/debug/pprof/"); code != 404 {
		t.Fatalf("pprof must not be on the metrics port, got %d", code)
	}
}

func firstLines(s, contains string) string {
	var out []string
	for _, l := range strings.Split(s, "\n") {
		if strings.Contains(l, contains) {
			out = append(out, l)
		}
	}
	return strings.Join(out, "\n")
}

func TestStartLogsStartingLine(t *testing.T) {
	sink := &syncBuf{}
	reg := newReg()
	x := Start(context.Background(), Options{
		Service: "checkout-service", Loggers: []*logrus.Logger{logrus.New()}, Registerer: reg, Gatherer: reg, NoMetricsServer: true, Out: sink,
		Getenv: env(map[string]string{"SERVICE_VERSION": "1.0.0", "LOG_LEVEL": "bogus", "OTEL_EXPORTER_OTLP_ENDPOINT": "alloy:4317"}),
	})
	defer x.Shutdown()
	var lines []map[string]any
	for _, l := range strings.Split(strings.TrimSpace(sink.String()), "\n") {
		var m map[string]any
		if err := json.Unmarshal([]byte(l), &m); err != nil {
			t.Fatalf("not JSON: %q", l)
		}
		lines = append(lines, m)
	}
	first := lines[0]
	if first["message"] != "starting" || first["version"] != "1.0.0" || first["commit"] == "" || first["service"] != "checkout-service" || first["severity"] != "info" {
		t.Fatalf("%v", first)
	}
	all := sink.String()
	for _, want := range []string{"LOG_LEVEL=\\\"bogus\\\" is not a level", "is not an absolute http(s) URL", "telemetry disabled (OTEL_EXPORTER_OTLP_ENDPOINT is not a valid", "profiling disabled (PYROSCOPE_ADDR is not set)"} {
		if !strings.Contains(all, want) {
			t.Errorf("missing %q in\n%s", want, all)
		}
	}
}

func TestPprofOnlyWithPprofAddr(t *testing.T) {
	reg := newReg()
	x := Start(context.Background(), Options{
		Service: "svc", Loggers: []*logrus.Logger{logrus.New()}, Registerer: reg, Gatherer: reg, MetricsAddr: "127.0.0.1:0",
		Getenv: env(map[string]string{"PPROF_ADDR": "127.0.0.1:0"}),
	})
	defer x.Shutdown()
	if x.PprofAddr() == "" {
		t.Fatal("PPROF_ADDR set but no pprof listener")
	}
	if code, _ := get(t, "http://"+x.PprofAddr()+"/debug/pprof/cmdline"); code != 200 {
		t.Fatalf("pprof: %d", code)
	}
	if code, _ := get(t, "http://"+x.MetricsAddr()+"/debug/pprof/cmdline"); code != 404 {
		t.Fatalf("pprof leaked to the metrics port: %d", code)
	}
}

func TestResourceAttributes(t *testing.T) {
	t.Setenv("OTEL_RESOURCE_ATTRIBUTES", "team=shop,deployment.environment=override")
	s := LoadSettings("checkout-service", env(map[string]string{"SERVICE_VERSION": "3.1.4", "HOSTNAME": "pod-3", "DEPLOYMENT_ENVIRONMENT": "dev"}))
	res, err := Resource(context.Background(), s)
	if err != nil {
		t.Fatal(err)
	}
	got := map[string]string{}
	for _, kv := range res.Attributes() {
		got[string(kv.Key)] = kv.Value.Emit()
	}
	for k, want := range map[string]string{
		"service.name": "checkout-service", "service.version": "3.1.4", "service.namespace": "everly",
		"service.instance.id": "pod-3", "team": "shop",
		"deployment.environment": "override", // OTEL_RESOURCE_ATTRIBUTES is applied last
	} {
		if got[k] != want {
			t.Errorf("%s=%q want %q (all: %v)", k, got[k], want, got)
		}
	}

	t.Setenv("OTEL_RESOURCE_ATTRIBUTES", "")
	res, _ = Resource(context.Background(), s)
	for _, kv := range res.Attributes() {
		if string(kv.Key) == "deployment.environment" && kv.Value.Emit() != "dev" {
			t.Errorf("deployment.environment=%s", kv.Value.Emit())
		}
	}
	d, _ := Resource(context.Background(), LoadSettings("x", env(nil)))
	for _, kv := range d.Attributes() {
		if string(kv.Key) == "deployment.environment" && kv.Value.Emit() != "local" {
			t.Errorf("default deployment.environment=%s", kv.Value.Emit())
		}
		if string(kv.Key) == "service.version" && kv.Value.Emit() == "" {
			t.Error("empty service.version")
		}
	}
}

// blackhole accepts TCP connections and never answers: an unreachable-but-listening collector.
func blackhole(t *testing.T) string {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	var mu sync.Mutex
	var conns []net.Conn
	go func() {
		for {
			c, err := ln.Accept()
			if err != nil {
				return
			}
			mu.Lock()
			conns = append(conns, c)
			mu.Unlock()
		}
	}()
	t.Cleanup(func() {
		_ = ln.Close()
		mu.Lock()
		for _, c := range conns {
			_ = c.Close()
		}
		mu.Unlock()
	})
	return ln.Addr().String()
}

// OBS-04 / CODE-04: a collector that never answers neither delays the start nor the
// shutdown beyond the bound. The deadlines are generous (the host clock jumps).
func TestBlackholedCollectorAndPyroscope(t *testing.T) {
	addr := blackhole(t)
	reg := newReg()
	begin := time.Now()
	x := Start(context.Background(), Options{
		Service: "svc", Loggers: []*logrus.Logger{logrus.New()}, Registerer: reg, Gatherer: reg, NoMetricsServer: true,
		ShutdownTimeout: time.Second,
		Getenv: env(map[string]string{
			"OTEL_EXPORTER_OTLP_ENDPOINT": "http://" + addr,
			"PYROSCOPE_ADDR":              "http://" + addr,
		}),
	})
	if !x.Settings.TelemetryOn() || !x.Settings.ProfilingOn() || x.telemetry == nil {
		t.Fatalf("telemetry not started: %+v", x.Settings)
	}
	if d := time.Since(begin); d > 8*time.Second {
		t.Fatalf("Start took %s with a blackholed collector", d)
	}
	// Produce a span and a log line so there is something to flush.
	_, span := otel.Tracer("t").Start(context.Background(), "work")
	span.End()

	begin = time.Now()
	x.Shutdown()
	if d := time.Since(begin); d > 8*time.Second {
		t.Fatalf("Shutdown took %s with a blackholed collector (bound is 1s)", d)
	}
	x.Shutdown() // idempotent
}

func TestTelemetryOffStartsNoPipeline(t *testing.T) {
	reg := newReg()
	for name, e := range map[string]map[string]string{
		"no endpoint":      nil,
		"malformed":        {"OTEL_EXPORTER_OTLP_ENDPOINT": "alloy:4317"},
		"sdk disabled":     {"OTEL_EXPORTER_OTLP_ENDPOINT": "http://127.0.0.1:1", "OTEL_SDK_DISABLED": "true"},
		"profiling no url": {"ENABLE_PROFILING": "1"},
	} {
		reg = newReg()
		x := Start(context.Background(), Options{Service: "svc", Loggers: []*logrus.Logger{logrus.New()}, Registerer: reg, Gatherer: reg, NoMetricsServer: true, Getenv: env(e)})
		if x.telemetry != nil || x.profiler != nil {
			t.Errorf("%s: a pipeline was started", name)
		}
		x.Shutdown()
	}
}
