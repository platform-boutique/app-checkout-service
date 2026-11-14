package obs

import (
	"runtime/debug"
	"testing"

	"github.com/sirupsen/logrus"
)

func env(m map[string]string) Getenv { return func(k string) string { return m[k] } }

func TestTelemetryMatrix(t *testing.T) {
	cases := []struct {
		name     string
		env      map[string]string
		wantOn   bool
		wantWhy  string
		wantAddr string
	}{
		{"nothing set", nil, false, "OTEL_EXPORTER_OTLP_ENDPOINT is not set", ""},
		{"empty endpoint", map[string]string{"OTEL_EXPORTER_OTLP_ENDPOINT": ""}, false, "OTEL_EXPORTER_OTLP_ENDPOINT is not set", ""},
		{"blank endpoint", map[string]string{"OTEL_EXPORTER_OTLP_ENDPOINT": "   "}, false, "OTEL_EXPORTER_OTLP_ENDPOINT is not set", ""},
		{"valid http", map[string]string{"OTEL_EXPORTER_OTLP_ENDPOINT": "http://alloy:4317"}, true, "", "http://alloy:4317"},
		{"valid https, trimmed", map[string]string{"OTEL_EXPORTER_OTLP_ENDPOINT": " https://otel.example.com:4317 "}, true, "", "https://otel.example.com:4317"},
		{"host:port without scheme", map[string]string{"OTEL_EXPORTER_OTLP_ENDPOINT": "alloy:4317"}, false, "OTEL_EXPORTER_OTLP_ENDPOINT is not a valid absolute http(s) URL", ""},
		{"bare host", map[string]string{"OTEL_EXPORTER_OTLP_ENDPOINT": "alloy"}, false, "OTEL_EXPORTER_OTLP_ENDPOINT is not a valid absolute http(s) URL", ""},
		{"wrong scheme", map[string]string{"OTEL_EXPORTER_OTLP_ENDPOINT": "ftp://alloy:21"}, false, "OTEL_EXPORTER_OTLP_ENDPOINT is not a valid absolute http(s) URL", ""},
		{"scheme without host", map[string]string{"OTEL_EXPORTER_OTLP_ENDPOINT": "http://"}, false, "OTEL_EXPORTER_OTLP_ENDPOINT is not a valid absolute http(s) URL", ""},
		{"unparsable", map[string]string{"OTEL_EXPORTER_OTLP_ENDPOINT": "http://[::1"}, false, "OTEL_EXPORTER_OTLP_ENDPOINT is not a valid absolute http(s) URL", ""},
		{"sdk disabled wins", map[string]string{"OTEL_EXPORTER_OTLP_ENDPOINT": "http://alloy:4317", "OTEL_SDK_DISABLED": "true"}, false, "OTEL_SDK_DISABLED=true", ""},
		{"sdk disabled upper, padded", map[string]string{"OTEL_EXPORTER_OTLP_ENDPOINT": "http://alloy:4317", "OTEL_SDK_DISABLED": " TRUE "}, false, "OTEL_SDK_DISABLED=true", ""},
		{"sdk disabled=false", map[string]string{"OTEL_EXPORTER_OTLP_ENDPOINT": "http://alloy:4317", "OTEL_SDK_DISABLED": "false"}, true, "", "http://alloy:4317"},
		{"sdk disabled=1 does not disable", map[string]string{"OTEL_EXPORTER_OTLP_ENDPOINT": "http://alloy:4317", "OTEL_SDK_DISABLED": "1"}, true, "", "http://alloy:4317"},
		{"legacy COLLECTOR_SERVICE_ADDR alone is ignored", map[string]string{"COLLECTOR_SERVICE_ADDR": "alloy:4317"}, false, "OTEL_EXPORTER_OTLP_ENDPOINT is not set", ""},
		{"legacy ENABLE_TRACING=1 alone starts nothing", map[string]string{"ENABLE_TRACING": "1"}, false, "OTEL_EXPORTER_OTLP_ENDPOINT is not set", ""},
		{"legacy ENABLE_TRACING=0 is not read", map[string]string{"ENABLE_TRACING": "0", "OTEL_EXPORTER_OTLP_ENDPOINT": "http://alloy:4317"}, true, "", "http://alloy:4317"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			s := LoadSettings("svc", env(tc.env))
			if s.TelemetryOn() != tc.wantOn || s.OTLPEndpoint != tc.wantAddr {
				t.Fatalf("on=%v endpoint=%q, want on=%v endpoint=%q", s.TelemetryOn(), s.OTLPEndpoint, tc.wantOn, tc.wantAddr)
			}
			if s.TelemetryOffReason != tc.wantWhy {
				t.Fatalf("reason=%q want %q", s.TelemetryOffReason, tc.wantWhy)
			}
		})
	}
}

func TestProfilingMatrix(t *testing.T) {
	cases := []struct {
		name string
		env  map[string]string
		want string
	}{
		{"nothing", nil, ""},
		{"ENABLE_PROFILING=1 alone starts nothing (no default address)", map[string]string{"ENABLE_PROFILING": "1"}, ""},
		{"addr only", map[string]string{"PYROSCOPE_ADDR": "http://pyroscope:4040"}, "http://pyroscope:4040"},
		{"addr and ENABLE_PROFILING=1", map[string]string{"PYROSCOPE_ADDR": "http://pyroscope:4040", "ENABLE_PROFILING": "1"}, "http://pyroscope:4040"},
		{"ENABLE_PROFILING=0 forces off", map[string]string{"PYROSCOPE_ADDR": "http://pyroscope:4040", "ENABLE_PROFILING": "0"}, ""},
		{"OTEL_SDK_DISABLED forces off", map[string]string{"PYROSCOPE_ADDR": "http://pyroscope:4040", "OTEL_SDK_DISABLED": "True"}, ""},
		{"malformed addr", map[string]string{"PYROSCOPE_ADDR": "pyroscope:4040"}, ""},
		{"blank addr", map[string]string{"PYROSCOPE_ADDR": " "}, ""},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			s := LoadSettings("svc", env(tc.env))
			if s.PyroscopeAddr != tc.want || s.ProfilingOn() != (tc.want != "") {
				t.Fatalf("addr=%q on=%v, want %q", s.PyroscopeAddr, s.ProfilingOn(), tc.want)
			}
			if tc.want == "" && s.ProfilingOffReason == "" {
				t.Fatal("an off setting must say why")
			}
		})
	}
}

func TestDefaultsAndOverrides(t *testing.T) {
	s := LoadSettings("checkout-service", env(map[string]string{"HOSTNAME": "pod-1"}))
	if s.Service != "checkout-service" || s.Environment != "local" || s.Instance != "pod-1" {
		t.Fatalf("defaults: %+v", s)
	}
	if s.MetricsAddr != ":9090" || s.PprofAddr != "" || s.LogLevel != logrus.InfoLevel {
		t.Fatalf("defaults: %+v", s)
	}
	s = LoadSettings("checkout-service", env(map[string]string{
		"OTEL_SERVICE_NAME": " other ", "DEPLOYMENT_ENVIRONMENT": "dev", "METRICS_PORT": "9191", "PPROF_ADDR": "127.0.0.1:6060", "LOG_LEVEL": "DEBUG",
	}))
	if s.Service != "other" || s.Environment != "dev" || s.MetricsAddr != ":9191" || s.PprofAddr != "127.0.0.1:6060" || s.LogLevel != logrus.DebugLevel {
		t.Fatalf("overrides: %+v", s)
	}
}

func TestBadValuesFallBack(t *testing.T) {
	for _, port := range []string{"abc", "0", "70000", "-1"} {
		s := LoadSettings("svc", env(map[string]string{"METRICS_PORT": port}))
		if s.MetricsAddr != ":9090" || s.MetricsPortInvalid != port {
			t.Errorf("METRICS_PORT=%q: %+v", port, s)
		}
	}
	s := LoadSettings("svc", env(map[string]string{"LOG_LEVEL": "loud"}))
	if s.LogLevel != logrus.InfoLevel || s.LogLevelInvalid != "loud" {
		t.Errorf("LOG_LEVEL=loud: %+v", s)
	}
	for in, want := range map[string]logrus.Level{"warn": logrus.WarnLevel, "warning": logrus.WarnLevel, "error": logrus.ErrorLevel, "info": logrus.InfoLevel, " Debug ": logrus.DebugLevel} {
		if got := LoadSettings("svc", env(map[string]string{"LOG_LEVEL": in})).LogLevel; got != want {
			t.Errorf("LOG_LEVEL=%q: %v want %v", in, got, want)
		}
	}
}

func TestResolveBuild(t *testing.T) {
	defer func(v, c string) { Version, Commit = v, c }(Version, Commit)
	defer func(f func() (*debug.BuildInfo, bool)) { readBuildInfo = f }(readBuildInfo)
	vcs := func(rev, modified string) func() (*debug.BuildInfo, bool) {
		return func() (*debug.BuildInfo, bool) {
			return &debug.BuildInfo{Settings: []debug.BuildSetting{{Key: "vcs.revision", Value: rev}, {Key: "vcs.modified", Value: modified}}}, true
		}
	}
	none := func() (*debug.BuildInfo, bool) { return nil, false }
	const rev = "0123456789abcdef0123456789abcdef01234567"

	cases := []struct {
		name            string
		bi              func() (*debug.BuildInfo, bool)
		ldVersion, ldC  string
		env             map[string]string
		wantV, wantComm string
	}{
		{"nothing", none, "", "", nil, "dev", "unknown"},
		{"vcs", vcs(rev, "false"), "", "", nil, "0123456789ab", rev},
		{"vcs dirty", vcs(rev, "true"), "", "", nil, "0123456789ab-dirty", rev},
		{"ldflags beat vcs", vcs(rev, "false"), "1.4.2", "abc1234", nil, "1.4.2", "abc1234"},
		{"env beats everything", vcs(rev, "false"), "1.4.2", "abc1234", map[string]string{"SERVICE_VERSION": " 9.9.9 "}, "9.9.9", "abc1234"},
		{"env alone", none, "", "", map[string]string{"SERVICE_VERSION": "2.0.0"}, "2.0.0", "unknown"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			readBuildInfo, Version, Commit = tc.bi, tc.ldVersion, tc.ldC
			v, c := ResolveBuild(env(tc.env))
			if v != tc.wantV || c != tc.wantComm {
				t.Fatalf("got %q %q, want %q %q", v, c, tc.wantV, tc.wantComm)
			}
		})
	}
}
