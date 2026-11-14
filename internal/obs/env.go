// Package obs is the observability kit of the Everly Go services (the same code,
// package name and API in every Go repo): the environment contract (OBS-01, OBS-04),
// resource attributes (OBS-02), JSON logs with trace correlation (OBS-30), request ids,
// the OTLP pipelines, Pyroscope, build_info and the metrics endpoint on :9090 (OBS-20).
package obs

import (
	"net/url"
	"os"
	"strconv"
	"strings"

	"github.com/sirupsen/logrus"
)

// Getenv looks an environment variable up (os.Getenv in production, a map in tests).
type Getenv func(string) string

// Namespace is the service.namespace of every Everly service.
const Namespace = "everly"

// Settings is everything the environment decides. It is a plain value so the whole
// contract can be tested with a table (see env_test.go).
type Settings struct {
	Service     string // service.name: OTEL_SERVICE_NAME, else the repo name
	Version     string // SERVICE_VERSION, else build info, else "dev"
	Commit      string // ldflags, else vcs.revision, else "unknown"
	Environment string // DEPLOYMENT_ENVIRONMENT, default "local"
	Instance    string // service.instance.id: HOSTNAME, else os.Hostname()

	// OTLPEndpoint is the validated OTLP/gRPC collector URL; "" means telemetry is OFF
	// and TelemetryOffReason says why.
	OTLPEndpoint       string
	TelemetryOffReason string

	// PyroscopeAddr is the validated Pyroscope URL; "" means profiling is OFF.
	PyroscopeAddr       string
	ProfilingOffReason  string
	MetricsAddr         string // listen address of /metrics, ":9090" unless METRICS_PORT says otherwise
	PprofAddr           string // PPROF_ADDR; "" = no pprof listener (the default)
	LogLevel            logrus.Level
	LogLevelInvalid     string // the rejected LOG_LEVEL value, "" if it was fine
	MetricsPortInvalid  string // the rejected METRICS_PORT value, "" if it was fine
	OTLPEndpointInvalid string // the rejected endpoint value, "" if it was fine
}

// DefaultMetricsPort is the one metrics port of every Go service (OBS-20).
const DefaultMetricsPort = "9090"

// SDKDisabled reports OTEL_SDK_DISABLED=true (trimmed, case-insensitive, as the
// OpenTelemetry specification says; any other value does not disable).
func SDKDisabled(get Getenv) bool {
	return strings.EqualFold(strings.TrimSpace(get("OTEL_SDK_DISABLED")), "true")
}

// ValidHTTPURL reports whether s is an absolute http or https URL with a host.
func ValidHTTPURL(s string) bool {
	u, err := url.Parse(s)
	if err != nil || u.Host == "" || u.Hostname() == "" {
		return false
	}
	return u.Scheme == "http" || u.Scheme == "https"
}

// LoadSettings reads the environment. It never fails: a bad value turns the feature
// off (or falls back to the default) and is reported in the Settings, never a crash.
//
//   - Telemetry is ON only if OTEL_EXPORTER_OTLP_ENDPOINT is a valid absolute http(s)
//     URL; OTEL_SDK_DISABLED=true forces it OFF. The legacy COLLECTOR_SERVICE_ADDR and
//     ENABLE_TRACING are not read any more.
//   - Profiling is ON only if PYROSCOPE_ADDR is a valid URL (no default address);
//     ENABLE_PROFILING=0 and OTEL_SDK_DISABLED=true force it OFF; ENABLE_PROFILING=1
//     alone starts nothing.
func LoadSettings(service string, get Getenv) Settings {
	if get == nil {
		get = os.Getenv
	}
	s := Settings{Service: service, Environment: "local"}
	if v := strings.TrimSpace(get("OTEL_SERVICE_NAME")); v != "" {
		s.Service = v
	}
	s.Version, s.Commit = ResolveBuild(get)
	if v := strings.TrimSpace(get("DEPLOYMENT_ENVIRONMENT")); v != "" {
		s.Environment = v
	}
	s.Instance = strings.TrimSpace(get("HOSTNAME")) // the pod name in Kubernetes
	if s.Instance == "" {
		s.Instance, _ = os.Hostname()
	}

	disabled := SDKDisabled(get)
	switch ep := strings.TrimSpace(get("OTEL_EXPORTER_OTLP_ENDPOINT")); {
	case disabled:
		s.TelemetryOffReason = "OTEL_SDK_DISABLED=true"
	case ep == "":
		s.TelemetryOffReason = "OTEL_EXPORTER_OTLP_ENDPOINT is not set"
	case !ValidHTTPURL(ep):
		s.TelemetryOffReason = "OTEL_EXPORTER_OTLP_ENDPOINT is not a valid absolute http(s) URL"
		s.OTLPEndpointInvalid = ep
	default:
		s.OTLPEndpoint = ep
	}

	switch addr := strings.TrimSpace(get("PYROSCOPE_ADDR")); {
	case disabled:
		s.ProfilingOffReason = "OTEL_SDK_DISABLED=true"
	case strings.TrimSpace(get("ENABLE_PROFILING")) == "0":
		s.ProfilingOffReason = "ENABLE_PROFILING=0"
	case addr == "":
		s.ProfilingOffReason = "PYROSCOPE_ADDR is not set"
	case !ValidHTTPURL(addr):
		s.ProfilingOffReason = "PYROSCOPE_ADDR is not a valid absolute http(s) URL"
	default:
		s.PyroscopeAddr = addr
	}

	port := DefaultMetricsPort
	if v := strings.TrimSpace(get("METRICS_PORT")); v != "" {
		if n, err := strconv.Atoi(v); err == nil && n > 0 && n < 65536 {
			port = v
		} else {
			s.MetricsPortInvalid = v
		}
	}
	s.MetricsAddr = ":" + port
	s.PprofAddr = strings.TrimSpace(get("PPROF_ADDR"))

	s.LogLevel = logrus.InfoLevel
	if v := strings.TrimSpace(get("LOG_LEVEL")); v != "" {
		if l, err := logrus.ParseLevel(strings.ToLower(v)); err == nil {
			s.LogLevel = l
		} else {
			s.LogLevelInvalid = v
		}
	}
	return s
}

// TelemetryOn reports whether the OTLP pipelines are started.
func (s Settings) TelemetryOn() bool { return s.OTLPEndpoint != "" }

// ProfilingOn reports whether Pyroscope is started.
func (s Settings) ProfilingOn() bool { return s.PyroscopeAddr != "" }
