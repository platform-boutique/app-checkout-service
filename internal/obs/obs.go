package obs

import (
	"context"
	"fmt"
	"io"
	"net/http"
	"os"
	"sync"
	"time"

	"github.com/grafana/pyroscope-go"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/sirupsen/logrus"
)

// DefaultShutdownTimeout bounds Shutdown: the telemetry flush must not keep the process
// alive when the collector is gone (CODE-04: exit within 10 s after SIGTERM).
const DefaultShutdownTimeout = 4 * time.Second

// Options configures Start. Only Service and Loggers are normally set.
type Options struct {
	Service string           // repo name, the default service.name
	Loggers []*logrus.Logger // every logger of the process (main's and the packages')
	Getenv  Getenv           // default os.Getenv
	// Registry carries build_info and is served on /metrics; default prometheus.DefaultRegisterer/Gatherer.
	Registerer prometheus.Registerer
	Gatherer   prometheus.Gatherer
	// ShutdownTimeout bounds Shutdown, default DefaultShutdownTimeout.
	ShutdownTimeout time.Duration
	// NoMetricsServer skips the :9090 listener (tests).
	NoMetricsServer bool
	// MetricsAddr overrides the listen address that METRICS_PORT would give (tests: 127.0.0.1:0).
	MetricsAddr string
	// Out replaces stdout as the log destination (tests).
	Out io.Writer
}

// Obs is the running observability kit of a process.
type Obs struct {
	Settings Settings

	log             *logrus.Logger
	shutdownTimeout time.Duration
	telemetry       func(context.Context) error
	profiler        *pyroscope.Profiler
	servers         []*http.Server
	metricsAddr     string
	pprofAddr       string
	once            sync.Once
}

// Start applies the environment contract and starts what it asks for, in this order:
// JSON logger (level from LOG_LEVEL), "starting" line with version and commit, W3C
// propagator, build_info, OTLP pipelines (only with a valid OTEL_EXPORTER_OTLP_ENDPOINT
// and no OTEL_SDK_DISABLED=true), Pyroscope (only with PYROSCOPE_ADDR), the /metrics
// listener on :9090 and, only with PPROF_ADDR, a pprof listener. It never fails the
// process: a part that cannot start is logged and skipped. Loggers[0] is the main logger.
func Start(ctx context.Context, o Options) *Obs {
	s := LoadSettings(o.Service, o.Getenv)
	if o.MetricsAddr != "" {
		s.MetricsAddr = o.MetricsAddr
	}
	loggers := o.Loggers
	if len(loggers) == 0 {
		loggers = []*logrus.Logger{logrus.StandardLogger()}
	}
	for _, l := range loggers {
		ConfigureLogger(l, s)
		if o.Out != nil {
			l.SetOutput(o.Out)
		}
	}
	log := loggers[0]
	reg, gather := o.Registerer, o.Gatherer
	if reg == nil {
		reg = prometheus.DefaultRegisterer
	}
	if gather == nil {
		gather = prometheus.DefaultGatherer
	}
	x := &Obs{Settings: s, log: log, shutdownTimeout: o.ShutdownTimeout}
	if x.shutdownTimeout <= 0 {
		x.shutdownTimeout = DefaultShutdownTimeout
	}

	log.WithFields(logrus.Fields{"commit": s.Commit, "environment": s.Environment, "build_time": BuildTime}).Info("starting")
	if s.LogLevelInvalid != "" {
		log.Warnf("LOG_LEVEL=%q is not a level (debug, info, warn, error), using info", s.LogLevelInvalid)
	}
	if s.MetricsPortInvalid != "" {
		log.Warnf("METRICS_PORT=%q is not a port, using %s", s.MetricsPortInvalid, DefaultMetricsPort)
	}

	SetPropagator()
	if err := RegisterBuildInfo(reg, s); err != nil {
		log.WithError(err).Warn("build_info not registered")
	}

	if s.TelemetryOn() {
		if sd, err := startTelemetry(ctx, s, loggers...); err != nil {
			log.WithError(err).Warn("telemetry disabled, continuing without it")
		} else {
			x.telemetry = sd
			log.Infof("telemetry enabled (OTLP/gRPC to %s)", s.OTLPEndpoint)
		}
	} else {
		if s.OTLPEndpointInvalid != "" {
			log.Warnf("OTEL_EXPORTER_OTLP_ENDPOINT=%q is not an absolute http(s) URL", s.OTLPEndpointInvalid)
		}
		log.Infof("telemetry disabled (%s)", s.TelemetryOffReason)
	}

	if s.ProfilingOn() {
		if p, err := startProfiling(s); err != nil {
			log.WithError(err).Warn("profiling disabled, continuing without it")
		} else {
			x.profiler = p
			log.Infof("profiling enabled (Pyroscope at %s)", s.PyroscopeAddr)
		}
	} else {
		log.Infof("profiling disabled (%s)", s.ProfilingOffReason)
	}

	if !o.NoMetricsServer {
		mux := http.NewServeMux()
		mux.Handle("/metrics", MetricsHandler(gather))
		srv := newServer(mux)
		if addr, err := listenAndServe(srv, s.MetricsAddr); err != nil {
			log.WithError(err).Errorf("metrics endpoint not started on %s", s.MetricsAddr)
		} else {
			x.servers = append(x.servers, srv)
			x.metricsAddr = addr.String()
			log.Infof("metrics endpoint on %s/metrics", s.MetricsAddr)
		}
		if s.PprofAddr != "" {
			psrv := newServer(pprofMux())
			if addr, err := listenAndServe(psrv, s.PprofAddr); err != nil {
				log.WithError(err).Errorf("pprof endpoint not started on %s", s.PprofAddr)
			} else {
				x.servers = append(x.servers, psrv)
				x.pprofAddr = addr.String()
				log.Warnf("pprof endpoint on %s (PPROF_ADDR; do not expose it)", s.PprofAddr)
			}
		}
	}
	return x
}

// MetricsAddr is the real listen address of /metrics ("" if it is not served).
func (x *Obs) MetricsAddr() string { return x.metricsAddr }

// PprofAddr is the real listen address of pprof ("" when PPROF_ADDR is unset).
func (x *Obs) PprofAddr() string { return x.pprofAddr }

// Shutdown flushes telemetry and profiles and stops the listeners. It is idempotent
// and returns after at most the shutdown timeout, even when the collector or Pyroscope
// is unreachable (the flush is abandoned then and logged).
func (x *Obs) Shutdown() {
	x.once.Do(func() {
		ctx, cancel := context.WithTimeout(context.Background(), x.shutdownTimeout)
		defer cancel()
		done := make(chan error, 1)
		go func() {
			var err error
			if x.telemetry != nil {
				err = x.telemetry(ctx)
			}
			if x.profiler != nil {
				if perr := x.profiler.Stop(); err == nil {
					err = perr
				}
			}
			done <- err
		}()
		select {
		case err := <-done:
			if err != nil {
				x.log.WithError(err).Warn("telemetry flush on shutdown failed")
			}
		case <-ctx.Done():
			x.log.Warnf("telemetry flush on shutdown abandoned after %s", x.shutdownTimeout)
		}
		for _, srv := range x.servers {
			shutdownServer(ctx, srv)
		}
	})
}

// Recover is deferred first in main: an uncaught panic is logged as one JSON line
// (error field) and then re-panics, so the process still dies with its stack trace.
func (x *Obs) Recover() {
	if r := recover(); r != nil {
		x.log.WithError(fmt.Errorf("%v", r)).Error("panic")
		panic(r)
	}
}

// Fatal logs a startup error as JSON and exits 1 (after the flush).
func (x *Obs) Fatal(msg string, err error) {
	x.log.WithError(err).Error(msg)
	x.Shutdown()
	os.Exit(1)
}
