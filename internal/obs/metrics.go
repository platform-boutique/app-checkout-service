package obs

import (
	"context"
	"errors"
	"net"
	"net/http"
	"net/http/pprof"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
)

// RegisterBuildInfo registers the gauge build_info{service,version,commit} = 1 (CODE-06).
func RegisterBuildInfo(reg prometheus.Registerer, s Settings) error {
	g := prometheus.NewGauge(prometheus.GaugeOpts{
		Name:        "build_info",
		Help:        "Build information of the running binary; the value is always 1.",
		ConstLabels: prometheus.Labels{"service": s.Service, "version": s.Version, "commit": s.Commit},
	})
	g.Set(1)
	return reg.Register(g)
}

// MetricsHandler serves the Prometheus text format of gatherer.
func MetricsHandler(g prometheus.Gatherer) http.Handler {
	return promhttp.HandlerFor(g, promhttp.HandlerOpts{})
}

// newServer is an HTTP server with explicit timeouts (gosec G114).
func newServer(h http.Handler) *http.Server {
	return &http.Server{
		Handler:           h,
		ReadHeaderTimeout: 10 * time.Second,
		ReadTimeout:       30 * time.Second,
		WriteTimeout:      60 * time.Second,
		IdleTimeout:       120 * time.Second,
	}
}

// listenAndServe listens synchronously (so the caller learns the real address and a
// busy port is reported at start) and serves in the background.
func listenAndServe(srv *http.Server, addr string) (net.Addr, error) {
	ln, err := net.Listen("tcp", addr)
	if err != nil {
		return nil, err
	}
	go func() {
		if err := srv.Serve(ln); err != nil && !errors.Is(err, http.ErrServerClosed) {
			// Nothing sensible to do: metrics are best effort.
			_ = err
		}
	}()
	return ln.Addr(), nil
}

// pprofMux holds the pprof handlers. They are registered on their own mux, never on
// http.DefaultServeMux (gosec G108), and only served when PPROF_ADDR is set.
func pprofMux() *http.ServeMux {
	mux := http.NewServeMux()
	mux.HandleFunc("/debug/pprof/", pprof.Index)
	mux.HandleFunc("/debug/pprof/cmdline", pprof.Cmdline)
	mux.HandleFunc("/debug/pprof/profile", pprof.Profile)
	mux.HandleFunc("/debug/pprof/symbol", pprof.Symbol)
	mux.HandleFunc("/debug/pprof/trace", pprof.Trace)
	return mux
}

func shutdownServer(ctx context.Context, srv *http.Server) {
	if srv != nil {
		_ = srv.Shutdown(ctx)
	}
}
