package healthcheck

import (
	"net"
	"strconv"
	"testing"

	"google.golang.org/grpc"
	"google.golang.org/grpc/health"
	healthpb "google.golang.org/grpc/health/grpc_health_v1"
)

// startHealthServer serves grpc.health.v1 on a free loopback port with the given status for "" and
// returns the port. The server is stopped when the test ends.
func startHealthServer(t *testing.T, status healthpb.HealthCheckResponse_ServingStatus) string {
	t.Helper()
	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	srv := grpc.NewServer()
	hs := health.NewServer()
	hs.SetServingStatus("", status)
	healthpb.RegisterHealthServer(srv, hs)
	go func() { _ = srv.Serve(l) }()
	t.Cleanup(srv.Stop)
	return strconv.Itoa(l.Addr().(*net.TCPAddr).Port)
}

func TestHealthcheckServing(t *testing.T) {
	if got := Check(startHealthServer(t, healthpb.HealthCheckResponse_SERVING)); got != 0 {
		t.Fatalf("Check against a SERVING server = %d, want 0", got)
	}
}

func TestHealthcheckNotServing(t *testing.T) {
	if got := Check(startHealthServer(t, healthpb.HealthCheckResponse_NOT_SERVING)); got != 1 {
		t.Fatalf("Check against a NOT_SERVING server = %d, want 1", got)
	}
}

func TestHealthcheckNothingListening(t *testing.T) {
	// Reserve a port, close it, and probe it: nothing answers, the probe must fail within its deadline.
	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	port := strconv.Itoa(l.Addr().(*net.TCPAddr).Port)
	_ = l.Close()
	if got := Check(port); got != 1 {
		t.Fatalf("Check with no server = %d, want 1", got)
	}
}
