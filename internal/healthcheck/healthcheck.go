// Package healthcheck is the container health probe: `<service> healthcheck` asks the service's own
// grpc.health.v1 server whether it is SERVING, so the image needs no nc, wget or shell.
package healthcheck

import (
	"context"
	"fmt"
	"os"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	healthpb "google.golang.org/grpc/health/grpc_health_v1"
)

// healthcheckTimeout bounds the whole probe (dial + Check); the Docker HEALTHCHECK timeout is longer.
const healthcheckTimeout = 3 * time.Second

// Check calls grpc.health.v1.Health/Check on the local server and returns the exit code
// (0 = SERVING, 1 = anything else). Used by the container HEALTHCHECK, so the image needs no extra tool.
func Check(port string) int {
	ctx, cancel := context.WithTimeout(context.Background(), healthcheckTimeout)
	defer cancel()
	conn, err := grpc.NewClient("127.0.0.1:"+port, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		fmt.Fprintln(os.Stderr, "healthcheck:", err)
		return 1
	}
	defer func() { _ = conn.Close() }()
	resp, err := healthpb.NewHealthClient(conn).Check(ctx, &healthpb.HealthCheckRequest{})
	if err != nil {
		fmt.Fprintln(os.Stderr, "healthcheck:", err)
		return 1
	}
	fmt.Println(resp.GetStatus().String())
	if resp.GetStatus() != healthpb.HealthCheckResponse_SERVING {
		return 1
	}
	return 0
}
