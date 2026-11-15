package checkout

import (
	"context"
	"fmt"
	"runtime/debug"

	"github.com/sirupsen/logrus"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// RecoveryUnaryInterceptor turns a panic in a unary handler into an INTERNAL error with a
// generic message instead of crashing the process (gRPC-Go does not recover handler panics;
// a crash here would drop every PlaceOrder in flight, including ones past the charge).
// The panic value and the stack go to the log only. Install it innermost so a recovered
// panic is still counted as INTERNAL by the metrics and trace interceptors.
func RecoveryUnaryInterceptor(l *logrus.Logger) grpc.UnaryServerInterceptor {
	return func(ctx context.Context, req any, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (resp any, err error) {
		defer func() {
			if r := recover(); r != nil {
				l.WithContext(ctx).WithFields(logrus.Fields{
					"method": info.FullMethod,
					"panic":  fmt.Sprint(r),
					"stack":  string(debug.Stack()),
				}).Error("recovered from handler panic")
				resp, err = nil, status.Error(codes.Internal, "internal error")
			}
		}()
		return handler(ctx, req)
	}
}
