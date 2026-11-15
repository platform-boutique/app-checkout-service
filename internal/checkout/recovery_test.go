package checkout

import (
	"bytes"
	"context"
	"strings"
	"testing"

	"github.com/sirupsen/logrus"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func TestRecoveryInterceptor_PanicBecomesGenericInternal_DetailsOnlyInLog(t *testing.T) {
	var buf bytes.Buffer
	l := logrus.New()
	l.SetOutput(&buf)
	icpt := RecoveryUnaryInterceptor(l)
	info := &grpc.UnaryServerInfo{FullMethod: "/hipstershop.CheckoutService/PlaceOrder"}

	_, err := icpt(context.Background(), nil, info, func(context.Context, any) (any, error) {
		panic("secret internal detail")
	})

	if status.Code(err) != codes.Internal || strings.Contains(err.Error(), "secret") {
		t.Fatalf("want a generic INTERNAL, got %v", err)
	}
	if !strings.Contains(buf.String(), "secret internal detail") || !strings.Contains(buf.String(), "recovered from handler panic") {
		t.Errorf("the panic must be in the log: %s", buf.String())
	}
}

func TestRecoveryInterceptor_PassesThrough(t *testing.T) {
	l := logrus.New()
	l.SetOutput(&bytes.Buffer{})
	resp, err := RecoveryUnaryInterceptor(l)(context.Background(), nil, &grpc.UnaryServerInfo{}, func(context.Context, any) (any, error) { return "ok", nil })
	if err != nil || resp != "ok" {
		t.Fatalf("got %v, %v", resp, err)
	}
}
