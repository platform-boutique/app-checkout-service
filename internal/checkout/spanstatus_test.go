package checkout

import (
	"context"
	"net"
	"testing"
	"time"

	"go.opentelemetry.io/contrib/instrumentation/google.golang.org/grpc/otelgrpc"
	otelcodes "go.opentelemetry.io/otel/codes"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/sdk/trace/tracetest"
	oteltrace "go.opentelemetry.io/otel/trace"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/emptypb"
)

// instrumentedConn dials addr with the production client handler stack (otelgrpc wrapped by
// ExpectedStatusHandler) and a private tracer provider, so the tests see the real span status.
func instrumentedConn(t *testing.T, addr string, rec *tracetest.SpanRecorder) *grpc.ClientConn {
	t.Helper()
	tp := sdktrace.NewTracerProvider(sdktrace.WithSpanProcessor(rec))
	conn, err := grpc.NewClient(addr,
		grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithStatsHandler(ExpectedStatusHandler(otelgrpc.NewClientHandler(otelgrpc.WithTracerProvider(tp)))),
	)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = conn.Close() })
	return conn
}

// clientSpans waits (generous deadline, no timing assumption) until n client spans have ended:
// the client span ends in the stats handler, which can be just after the call returned.
func clientSpans(t *testing.T, rec *tracetest.SpanRecorder, n int) []sdktrace.ReadOnlySpan {
	t.Helper()
	for deadline := time.Now().Add(5 * time.Second); time.Now().Before(deadline); time.Sleep(2 * time.Millisecond) {
		var out []sdktrace.ReadOnlySpan
		for _, s := range rec.Ended() {
			if s.SpanKind() == oteltrace.SpanKindClient {
				out = append(out, s)
			}
		}
		if len(out) >= n {
			return out
		}
	}
	t.Fatalf("want %d client spans, have %v", n, rec.Ended())
	return nil
}

func rpcStatusAttr(s sdktrace.ReadOnlySpan) string {
	for _, kv := range s.Attributes() {
		if kv.Key == "rpc.response.status_code" || kv.Key == "rpc.grpc.status_code" {
			return kv.Value.String()
		}
	}
	return ""
}

// replyServer answers every /t.T/Ping with err (nil = OK).
func replyServer(t *testing.T, err error) string {
	t.Helper()
	desc := grpc.ServiceDesc{
		ServiceName: "t.T",
		HandlerType: (*any)(nil),
		Methods: []grpc.MethodDesc{{
			MethodName: "Ping",
			Handler: func(_ any, _ context.Context, dec func(any) error, _ grpc.UnaryServerInterceptor) (any, error) {
				if e := dec(new(emptypb.Empty)); e != nil {
					return nil, e
				}
				return new(emptypb.Empty), err
			},
		}},
	}
	lis, lerr := net.Listen("tcp", "127.0.0.1:0")
	if lerr != nil {
		t.Fatal(lerr)
	}
	srv := grpc.NewServer()
	srv.RegisterService(&desc, struct{}{})
	go func() { _ = srv.Serve(lis) }()
	t.Cleanup(srv.Stop)
	return lis.Addr().String()
}

// The client span of an expected status is not an error; every other outcome still is.
func TestExpectedStatusHandler_SpanStatus(t *testing.T) {
	notFound := status.Error(codes.NotFound, "nope")
	cases := []struct {
		name      string
		serverErr error
		expect    []codes.Code // nil = call made without expectCodes
		wantErr   bool         // the span status must be Error
		wantAttr  string
	}{
		{"expected NotFound is not an error span", notFound, []codes.Code{codes.NotFound}, false, "NOT_FOUND"},
		{"NotFound without the marker is an error span", notFound, nil, true, "NOT_FOUND"},
		{"marker for NotFound, Unavailable stays an error", status.Error(codes.Unavailable, "down"), []codes.Code{codes.NotFound}, true, "UNAVAILABLE"},
		{"marker for NotFound, Internal stays an error", status.Error(codes.Internal, "boom"), []codes.Code{codes.NotFound}, true, "INTERNAL"},
		{"marker for NotFound, DeadlineExceeded stays an error", status.Error(codes.DeadlineExceeded, "slow"), []codes.Code{codes.NotFound}, true, "DEADLINE_EXCEEDED"},
		{"marker, success is not an error", nil, []codes.Code{codes.NotFound}, false, "OK"},
		{"several expected codes, second one matches", status.Error(codes.AlreadyExists, "dup"), []codes.Code{codes.NotFound, codes.AlreadyExists}, false, "ALREADY_EXISTS"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			rec := tracetest.NewSpanRecorder()
			conn := instrumentedConn(t, replyServer(t, tc.serverErr), rec)
			ctx := context.Background()
			if tc.expect != nil {
				ctx = expectCodes(ctx, tc.expect...)
			}
			err := conn.Invoke(ctx, "/t.T/Ping", new(emptypb.Empty), new(emptypb.Empty))
			if status.Code(err) != status.Code(tc.serverErr) {
				t.Fatalf("the caller must still see the real status: got %v, want %v", err, tc.serverErr)
			}
			s := clientSpans(t, rec, 1)[0]
			if got := s.Status().Code == otelcodes.Error; got != tc.wantErr {
				t.Errorf("span status %v (%q), error span = %v, want %v", s.Status().Code, s.Status().Description, got, tc.wantErr)
			}
			if got := rpcStatusAttr(s); got != tc.wantAttr {
				t.Errorf("rpc status attribute = %q, want %q (the real status stays visible on the span)", got, tc.wantAttr)
			}
		})
	}
}

// The marker belongs to one call: the next call with a plain context is judged normally.
func TestExpectedStatusHandler_MarkerIsPerCall(t *testing.T) {
	rec := tracetest.NewSpanRecorder()
	conn := instrumentedConn(t, replyServer(t, status.Error(codes.NotFound, "nope")), rec)
	_ = conn.Invoke(expectCodes(context.Background(), codes.NotFound), "/t.T/Ping", new(emptypb.Empty), new(emptypb.Empty))
	_ = conn.Invoke(context.Background(), "/t.T/Ping", new(emptypb.Empty), new(emptypb.Empty))
	spans := clientSpans(t, rec, 2)
	errs := 0
	for _, s := range spans {
		if s.Status().Code == otelcodes.Error {
			errs++
		}
	}
	if errs != 1 {
		t.Errorf("%d error spans of 2 calls, want exactly 1 (the unmarked one)", errs)
	}
}

// Through the real saga: a checkout with an idempotency key looks the order up first and gets
// NotFound for a new order. That call's client span must not be an error, and a successful
// checkout must have no error span at all; a lookup that really fails (Unavailable) must.
func TestPlaceOrder_IdempotencyLookupNotFoundIsNotAnErrorSpan(t *testing.T) {
	lookupSpans := func(t *testing.T, getErr error) (spans []sdktrace.ReadOnlySpan, placeErr error) {
		t.Helper()
		m := sagaMocks()
		m.orders.getErr = getErr
		svc, cleanup := startMockServers(t, m)
		defer cleanup()
		rec := tracetest.NewSpanRecorder()
		svc.OrderConn = instrumentedConn(t, svc.OrderConn.Target(), rec)
		_, placeErr = placeKeyed(svc, "key-1")
		n := 3 // GetOrder, CreateOrder, ConfirmOrder on success; only GetOrder when the lookup fails
		if placeErr != nil {
			n = 1
		}
		return clientSpans(t, rec, n), placeErr
	}
	byMethod := func(spans []sdktrace.ReadOnlySpan, method string) sdktrace.ReadOnlySpan {
		for _, s := range spans {
			if s.Name() == "hipstershop.orders.OrderService/"+method {
				return s
			}
		}
		t.Fatalf("no client span for %s in %v", method, spans)
		return nil
	}

	t.Run("new order", func(t *testing.T) {
		spans, err := lookupSpans(t, nil)
		if err != nil {
			t.Fatalf("PlaceOrder: %v", err)
		}
		if got := byMethod(spans, "GetOrder"); got.Status().Code == otelcodes.Error || rpcStatusAttr(got) != "NOT_FOUND" {
			t.Errorf("GetOrder span status %v attr %q, want not an error with the real NOT_FOUND status attribute", got.Status().Code, rpcStatusAttr(got))
		}
		for _, s := range spans {
			if s.Status().Code == otelcodes.Error {
				t.Errorf("error span on a successful checkout: %s %q", s.Name(), s.Status().Description)
			}
		}
	})
	t.Run("lookup fails", func(t *testing.T) {
		spans, err := lookupSpans(t, status.Error(codes.Unavailable, "down"))
		assertStatus(t, err, codes.Unavailable)
		if got := byMethod(spans, "GetOrder"); got.Status().Code != otelcodes.Error {
			t.Errorf("GetOrder span status %v, want Error for a real failure", got.Status().Code)
		}
	})
}
