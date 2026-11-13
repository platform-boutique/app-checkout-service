package obs

import (
	"bytes"
	"context"
	"encoding/json"
	"net"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"

	"github.com/sirupsen/logrus"
	"go.opentelemetry.io/contrib/instrumentation/google.golang.org/grpc/otelgrpc"
	"go.opentelemetry.io/otel/propagation"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/sdk/trace/tracetest"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/test/bufconn"
	"google.golang.org/protobuf/types/known/emptypb"
)

// syncBuf is a goroutine-safe log sink.
type syncBuf struct {
	mu sync.Mutex
	b  bytes.Buffer
}

func (s *syncBuf) Write(p []byte) (int, error) { s.mu.Lock(); defer s.mu.Unlock(); return s.b.Write(p) }
func (s *syncBuf) String() string              { s.mu.Lock(); defer s.mu.Unlock(); return s.b.String() }

func testLogger(sink *syncBuf) *logrus.Logger {
	l := logrus.New()
	ConfigureLogger(l, LoadSettings("test-service", env(nil)))
	l.SetOutput(sink)
	return l
}

// The request path end to end: real otelgrpc client and server handlers over bufconn, the
// obs interceptors, a handler logging with log.WithContext(ctx). The line must carry the
// trace id and span id of the SERVER span, and the request id sent by the client.
func TestGRPCRequestPathLogsCarryTraceIDs(t *testing.T) {
	rec := tracetest.NewSpanRecorder()
	tp := sdktrace.NewTracerProvider(sdktrace.WithSpanProcessor(rec))
	prop := propagation.NewCompositeTextMapPropagator(propagation.TraceContext{}, propagation.Baggage{})
	sink := &syncBuf{}
	log := testLogger(sink)

	desc := grpc.ServiceDesc{
		ServiceName: "t.T",
		HandlerType: (*any)(nil),
		Methods: []grpc.MethodDesc{{
			MethodName: "Ping",
			Handler: func(srv any, ctx context.Context, dec func(any) error, ic grpc.UnaryServerInterceptor) (any, error) {
				in := new(emptypb.Empty)
				if err := dec(in); err != nil {
					return nil, err
				}
				h := func(ctx context.Context, _ any) (any, error) {
					log.WithContext(ctx).Info("inside the handler")
					return new(emptypb.Empty), nil
				}
				if ic == nil {
					return h(ctx, in)
				}
				return ic(ctx, in, &grpc.UnaryServerInfo{Server: srv, FullMethod: "/t.T/Ping"}, h)
			},
		}},
	}
	lis := bufconn.Listen(1 << 20)
	srv := grpc.NewServer(
		grpc.StatsHandler(otelgrpc.NewServerHandler(otelgrpc.WithTracerProvider(tp), otelgrpc.WithPropagators(prop))),
		grpc.ChainUnaryInterceptor(UnaryServerInterceptor()),
		grpc.ChainStreamInterceptor(StreamServerInterceptor()),
	)
	srv.RegisterService(&desc, struct{}{})
	go func() { _ = srv.Serve(lis) }()
	defer srv.Stop()

	conn, err := grpc.NewClient("passthrough:///bufnet",
		grpc.WithContextDialer(func(context.Context, string) (net.Conn, error) { return lis.Dial() }),
		grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithStatsHandler(otelgrpc.NewClientHandler(otelgrpc.WithTracerProvider(tp), otelgrpc.WithPropagators(prop))),
		grpc.WithChainUnaryInterceptor(UnaryClientInterceptor()),
	)
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()

	ctx := WithRequestID(context.Background(), "req-42")
	if err := conn.Invoke(ctx, "/t.T/Ping", new(emptypb.Empty), new(emptypb.Empty)); err != nil {
		t.Fatal(err)
	}

	var serverSpan sdktrace.ReadOnlySpan
	for _, s := range rec.Ended() {
		if s.SpanKind().String() == "server" {
			serverSpan = s
		}
	}
	if serverSpan == nil {
		t.Fatalf("no server span recorded: %v", rec.Ended())
	}
	var m map[string]any
	if err := json.Unmarshal([]byte(strings.TrimSpace(sink.String())), &m); err != nil {
		t.Fatalf("%v: %q", err, sink.String())
	}
	if m["trace_id"] != serverSpan.SpanContext().TraceID().String() || m["span_id"] != serverSpan.SpanContext().SpanID().String() {
		t.Fatalf("log ids %v/%v differ from the server span %s/%s", m["trace_id"], m["span_id"], serverSpan.SpanContext().TraceID(), serverSpan.SpanContext().SpanID())
	}
	if m["request_id"] != "req-42" || m["message"] != "inside the handler" || m["service"] != "test-service" {
		t.Fatalf("%v", m)
	}
}

func TestIncomingRequestIDIsSanitised(t *testing.T) {
	for in, want := range map[string]string{
		"abc-123_X.y":           "abc-123_X.y",
		"":                      "",
		"bad id":                "",
		`x"}{"a":"b`:            "",
		strings.Repeat("a", 65): "",
		strings.Repeat("a", 64): strings.Repeat("a", 64),
	} {
		ctx := metadata.NewIncomingContext(context.Background(), metadata.Pairs("x-request-id", in))
		if got := RequestID(withIncomingRequestID(ctx)); got != want {
			t.Errorf("%q -> %q, want %q", in, got, want)
		}
	}
	if RequestID(withIncomingRequestID(context.Background())) != "" {
		t.Error("no metadata must give no id")
	}
}

func TestHTTPMiddleware(t *testing.T) {
	var seen string
	h := HTTPMiddleware(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { seen = RequestID(r.Context()) }))

	rr := httptest.NewRecorder()
	req := httptest.NewRequest("GET", "/", nil)
	req.Header.Set(RequestIDHeader, "from-client-1")
	h.ServeHTTP(rr, req)
	if seen != "from-client-1" || rr.Header().Get(RequestIDHeader) != "from-client-1" {
		t.Fatalf("kept id: %q %q", seen, rr.Header().Get(RequestIDHeader))
	}

	rr = httptest.NewRecorder()
	req = httptest.NewRequest("GET", "/", nil)
	req.Header.Set(RequestIDHeader, "evil\"id")
	h.ServeHTTP(rr, req)
	if len(seen) != 32 || seen == "evil\"id" || rr.Header().Get(RequestIDHeader) != seen {
		t.Fatalf("generated id: %q", seen)
	}
}
