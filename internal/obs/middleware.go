package obs

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"net/http"

	"google.golang.org/grpc"
	"google.golang.org/grpc/metadata"
)

// RequestIDHeader is the HTTP header and gRPC metadata key of the request id.
const RequestIDHeader = "X-Request-Id"

const maxRequestIDLen = 64

// cleanRequestID returns id if it is safe to log (1..64 of A-Z a-z 0-9 . _ -), else "".
// An id from the network is never trusted: it ends up in a log line.
func cleanRequestID(id string) string {
	if id == "" || len(id) > maxRequestIDLen {
		return ""
	}
	for _, c := range id {
		ok := c >= 'a' && c <= 'z' || c >= 'A' && c <= 'Z' || c >= '0' && c <= '9' || c == '.' || c == '_' || c == '-'
		if !ok {
			return ""
		}
	}
	return id
}

// NewRequestID returns 16 random bytes in hex.
func NewRequestID() string {
	var b [16]byte
	if _, err := rand.Read(b[:]); err != nil {
		return ""
	}
	return hex.EncodeToString(b[:])
}

// UnaryServerInterceptor puts the caller's request id (metadata x-request-id, if any)
// into the context so that log.WithContext(ctx) shows it next to the trace ids. The
// span itself comes from the otelgrpc stats handler, which runs before the interceptor.
func UnaryServerInterceptor() grpc.UnaryServerInterceptor {
	return func(ctx context.Context, req any, _ *grpc.UnaryServerInfo, h grpc.UnaryHandler) (any, error) {
		return h(withIncomingRequestID(ctx), req)
	}
}

// StreamServerInterceptor is the streaming twin of UnaryServerInterceptor.
func StreamServerInterceptor() grpc.StreamServerInterceptor {
	return func(srv any, ss grpc.ServerStream, _ *grpc.StreamServerInfo, h grpc.StreamHandler) error {
		return h(srv, &ctxStream{ServerStream: ss, ctx: withIncomingRequestID(ss.Context())})
	}
}

type ctxStream struct {
	grpc.ServerStream
	ctx context.Context
}

func (s *ctxStream) Context() context.Context { return s.ctx }

func withIncomingRequestID(ctx context.Context) context.Context {
	if md, ok := metadata.FromIncomingContext(ctx); ok {
		if v := md.Get("x-request-id"); len(v) > 0 {
			return WithRequestID(ctx, cleanRequestID(v[0]))
		}
	}
	return ctx
}

// OutgoingRequestID copies the request id of ctx into the outgoing gRPC metadata, so
// the next service logs the same id.
func OutgoingRequestID(ctx context.Context) context.Context {
	if id := RequestID(ctx); id != "" {
		return metadata.AppendToOutgoingContext(ctx, "x-request-id", id)
	}
	return ctx
}

// UnaryClientInterceptor forwards the request id on every outgoing unary call.
func UnaryClientInterceptor() grpc.UnaryClientInterceptor {
	return func(ctx context.Context, method string, req, reply any, cc *grpc.ClientConn, inv grpc.UnaryInvoker, opts ...grpc.CallOption) error {
		return inv(OutgoingRequestID(ctx), method, req, reply, cc, opts...)
	}
}

// HTTPMiddleware gives every request an id (a clean inbound X-Request-Id, else a new
// one), puts it into the request context and echoes it in the response header. The
// trace ids are in the context already when the handler is wrapped by otelhttp.
func HTTPMiddleware(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		id := cleanRequestID(r.Header.Get(RequestIDHeader))
		if id == "" {
			id = NewRequestID()
		}
		w.Header().Set(RequestIDHeader, id)
		next.ServeHTTP(w, r.WithContext(WithRequestID(r.Context(), id)))
	})
}
