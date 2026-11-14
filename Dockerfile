# --- Stage 1: Build ---
# BuildKit sets TARGETOS/TARGETARCH from --platform, so the same Dockerfile builds amd64 and arm64.
FROM --platform=$BUILDPLATFORM golang:1.26-alpine AS builder
ARG TARGETOS TARGETARCH
# Build identity, linked into internal/obs (a -X for a symbol the binary does not have is ignored by the linker)
ARG SERVICE_VERSION=dev
ARG SERVICE_COMMIT=unknown

WORKDIR /app
# go.mod needs a newer patch release than the golang:1.26-alpine tag ships: let Go fetch the toolchain it asks for
ENV GOTOOLCHAIN=auto

COPY go.mod go.sum ./
RUN go mod download

COPY . .
RUN CGO_ENABLED=0 GOOS=${TARGETOS:-linux} GOARCH=${TARGETARCH} go build \
    -ldflags="-s -w -X checkout-service/internal/obs.Version=${SERVICE_VERSION} -X checkout-service/internal/obs.Commit=${SERVICE_COMMIT}" \
    -o /checkout-service \
    ./cmd/server

# --- Stage 2: Run ---
FROM alpine:3.24
ARG SERVICE_VERSION=dev
ARG SERVICE_COMMIT=unknown

LABEL org.opencontainers.image.title="checkout-service" \
      org.opencontainers.image.source="https://github.com/platform-boutique/app-checkout-service" \
      org.opencontainers.image.revision="${SERVICE_COMMIT}" \
      org.opencontainers.image.version="${SERVICE_VERSION}" \
      org.opencontainers.image.licenses="Apache-2.0"

WORKDIR /app
# go.mod needs a newer patch release than the golang:1.26-alpine tag ships: let Go fetch the toolchain it asks for
ENV GOTOOLCHAIN=auto

COPY --from=builder /checkout-service .

ENV GOTRACEBACK=single \
    SERVICE_VERSION=${SERVICE_VERSION} \
    SERVICE_COMMIT=${SERVICE_COMMIT}

# numeric non-root user (no entry in /etc/passwd needed); matches the charts' runAsUser. Nothing is written to disk.
USER 10001:10001

EXPOSE 5050 9090

# The service has no healthcheck subcommand: TCP connect to the gRPC port with busybox nc (the image has no other client)
HEALTHCHECK --interval=10s --timeout=5s --start-period=10s --retries=3 \
    CMD nc -z localhost ${PORT:-5050} || exit 1

ENTRYPOINT ["/app/checkout-service"]
