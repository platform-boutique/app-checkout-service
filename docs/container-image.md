# Container image

Multi-stage [`Dockerfile`](https://github.com/platform-boutique/app-checkout-service/tree/main/Dockerfile):

| Stage | Base | What happens |
|---|---|---|
| builder | `golang:1.26-alpine` | `go mod download`, then `CGO_ENABLED=0 GOOS=linux GOARCH=amd64 go build -ldflags="-s -w" ./cmd/server` |
| runtime | `alpine:3.21` | single binary at `/app/checkout-service` |

- **Entrypoint:** `/app/checkout-service`. `GOTRACEBACK=single`.
- **User: root.** The Dockerfile has no `USER`, and the image runs as UID 0 (verified). It needs no privileges (ports > 1024, no file writes), so set a non-root `runAsUser` in Kubernetes (e.g. `10001`) — no image change required.
- **Architecture: `amd64` only** — `GOARCH=amd64` is hard-coded. On `arm64` nodes the pod fails with `exec format error`. Remove the flag (or use `buildx` with `TARGETARCH`) for multi-arch.
- **EXPOSE:** `5050`, `9090`. **`HEALTHCHECK`:** exec form `["/app/checkout-service", "healthcheck"]` (gRPC `grpc.health.v1` Check on `127.0.0.1:${PORT:-5050}`, 3 s deadline, no shell tools).
- **Image size:** ~10 MB. Alpine includes a shell, so `kubectl exec` works.
- `.dockerignore` excludes `vendor/` only; `.git` and `tests/` are sent in the build context.
