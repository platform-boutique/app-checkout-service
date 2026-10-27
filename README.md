# checkout-service

A gRPC service that orchestrates order placement for the platform-demo e-commerce platform. A single `PlaceOrder` call fans out to six other services — cart, product catalog, currency, shipping, payment and email — to price the order, charge the card, ship it and confirm it. It is the most connected service in the system and holds no state of its own.

Part of the [platform-demo](https://github.com/platform-boutique) platform — microservices, observability (LGTM stack), GitOps (Argo CD), policy enforcement (Kyverno), infrastructure provisioning (Crossplane) and an internal developer portal (Backstage). See the master `README.md` in the platform-demo root for the whole-system view.

## At a glance

| | |
|---|---|
| **Language / runtime** | Go 1.25 (static binary, `CGO_ENABLED=0`) |
| **Image** | `mladenovskistefan/checkout-service` |
| **Protocol / port** | gRPC (HTTP/2, plaintext) on `5050` |
| **Metrics** | Pushed over OTLP/gRPC (Prometheus registry bridged into OpenTelemetry); `/metrics` on `9090` is still served for debugging |
| **Health** | Standard `grpc.health.v1.Health/Check` on `5050` — static `SERVING`, does **not** reflect downstream health |
| **State** | None |
| **Depends on** | `product-catalog-service`, `cart-service`, `currency-service`, `shipping-service`, `payment-service`, `email-service` |
| **Called by** | `frontend` |
| **gRPC reflection** | Enabled — `grpcurl` works without `-proto` |

## API

Package `hipstershop`, service `CheckoutService` ([`proto/checkout.proto`](proto/checkout.proto)). The proto file also contains client-side copies of the downstream service definitions.

| Method | Request | Response |
|---|---|---|
| `PlaceOrder` | `PlaceOrderRequest{ user_id, user_currency, Address address, email, CreditCardInfo credit_card }` | `PlaceOrderResponse{ OrderResult order }` (`order_id`, `shipping_tracking_id`, `shipping_cost`, `shipping_address`, `items[]`) |

### What `PlaceOrder` does (in order)

1. Reject requests without `user_id`, `user_currency`, address or credit card (`INVALID_ARGUMENT`), before any downstream call.
2. Generate the order UUID (random v4). It is the `order_id` of the `OrderResult`.
3. `cart-service.GetCart(user_id)`; an empty cart is `FAILED_PRECONDITION`.
4. For every cart line, **in parallel**: `product-catalog-service.GetProduct` then `currency-service.Convert` (USD → `user_currency`).
5. `shipping-service.GetQuote` (USD) then `currency-service.Convert` to `user_currency`.
6. Sum items × quantity + shipping.
7. **Only if `ORDER_SERVICE_ADDR` is set:** `order-service.CreateOrder` stores the order as `PENDING` (see [Order storage](#order-storage)). If that fails, nothing is charged.
8. `payment-service.Charge(total, credit_card)` — failure aborts the order (nothing is shipped, emptied or mailed; with order storage the order is marked `FAILED`).
9. `shipping-service.ShipOrder`. If it fails, the charge is **compensated**: `payment-service.Refund(transaction_id, total)` is called once (on a context detached from the caller, 10 s timeout), then the shipping error is returned with the note "your payment has been refunded". If the refund itself fails, the original error is kept, the failure is logged at error level with the transaction id, and the message says the refund is pending (with the transaction id as reference).
   With order storage the order is then marked `FAILED` too (after the refund).
10. **Only with order storage:** `order-service.ConfirmOrder(order_id, tracking_id, transaction_id)`; the order becomes `PLACED`.
11. `cart-service.EmptyCart` — error ignored.
12. `email-service.SendOrderConfirmation` — error logged, **never fails the order**.

Everything that can fail without side effects (steps 1-6) runs before the card is charged.

### Error codes returned by `PlaceOrder`

Downstream gRPC codes are passed through; the message is `<prefix>: <downstream message>`; payment's machine-readable `reason:` prefix is stripped. Raw details of other failures stay in the server log.

| Downstream code (cart, product, currency, shipping, payment) | `PlaceOrder` code | Message |
|---|---|---|
| `INVALID_ARGUMENT` | `INVALID_ARGUMENT` | `<service>: <message>`; payment: `card rejected: <message>`; e.g. `shipping: invalid address: country "X" is not ...` |
| `FAILED_PRECONDITION` | `FAILED_PRECONDITION` | `<service>: <message>`; payment: `card declined: <message>` |
| `NOT_FOUND` | `NOT_FOUND` | `<service>: <message>`, e.g. `product: ...` |
| `UNAVAILABLE`, `DEADLINE_EXCEEDED`, `RESOURCE_EXHAUSTED` | `UNAVAILABLE` | `<service> service is temporarily unavailable, please retry` |
| `CANCELED` | `CANCELED` | `request canceled` |
| anything else (`UNKNOWN`, `INTERNAL`, `UNIMPLEMENTED`, ...) | `INTERNAL` | `internal error while placing the order` |

`<service>` is `cart`, `product`, `currency` or `shipping`. Own validation: missing user, currency, address or card -> `INVALID_ARGUMENT` (`order: ...`); empty cart -> `FAILED_PRECONDITION` (`order: the cart is empty`).

## Configuration

| Variable | Default | Description |
|---|---|---|
| `PORT` | `5050` | gRPC listen port |
| `METRICS_PORT` | `9090` | Prometheus (+pprof) HTTP port |
| `PRODUCT_CATALOG_SERVICE_ADDR` | `product-catalog-service:3550` | `host:port` of product-catalog (gRPC) |
| `CART_SERVICE_ADDR` | `cart-service:7070` | `host:port` of cart |
| `CURRENCY_SERVICE_ADDR` | `currency-service:7000` | `host:port` of currency |
| `SHIPPING_SERVICE_ADDR` | `shipping-service:50051` | `host:port` of shipping |
| `PAYMENT_SERVICE_ADDR` | `payment-service:50051` | `host:port` of payment |
| `EMAIL_SERVICE_ADDR` | `email-service:8080` | `host:port` of email |
| `OTEL_EXPORTER_OTLP_ENDPOINT` | unset (telemetry off) | OTLP/**gRPC** endpoint for **traces, metrics and logs** (scheme required), e.g. `http://alloy.monitoring.svc.cluster.local:4317`. Legacy fallback: `COLLECTOR_SERVICE_ADDR` (`host:port`). Telemetry is on iff an endpoint is set |
| `ENABLE_TRACING` | — | Legacy switch: `0` forces all telemetry off even if an endpoint is set (it is no longer needed to turn telemetry on) |
| `OTEL_METRIC_EXPORT_INTERVAL` | `15000` | Milliseconds between metric pushes (Go also accepts durations such as `15s`) |
| `OTEL_SERVICE_NAME` | `checkout-service` | `service.name` — becomes the Prometheus `job` and the Loki `service_name`; keep the default, dashboards depend on it |
| `PYROSCOPE_ADDR` | `http://pyroscope:4040` | Profiles use the Pyroscope SDK push protocol (**not** OTLP). Point it at Alloy (`http://alloy.monitoring.svc.cluster.local:4040`), which forwards to Pyroscope, or directly at Pyroscope |
| `ENABLE_PROFILING` | off | `1` starts the Pyroscope SDK **and** exposes `/debug/pprof/*` on the metrics port |

Notes:

- All six downstream addresses are **plain `host:port`** values (no scheme). Defaults are short names that only resolve if everything is in the same namespace; across namespaces use `svc.<namespace>.svc.cluster.local:<port>`.
- The Pyroscope application name is hard-coded to `checkout-service` (not driven by `OTEL_SERVICE_NAME`).
- Connections use `grpc.NewClient`, which is **lazy**: the service starts and reports `SERVING` even when none of the downstreams exist. Failures surface per request, not at startup. (Older docs said all six must be up at boot — that is not the case.)
- The gRPC client connections use `pick_first` load balancing and are plaintext/insecure.
- Telemetry is **pushed**, not scraped: traces, metrics and logs share one OTLP/gRPC endpoint (Alloy); profiles go to Pyroscope (via Alloy). `service.instance.id` is the pod hostname, so replicas stay distinct in Mimir.
- A Prometheus `/metrics` endpoint is still served on `METRICS_PORT` for debugging, but nothing has to scrape it — the same registry is bridged into OTLP, so metric names are unchanged.
- Logs: the `logrus` logger keeps writing JSON to stdout and additionally mirrors every record over OTLP (with trace ids when a context is attached).

## Container image

Multi-stage [`Dockerfile`](Dockerfile):

| Stage | Base | What happens |
|---|---|---|
| builder | `golang:1.25-alpine` | `go mod download`, then `CGO_ENABLED=0 GOOS=linux GOARCH=amd64 go build -ldflags="-s -w" ./cmd/server` |
| runtime | `alpine:3.21` | single binary at `/app/checkout-service` |

- **Entrypoint:** `/app/checkout-service`. `GOTRACEBACK=single`.
- **User: root.** The Dockerfile has no `USER`, and the image runs as UID 0 (verified). It needs no privileges (ports > 1024, no file writes), so set a non-root `runAsUser` in Kubernetes (e.g. `10001`) — no image change required.
- **Architecture: `amd64` only** — `GOARCH=amd64` is hard-coded. On `arm64` nodes the pod fails with `exec format error`. Remove the flag (or use `buildx` with `TARGETARCH`) for multi-arch.
- **EXPOSE:** `5050`, `9090`. **No `HEALTHCHECK`.**
- **Image size:** ~10 MB. Alpine includes a shell, so `kubectl exec` works.
- `.dockerignore` excludes `vendor/` only; `.git` and `tests/` are sent in the build context.

## Kubernetes deployment notes

**Workload:** stateless `Deployment`. Multiple replicas are safe, but note that the *outbound* gRPC clients pin each connection to a single backend pod (see "gRPC load balancing" in the master README), and inbound traffic from `frontend` is likewise per-connection.

**Ports to declare**

| Name | Port | Protocol | Expose via Service? |
|---|---|---|---|
| `grpc` | `5050` | TCP (HTTP/2) | Yes — `frontend` dials `checkout-service:5050` |
| `metrics` | `9090` | TCP (HTTP/1.1) | Optional (debugging/pprof). Not scraped by default — metrics are pushed over OTLP |

**Service DNS name expected by callers:** `checkout-service` (`frontend`: `CHECKOUT_SERVICE_ADDR=checkout-service:5050`).

**Probes** — the health endpoint is static, so probes only prove the process is up:

```yaml
readinessProbe:
  grpc: { port: 5050 }
  periodSeconds: 5
livenessProbe:
  grpc: { port: 5050 }
  periodSeconds: 10
  failureThreshold: 3
```

Startup is near-instant; no `startupProbe` needed.

**Security context**

```yaml
securityContext:
  runAsNonRoot: true
  runAsUser: 10001        # image runs as root by default; any non-zero UID works
  runAsGroup: 10001
  readOnlyRootFilesystem: true     # static Go binary, writes nothing
  allowPrivilegeEscalation: false
  capabilities: { drop: ["ALL"] }
```

**Resources** — requests sized from measured idle usage in the test stack (see the chart's `values.yaml` for limits):

```yaml
resources:
  requests: { cpu: 50m, memory: 48Mi }
  limits:   { cpu: "500m", memory: 128Mi }
```

**Config / secrets:** no secrets. All six downstream addresses + observability endpoints belong in values (`dependencies.*`, `monitoring.*`) rendered into env.

**Telemetry:** pushed, not scraped — OTLP/gRPC to Alloy (`global.monitoring.otlpEndpoint`), profiles to Alloy `:4040`. Metrics carry `job="checkout-service"` and `instance=<pod>`; no scrape annotations or `ServiceMonitor` are needed.

**Helm chart:** [`platform-demo-helm/checkout-service`](https://github.com/platform-boutique/platform-demo-helm/tree/main/checkout-service) (built on the shared `platform-common` library chart). It ships with HA defaults — 2 replicas,
HPA, PodDisruptionBudget, topology spread, NetworkPolicy, restricted pod security context, probes and the full env contract (see Configuration) —
and was verified under the `restricted` Pod Security Standard on the earlier local 3-node kind cluster (since retired). The platform now targets AWS EKS, where this chart has not been deployed or verified yet.

**Deployment and admission policies:** Argo CD deploys the chart from `platform-demo-helm` (`envs/<env>/values/checkout-service.yaml` pins the image digest; promotion is a pull request). In the cluster Kyverno enforces, for this service: pods named `app.kubernetes.io/name: checkout-service` that run under the ServiceAccount of the same name (Cilium derives the network identity from that label), an image from this repository pinned by digest and signed by our release pipeline, a read-only root filesystem, resource requests and a memory limit, readiness and liveness probes and no API token. The Helm CI evaluates the rendered chart against these policies before merge; see [`platform-demo-helm/policies`](https://github.com/platform-boutique/platform-demo-helm/tree/main/policies).

## Observability

| Signal | Implementation | Destination |
|---|---|---|
| **Traces** | OpenTelemetry (`otelgrpc`/`otelhttp` handlers), OTLP/gRPC exporter, `AlwaysSample`, W3C TraceContext + Baggage; the trace context is propagated to every downstream call | Alloy `:4317` → Tempo |
| **Metrics** | The Prometheus default registry (`grpc_server_handled_total`, `grpc_server_handling_seconds_*`, `grpc_server_started_total`, `grpc_server_msg_*`, Go runtime/process) is bridged into OpenTelemetry and pushed over OTLP/gRPC every 15 s — metric names are unchanged. `/metrics` is still served for debugging | Alloy → Mimir |
| **Logs** | `logrus` JSON to stdout **and** an OTLP/gRPC hook (`otellogrus`) (the `internal/checkout` logger is hooked too) | Alloy → Loki |
| **Profiles** | Pyroscope Go SDK (CPU, alloc/inuse objects & space) plus pprof HTTP when profiling is on | Alloy `:4040` → Pyroscope |

Every series carries `job="checkout-service"` (from `service.name`) and `instance=<pod name>` (`service.instance.id`), which is what the Grafana dashboards select on. Logs land in Loki with `service_name="checkout-service"` and trace/span ids, so a log line links to its trace.

## Running locally

Requirements: Go 1.25+, Docker, [`grpcurl`](https://github.com/fullstorydev/grpcurl), and the six downstream services (or stand-ins) for a *successful* order.

```bash
go build ./... && go run ./cmd/server

docker build -t checkout-service:local .
docker run --rm -p 5050:5050 -p 9090:9090 \
  -e CART_SERVICE_ADDR=host.docker.internal:7070 \
  -e PRODUCT_CATALOG_SERVICE_ADDR=host.docker.internal:3550 \
  checkout-service:local

go test ./...
```

The simplest full setup is the compose file in `platform-demo-observability/`.

### Manual gRPC testing (reflection is on)

```bash
grpcurl -plaintext -d '{
  "user_id": "test-user",
  "user_currency": "USD",
  "address": {"street_address": "123 Main St", "city": "New York", "state": "NY", "country": "US", "zip_code": 10001},
  "email": "test@example.com",
  "credit_card": {"credit_card_number": "4111111111111111", "credit_card_cvv": 123,
                  "credit_card_expiration_year": 2030, "credit_card_expiration_month": 12}
}' localhost:5050 hipstershop.CheckoutService/PlaceOrder

grpcurl -plaintext localhost:5050 grpc.health.v1.Health/Check
curl -s localhost:9090/metrics | grep grpc_server
```


### Order storage

Optional, switched on by `ORDER_SERVICE_ADDR` (for example `order-service:8085`). Contract: `order-service` `proto/orders.proto`, copied to [`proto/orders/orders.proto`](proto/orders/orders.proto) in its own Go package `checkout-service/proto/orders` (only `go_package` differs; `Money`, `Address` and `OrderItem` would clash with `hipstershop` otherwise).

- **Unset:** `PlaceOrder` behaves as before, no order is stored, order-service is never contacted (the old stack keeps working). The server logs that order storage is disabled.
- **Set:** storing the order is mandatory. The connection is lazy, so checkout starts even if order-service is down, but every `PlaceOrder` then fails with `UNAVAILABLE` **before** the card is charged.

| Situation (with `ORDER_SERVICE_ADDR`) | Calls | `PlaceOrder` result |
|---|---|---|
| Everything works | `CreateOrder`, `Charge`, `ShipOrder`, `ConfirmOrder`, empty cart, email | success, `order.order_id` is the order-service id |
| Validation / cart / product / price / quote fails | none to order-service | as without order storage |
| Email, address or cart outside the order-service limits (see below) | none to order-service | `INVALID_ARGUMENT` (`order: ...`) |
| `CreateOrder` `UNAVAILABLE` / `DEADLINE_EXCEEDED` / `RESOURCE_EXHAUSTED` / `CANCELED` | `CreateOrder` only | `UNAVAILABLE` (`order service is temporarily unavailable, please retry`), nothing charged |
| `CreateOrder` any other code (`INVALID_ARGUMENT`, `ALREADY_EXISTS`, ...) | `CreateOrder` only | `INTERNAL`; logged at error level with the detail (a checkout bug), nothing charged |
| Card declined or any `Charge` error | `CreateOrder`, `Charge`, `FailOrder` | the mapped payment error; `FailOrder` reason = that message |
| `ShipOrder` fails | `CreateOrder`, `Charge`, `ShipOrder`, `Refund`, `FailOrder` | the mapped shipping error + refund note; reason = that message |
| `ConfirmOrder` fails after charge and shipment | `CreateOrder`, `Charge`, `ShipOrder`, `ConfirmOrder` | **success**; logged at error level (`ORDER NOT CONFIRMED`) with order id, transaction id and tracking id; the order stays `PENDING` for manual follow-up |
| `FailOrder` fails | as above | the original error is returned unchanged; the `FailOrder` error is logged (the order stays `PENDING`) |

Details:

- `CreateOrder` carries `session_id` = the request `user_id`, the email, the address field for field, one item per cart line with `product_id`, `name`, `picture` and the unit price in the **user's currency** (the converted price), `shipping_cost` and `total`. The total is the amount that is charged: the same `money` package arithmetic (integer units and nanos, no floats) over the same numbers, so it equals `sum(quantity x unit_price) + shipping_cost` exactly, in one currency. `CreateOrder` has a 5 s timeout and runs on the request context.
- `FailOrder` and `ConfirmOrder` run on a context detached from the caller (10 s timeout), so a caller that gave up does not leave an order `PENDING` after money moved. The `FailOrder` reason is the message of the status returned to the caller (it can contain the last four card digits that payment-service puts in its decline message, never the card number or CVV).
- A `ConfirmOrder` failure is not retried and not turned into an error: the money is taken and the parcel is ordered, so the customer gets the success.
- Limits checked up front (they are order-service's): email 3-254 characters and a plain address; `user_id` at most 128; street 1-200, city 1-100, state at most 100, country 1-100, zip not negative; 1-100 cart lines, quantity 1-1000. A product name over 200 characters is cut, a picture over 500 characters is left out of the snapshot.
- `PlaceOrder` is still not idempotent: each call creates a new order id.

The user's cart must already contain items (add some via `cart-service`), otherwise the order has zero lines and a total of 0.

## Project structure

```
├── cmd/server/main.go         # entrypoint: tracing, profiling, gRPC server, downstream dials, metrics server
├── internal/checkout/         # PlaceOrder orchestration (service.go)
├── internal/money/            # Money arithmetic (Sum, MultiplySlow)
├── proto/                     # checkout.proto + generated Go (checkout.pb.go, checkout_grpc.pb.go)
├── Dockerfile
├── go.mod / go.sum
└── docs/                      # empty placeholder
```

## Known limitations

- **Compensation is best effort:** if shipping fails after the charge, one `Refund` is attempted (no retry). If it fails, the customer stays charged until someone refunds the transaction id found in the error log (the message says the refund is pending). A `Charge` that times out without a response cannot be refunded (no transaction id). `PlaceOrder` is not idempotent: a client retry charges again.
- `Refund` needs a payment-service that implements it (branch `feat/decline-and-refund`); an older payment answers `UNIMPLEMENTED` and the order ends with "refund is pending".
- The order-service integration was tested only against an in-process fake order-service (it re-implements the total rule with `big.Int`); it was **not run** against the real order-service or in the compose stack.
- Health is static; readiness does not reflect downstream availability.
- Image is `amd64`-only and runs as root unless the pod overrides it.
- Single-attempt calls: no retries or timeouts beyond the incoming request context.
