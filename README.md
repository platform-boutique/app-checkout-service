# checkout-service

A gRPC service that orchestrates order placement for the platform-demo e-commerce platform. A single `PlaceOrder` call fans out to six other services — cart, product catalog, currency, shipping, payment and email — to price the order, charge the card, ship it and confirm it. It is the most connected service in the system and holds no state of its own.

Part of the [platform-demo](https://github.com/platform-boutique) platform — microservices, observability (LGTM stack), GitOps (Argo CD), policy enforcement (Kyverno), infrastructure provisioning (Crossplane) and an internal developer portal (Backstage). See the master `README.md` in the platform-demo root for the whole-system view.

## At a glance

| | |
|---|---|
| **Language / runtime** | Go 1.25 (static binary, `CGO_ENABLED=0`) |
| **Image** | `mladenovskistefan/checkout-service` |
| **Protocol / port** | gRPC (HTTP/2, plaintext) on `5050` |
| **Metrics** | Pushed over OTLP/gRPC (Prometheus registry bridged into OpenTelemetry); `/metrics` on `9090` is scraped by Alloy; `build_info` gauge |
| **Health** | Standard `grpc.health.v1.Health/Check` on `5050` — static `SERVING`, does **not** reflect downstream health |
| **State** | None |
| **Depends on** | `product-catalog-service`, `cart-service`, `currency-service`, `shipping-service`, `payment-service`, `email-service`; optional: `order-service`, `inventory-service`, Kafka (`KAFKA_BROKERS`, produces `orders.placed`) |
| **Called by** | `frontend` |
| **gRPC reflection** | Enabled — `grpcurl` works without `-proto` |

## API

Package `hipstershop`, service `CheckoutService` ([`proto/checkout.proto`](proto/checkout.proto)). The proto file also contains client-side copies of the downstream service definitions.

| Method | Request | Response |
|---|---|---|
| `PlaceOrder` | `PlaceOrderRequest{ user_id, user_currency, Address address, email, CreditCardInfo credit_card, account_id, idempotency_key }` | `PlaceOrderResponse{ OrderResult order }` (`order_id`, `shipping_tracking_id`, `shipping_cost`, `shipping_address`, `items[]`) |

### What `PlaceOrder` does (in order)

1. Reject requests without `user_id`, `user_currency`, address or credit card, with an `account_id` over 128 characters, or with an `idempotency_key` that is not 0-64 characters of `[A-Za-z0-9._-]` (`INVALID_ARGUMENT`), before any downstream call.
2. Order id: a random v4 UUID, or, with an `idempotency_key` and order storage, `UUIDv5(namespace, account-or-cart-key NUL key)`; **with a key the stored order is looked up first** (`order-service.GetOrder`) and a PLACED order is returned as stored, see [Idempotency](#idempotency). It is the `order_id` of the `OrderResult` (and the id of the stock reservation).
3. `cart-service.GetCart(user_id)`; an empty cart is `FAILED_PRECONDITION`.
4. For every cart line, **in parallel**: `product-catalog-service.GetProduct` then `currency-service.Convert` (USD -> `user_currency`).
5. `shipping-service.GetQuote` (USD) then `currency-service.Convert` to `user_currency`.
6. Sum items x quantity + shipping.
7. **Only if `ORDER_SERVICE_ADDR` is set:** `order-service.CreateOrder` stores the order as `PENDING` (see [Order storage](#order-storage)). If that fails, nothing is charged.
8. **Only if `INVENTORY_SERVICE_ADDR` is set:** `inventory-service.Reserve(order_id, lines)` holds the stock, all or nothing (see [Stock reservation](#stock-reservation)). A shortage is `FAILED_PRECONDITION`, an unreachable inventory is `UNAVAILABLE`; either way the order is marked `FAILED` and nothing is charged.
9. `payment-service.Charge(total, credit_card)` - failure aborts the order: `Release` the stock, mark the order `FAILED` (nothing is shipped, emptied or mailed).
10. `shipping-service.ShipOrder`. If it fails, the charge is **compensated** in this order: `payment-service.Refund(transaction_id, total)` is called once (on a context detached from the caller, 10 s timeout), then `Release` of the stock, then `FailOrder`; the shipping error is returned with the note "your payment has been refunded". If the refund itself fails, the original error is kept, the failure is logged at error level with the transaction id, and the message says the refund is pending (with the transaction id as reference).
11. **Only with inventory:** `inventory-service.Commit(order_id)`, 3 attempts (3 s each, pauses of 1 s then 3 s), on a detached context. If it still fails the order goes on and `STOCK NOT COMMITTED order=<id>` is logged at error level (known gap).
12. **Only if `KAFKA_BROKERS` is set:** publish the `orders.placed` event (key = order id, `acks=all`, idempotent producer, 5 s bound, see [Order events](#order-events-kafka)). It runs after Ship and Commit and before `ConfirmOrder`. A failure or timeout is logged (`EVENT NOT PUBLISHED order=<id>`, error level) and **never fails or delays the order** beyond the bound (known gap: no outbox).
13. **Only with order storage:** `order-service.ConfirmOrder(order_id, tracking_id, transaction_id)`; the order becomes `PLACED`. 3 attempts (3 s each, pauses of 0.5 s then 2 s) on a detached context; a final answer (`FAILED_PRECONDITION`, `NOT_FOUND`, `INVALID_ARGUMENT`) is not retried. If every attempt fails the order goes on, `ORDER NOT CONFIRMED order=<id>` is logged at error level and the order stays `PENDING` (known gap).
14. `cart-service.EmptyCart` - error ignored.
15. `email-service.SendOrderConfirmation` - error logged, **never fails the order**. **Skipped when `KAFKA_BROKERS` is set**: the email consumer sends the confirmation from the event.

Everything that can fail without side effects (steps 1-6) runs before the card is charged. Release, Refund and FailOrder are best effort: their own failure is logged and never replaces the error `PlaceOrder` returns.

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

Errors of the saga and idempotency (own messages, not passed through from a downstream):

| Situation | `PlaceOrder` code | Message |
|---|---|---|
| `Reserve` answered `reserved = false` | `FAILED_PRECONDITION` | `out of stock: "Wool Scarf" (only 2 left, you asked for 3), "Tote Bag" (sold out)` (every short product by name, in cart order, at most 10 listed then `and N more`) |
| `Reserve` RPC fails (any error, inventory down) | `UNAVAILABLE` | `stock check unavailable, try again` |
| invalid `idempotency_key` | `INVALID_ARGUMENT` | `order: idempotency_key must be at most 64 characters of A-Z a-z 0-9 . _ -` |
| same key, stored order still `PENDING`, or the same key in flight in this process | `ABORTED` | `this order is already being processed` |
| same key, stored order `FAILED` | `FAILED_PRECONDITION` | `this attempt failed, start the checkout again` |
| same key, order-service lookup fails | `UNAVAILABLE` / `INTERNAL` | as for `CreateOrder` |

## Configuration

| Variable | Default | Description |
|---|---|---|
| `PORT` | `5050` | gRPC listen port |
| `PRODUCT_CATALOG_SERVICE_ADDR` | `product-catalog-service:3550` | `host:port` of product-catalog (gRPC) |
| `CART_SERVICE_ADDR` | `cart-service:7070` | `host:port` of cart |
| `CURRENCY_SERVICE_ADDR` | `currency-service:7000` | `host:port` of currency |
| `SHIPPING_SERVICE_ADDR` | `shipping-service:50051` | `host:port` of shipping |
| `PAYMENT_SERVICE_ADDR` | `payment-service:50051` | `host:port` of payment |
| `EMAIL_SERVICE_ADDR` | `email-service:8080` | `host:port` of email |
| `ORDER_SERVICE_ADDR` | unset (orders are not stored) | `host:port` of order-service. Set: the order is stored before the card is charged and `idempotency_key` works (see Order storage) |
| `INVENTORY_SERVICE_ADDR` | unset (no stock reservation) | `host:port` of inventory-service, e.g. `inventory-service:8087`. Set: stock is reserved before the charge and committed after the shipment, fail closed (see Stock reservation) |
| `KAFKA_BROKERS` | unset (no event) | Comma separated seed brokers of Kafka, e.g. `kafka:9092`. Set: `orders.placed` is published after Ship and Commit (best effort, 5 s bound) and checkout no longer calls email-service; empty: today's behaviour with the direct email call. The broker may be down at startup (lazy connect). See Order events |
| `METRICS_PORT` | `9090` | Port of the Prometheus `/metrics` endpoint (scraped by Alloy). A value that is not a port falls back to `9090` with a warning |
| `PPROF_ADDR` | unset (no pprof) | `host:port` of a separate pprof listener, e.g. `127.0.0.1:6060`. Off unless set; never expose it. pprof is never served on the metrics port |
| `LOG_LEVEL` | `info` | `debug`, `info`, `warn` or `error`; anything else falls back to `info` with a warning |
| `SERVICE_VERSION` | build info, else `dev` | `service.version`, the `version` log field and the `build_info` label. Fallbacks in order: ldflags `-X <module>/internal/obs.Version=...` (and `.Commit=`, `.BuildTime=`), the VCS revision Go embeds in the binary (12 characters, `-dirty` if modified), `dev` |
| `DEPLOYMENT_ENVIRONMENT` | `local` | `deployment.environment` on every signal and the Pyroscope tag `environment` |
| `OTEL_EXPORTER_OTLP_ENDPOINT` | unset (telemetry off) | OTLP/**gRPC** endpoint for **traces, metrics and logs**, e.g. `http://alloy:4317`. Telemetry is on only if this is a valid absolute http(s) URL; empty or malformed means off (one warning, never a crash). The legacy `COLLECTOR_SERVICE_ADDR` and `ENABLE_TRACING` are no longer read |
| `OTEL_SDK_DISABLED` | unset | `true` (trimmed, case-insensitive) forces everything off: OTLP export and profiling, even if an endpoint or `PYROSCOPE_ADDR` is set |
| `OTEL_EXPORTER_OTLP_PROTOCOL` | `grpc` | Documented knob; only gRPC is implemented, the value is not read |
| `OTEL_TRACES_SAMPLER`, `OTEL_TRACES_SAMPLER_ARG` | SDK default (`parentbased_always_on`) | Standard OpenTelemetry sampler variables, read by the SDK (tested for `always_on` and `always_off`) |
| `OTEL_RESOURCE_ATTRIBUTES` | unset | Extra resource attributes (`k=v,...`); applied after the defaults, so they can override them |
| `OTEL_SERVICE_NAME` | `checkout-service` | `service.name`; keep the default, dashboards depend on it |
| `OTEL_METRIC_EXPORT_INTERVAL` | `15000` | Milliseconds between metric pushes (also accepts durations such as `15s`) |
| `PYROSCOPE_ADDR` | unset (profiling off) | Pyroscope URL (SDK push protocol, not OTLP). The only switch: no default address, `ENABLE_PROFILING=1` alone starts nothing |
| `ENABLE_PROFILING` | unset | `0` forces profiling off even with `PYROSCOPE_ADDR`; any other value changes nothing |

Notes:

- All downstream addresses are **plain `host:port`** values (no scheme). Defaults are short names that only resolve if everything is in the same namespace; across namespaces use `svc.<namespace>.svc.cluster.local:<port>`.
- The Pyroscope application name follows `service.name` (`OTEL_SERVICE_NAME`, default `checkout-service`).
- Connections use `grpc.NewClient`, which is **lazy**: the service starts and reports `SERVING` even when none of the downstreams exist. Failures surface per request, not at startup. (Older docs said all six must be up at boot — that is not the case.)
- The gRPC client connections use `pick_first` load balancing and are plaintext/insecure.
- Traces, metrics and logs share one OTLP/gRPC endpoint (Alloy); profiles go to Pyroscope. `service.instance.id` is the pod hostname, so replicas stay distinct in Mimir. Metrics are also scraped from `:9090/metrics`.
- `/metrics` on `METRICS_PORT` (default 9090) serves the Prometheus registry plus `build_info`; the same registry is bridged into OTLP, so metric names are the same on both paths.
- Logs: JSON on stdout in the shape described under Observability, mirrored over OTLP when telemetry is on. Request-path code logs with `log.WithContext(ctx)` so the line carries `trace_id`/`span_id`.

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
| `metrics` | `9090` | TCP (HTTP/1.1) | Prometheus `/metrics` (scraped by Alloy in the local stack; the same metrics are also pushed over OTLP). pprof is not served here |

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
| **Traces** | OpenTelemetry (`otelgrpc` handlers), OTLP/gRPC exporter (batch processor, queue bounded at 2048), W3C TraceContext + Baggage propagated to every downstream call. Sampler from `OTEL_TRACES_SAMPLER` (SDK default `parentbased_always_on`) | Alloy `:4317` -> Tempo |
| **Metrics** | Prometheus default registry: `grpc_server_handled_total`, `grpc_server_handling_seconds_*`, `grpc_server_started_total`, `grpc_server_msg_*`, `checkout_order_events_total{result}`, `checkout_order_event_publish_seconds`, Go runtime/process. Served as Prometheus text on `:9090/metrics` (`METRICS_PORT`) and, because the same registry is bridged into OpenTelemetry, also pushed over OTLP every 15 s (metric names identical). Plus `build_info{service,version,commit} 1` | Alloy scrapes `:9090` -> Mimir |
| **Logs** | `logrus`, one JSON object per line on stdout, plus an OTLP/gRPC hook (`otellogrus`) | stdout -> Alloy -> Loki; OTLP -> Alloy -> Loki |
| **Profiles** | Pyroscope Go SDK (CPU, alloc/in-use objects and space, goroutines, mutex, block), only with `PYROSCOPE_ADDR`; tags `service_name`, `version`, `environment` | Pyroscope |

**Telemetry contract** (the code is `internal/obs`, the same package in every Go service): telemetry is on only if `OTEL_EXPORTER_OTLP_ENDPOINT` is a valid absolute http(s) URL; `OTEL_SDK_DISABLED=true` forces everything off (profiling too); profiling only with `PYROSCOPE_ADDR`. Start never waits for the collector (the exporters connect in the background) and on SIGTERM the flush is cut off after 4 s, so a blackholed collector cannot hold the process (unit-tested with a listener that never answers: start and shutdown both return within the bound).

**Resource attributes** on every signal: `service.name` (`OTEL_SERVICE_NAME`, default `checkout-service`), `service.version` (`SERVICE_VERSION`, else build info, else `dev`), `service.namespace=everly`, `deployment.environment` (`DEPLOYMENT_ENVIRONMENT`, default `local`), `service.instance.id` (hostname). `OTEL_RESOURCE_ATTRIBUTES` is applied last.

**Log shape** (one shape for all services): `timestamp` (RFC 3339, UTC, milliseconds), `severity` (`debug`, `info`, `warning`, `error`), `message`, `service`, `version`, `trace_id` and `span_id` (hex, only while a valid span is active, never empty or all zeros), `request_id` (when the caller sent one), `error` (string, on errors), plus the call's own fields (for example `order_id`). Level from `LOG_LEVEL`. `trace_id`/`span_id` come from the context of the log call: handlers and clients log with `log.WithContext(ctx)`, the formatter reads the span from it. Calls without a context (startup, background work) have no ids. No card data, tokens or e-mail addresses are logged.

**Startup and version**: the first line is `message: "starting"` with `version`, `commit` and `environment`; `version`/`commit` come from `SERVICE_VERSION`, ldflags `-X <module>/internal/obs.Version=` / `.Commit=`, or the VCS data Go embeds (no Dockerfile change was made: without any of them a container image reports `version: "dev"`, `commit: "unknown"`). Panics are logged as one JSON line (`message: "panic"`, `error`) before the process dies; a startup error is one JSON error line, exit 1.

**pprof** is off. `PPROF_ADDR=127.0.0.1:6060` starts a separate listener; it is never on the metrics port.

**Kafka trace propagation** (`orders.placed`): the producer starts a PRODUCER span `orders.placed publish` (child of the PlaceOrder server span; `messaging.system=kafka`, `messaging.destination.name`, `messaging.operation=publish`) and writes W3C `traceparent` (and `tracestate`/`baggage` when present) into the record headers next to `event-type`, `content-type` and `schema-version`. order-service and email-service continue the trace from them. Batches stay uncompressed. Tested: the record built for the broker carries the producer span's traceparent (`TestBuildRecordInjectsTraceContext`); the end-to-end trace through Kafka is **not run** here (needs the stack).

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
| Everything works | `CreateOrder`, `Charge`, `ShipOrder`, `ConfirmOrder`, empty cart, email (or the event, see Order events) | success, `order.order_id` is the order-service id |
| Validation / cart / product / price / quote fails | none to order-service | as without order storage |
| Email, address or cart outside the order-service limits (see below) | none to order-service | `INVALID_ARGUMENT` (`order: ...`) |
| `CreateOrder` `UNAVAILABLE` / `DEADLINE_EXCEEDED` / `RESOURCE_EXHAUSTED` / `CANCELED` | `CreateOrder` only | `UNAVAILABLE` (`order service is temporarily unavailable, please retry`), nothing charged |
| `CreateOrder` any other code (`INVALID_ARGUMENT`, `ALREADY_EXISTS`, ...) | `CreateOrder` only | `INTERNAL`; logged at error level with the detail (a checkout bug), nothing charged |
| Card declined or any `Charge` error | `CreateOrder`, `Charge`, `FailOrder` | the mapped payment error; `FailOrder` reason = that message |
| `ShipOrder` fails | `CreateOrder`, `Charge`, `ShipOrder`, `Refund`, `FailOrder` | the mapped shipping error + refund note; reason = that message |
| `ConfirmOrder` fails after charge and shipment | `CreateOrder`, `Charge`, `ShipOrder`, `ConfirmOrder` x3 | **success**; `ConfirmOrder` is tried 3 times, then `ORDER NOT CONFIRMED order=<id>` is logged at error level (with transaction id and tracking id); the order stays `PENDING` for manual follow-up |
| `FailOrder` fails | as above | the original error is returned unchanged; the `FailOrder` error is logged (the order stays `PENDING`) |

Details:

- `CreateOrder` carries `session_id` = the request `user_id` (the cart key) and `user_id` = the request `account_id` (empty for an anonymous order), the email, the address field for field, one item per cart line with `product_id`, `name`, `picture` and the unit price in the **user's currency** (the converted price), `shipping_cost` and `total`. The total is the amount that is charged: the same `money` package arithmetic (integer units and nanos, no floats) over the same numbers, so it equals `sum(quantity x unit_price) + shipping_cost` exactly, in one currency. `CreateOrder` has a 5 s timeout and runs on the request context.
- `FailOrder` and `ConfirmOrder` run on a context detached from the caller (10 s timeout), so a caller that gave up does not leave an order `PENDING` after money moved. The `FailOrder` reason is the message of the status returned to the caller (it can contain the last four card digits that payment-service puts in its decline message, never the card number or CVV).
- A `ConfirmOrder` failure is not retried and not turned into an error: the money is taken and the parcel is ordered, so the customer gets the success.
- Limits checked up front (they are order-service's): email 3-254 characters and a plain address; `user_id` at most 128; street 1-200, city 1-100, state at most 100, country 1-100, zip not negative; 1-100 cart lines, quantity 1-1000. A product name over 200 characters is cut, a picture over 500 characters is left out of the snapshot.
- `account_id` (field 7, optional, max 128 characters) is the id of a signed-in buyer (user-service). It is trimmed and passed as `CreateOrder.user_id` so the order belongs to the account from the start; empty means an anonymous order exactly as before. `user_id` keeps its meaning (cart key: the cart is read and emptied under it, and it is the order's `session_id`). Checkout does not verify the account id (it trusts its caller, the frontend, like it trusts `user_id`) and with `ORDER_SERVICE_ADDR` unset the field is validated but has no effect. Orders placed anonymously are handed to an account later by order-service's `ClaimOrders`, which the frontend calls after login (checkout does not need that RPC, so its copy of `orders.proto` is unchanged).
- `PlaceOrder` is idempotent only with an `idempotency_key`, see [Idempotency](#idempotency); without one every call creates a new order id.

### Stock reservation

Optional, switched on by `INVENTORY_SERVICE_ADDR` (for example `inventory-service:8087`). Contract: `inventory-service` `proto/inventory.proto`, copied to [`proto/inventory/inventory.proto`](proto/inventory/inventory.proto) in its own Go package `checkout-service/proto/inventory` (only `go_package` differs).

- **Unset:** no reserve, commit or release, inventory-service is never contacted: today's behaviour. The server logs that stock reservation is disabled.
- **Set:** a reservation is mandatory (fail closed, never sell without one). The connection is lazy: with inventory-service down every `PlaceOrder` fails with `UNAVAILABLE` before the card is charged. It works with or without `ORDER_SERVICE_ADDR`.
- The reservation lines are the cart lines, one per product (quantities of repeated lines are added up); the reservation id is the order id.

| Situation (with `INVENTORY_SERVICE_ADDR` and `ORDER_SERVICE_ADDR`) | Calls | `PlaceOrder` result |
|---|---|---|
| Everything works | `CreateOrder`, `Reserve`, `Charge`, `ShipOrder`, `Commit`, `ConfirmOrder`, empty cart, email | success |
| `Reserve` short | `CreateOrder`, `Reserve`, `FailOrder` | `FAILED_PRECONDITION` `out of stock: ...`; nothing charged, nothing held so no `Release` |
| `Reserve` RPC error | `CreateOrder`, `Reserve`, `Release`, `FailOrder` | `UNAVAILABLE`; nothing charged; `Release` because the request may have arrived although the answer did not |
| `Charge` fails | `CreateOrder`, `Reserve`, `Charge`, `Release`, `FailOrder` | the mapped payment error, no refund |
| `ShipOrder` fails | `CreateOrder`, `Reserve`, `Charge`, `ShipOrder`, `Refund`, `Release`, `FailOrder` | the mapped shipping error + refund note |
| `Commit` fails 3 times | `..., ShipOrder`, `Commit` x3, `ConfirmOrder` | **success**; `STOCK NOT COMMITTED order=<id>` logged at error level |
| `Release` fails | as above | the original error, unchanged; `STOCK NOT RELEASED` logged (3 attempts; the hold then expires by the TTL of inventory-service) |

`Release` (5 s per attempt) and `Commit` (3 s per attempt) run on a context detached from the caller, so a customer who closed the tab while the card was being charged does not leave stock held (or sold but uncommitted). `Reserve` runs on the request context with a 5 s timeout.

### Idempotency

`PlaceOrderRequest.idempotency_key` (field 8, optional) makes a retried or double-submitted `PlaceOrder` safe. It needs order storage (the stored order is the record of what happened); without `ORDER_SERVICE_ADDR` the key is validated and otherwise ignored (random order id, as before).

- The order id is `UUIDv5(fixed namespace, owner + "\x00" + key)`, owner = `account_id` when set, else `user_id` (the cart key), so two visitors using the same key never collide.
- Before anything else `GetOrder(order_id)` is called. Not found: a new attempt, the normal flow (`CreateOrder` is idempotent by order id). `PLACED` or later (`PROCESSING`, `SHIPPED`, `DELIVERED`): the stored result is returned (`order_id`, tracking id, shipping cost and address, lines with quantity and unit price) **without** charging, reserving, shipping, emptying the cart or sending the email again; it does not depend on the cart (which was emptied). `PENDING`: `ABORTED` `this order is already being processed`. `FAILED`: `FAILED_PRECONDITION` `this attempt failed, start the checkout again`; a new attempt needs a **new key**.
- Two replicas receiving the same key at the same moment: both lookups can miss, but `CreateOrder` (order-service with `Order.created`) answers `created = false` to all but the one that inserted the row; that call takes the same branches as the lookup above (`PENDING` -> `ABORTED`, `PLACED` or later -> stored result, `FAILED` -> `FAILED_PRECONDITION`) and reserves, charges and ships nothing. An order-service that does not send the field (unset = unknown) leaves this race open, see [Known limitations](#known-limitations).
- The same key arriving twice at once in one checkout process: the second call gets `ABORTED` immediately (in-memory guard), exactly one charge. After the first finished, the same key replays the stored order.
- A lookup failure (order-service down) is `UNAVAILABLE`: checkout does not guess.
- An order stuck `PENDING` (a crash, `FailOrder` or `ConfirmOrder` failing) answers `ABORTED` for its key forever; the customer needs a new key and the order needs manual follow-up.

### Order events (Kafka)

Optional, switched on by `KAFKA_BROKERS` (comma separated seed brokers, for example `kafka:9092`). The topic is `orders.placed` (3 partitions, replication 1, auto-created on first use by the broker config of the compose stack). Client: `github.com/twmb/franz-go` (`pkg/kgo`), pure Go.

- **Unset or empty:** no event, Kafka is never contacted, and `PlaceOrder` behaves as before **including the direct `email-service.SendOrderConfirmation` call**.
- **Set:** after `ShipOrder` and `Commit` succeeded and before checkout's own `ConfirmOrder`, one record is produced: key = the order id, headers `event-type=order.placed`, `content-type=application/json`, `schema-version=1`, `acks=all`, idempotent producer, a 5 s delivery timeout (the whole step is bounded by 5 s, also for a client that hangs). Checkout then **no longer calls email-service**: the email consumer sends the confirmation from the event. Order-service's consumer uses the same event to confirm an order whose `ConfirmOrder` was missed.
- The producer is created at startup; franz-go connects lazily, so a broker that is down at startup does not stop checkout and produce errors are per call. On SIGTERM/SIGINT the gRPC server drains (at most 15 s), then the producer is flushed (at most 5 s) and closed.
- A publish failure (broker down, timeout, ...) is logged at error level (`EVENT NOT PUBLISHED order=<id>` with transaction id and tracking id) and never fails the order: the customer is charged and the parcel is ordered. Metrics: `checkout_order_events_total{result="published|failed"}`, `checkout_order_event_publish_seconds`. The payload (email, address) is never logged.
- Failed orders (declined, out of stock, shipping failed) publish nothing. An idempotent replay of a stored `PLACED` order (same `idempotency_key`) **never republishes**.
- Delivery is at-least-once; consumers must be idempotent by `event_id` (`UUIDv5(UUIDv5(URL namespace, "everly/orders.placed"), order_id)`, so a republish of the same order would carry the same id) or `order_id`.

Value (JSON, UTF-8, schema version 1; every field is always present, an unknown one is `""` or `0`):

```json
{
  "event_id": "f9ebed3d-a36c-57ea-87c0-f0572344656e",
  "event_type": "order.placed",
  "schema_version": 1,
  "occurred_at": "2026-10-31T18:00:00.123Z",
  "order_id": "0d4c1a54-7a52-4a58-9a5e-0e5b5b1f3a11",
  "user_id": "account id or empty string",
  "email": "customer@example.com",
  "shipping_address": {"street_address": "1 Main St", "city": "Testville", "state": "NY", "country": "US", "zip_code": 10001},
  "items": [{"product_id": "prod-1", "name": "Test Widget", "quantity": 2, "unit_price": {"currency_code": "USD", "units": 12, "nanos": 990000000}, "picture": "/static/x.jpg"}],
  "shipping_cost": {"currency_code": "USD", "units": 5, "nanos": 0},
  "total": {"currency_code": "USD", "units": 30, "nanos": 980000000},
  "shipping_tracking_id": "TRK-1",
  "payment_transaction_id": "TX-1"
}
```

Money is the same integer units + nanos (in the user's currency) that were charged and stored in order-service. `user_id` is the account id (empty for an anonymous order), **never** the cart/session key; card data and CVV are never in the event.

Known gaps: there is **no outbox**. Publishing is best effort and happens in the request: if the broker is down or slow the event is lost (only the error log remembers it), the order-service consumer does not learn of the order (checkout's own `ConfirmOrder` still runs) and, because checkout no longer calls email-service when Kafka is on, **that customer gets no confirmation email**. Nothing re-publishes later.

The user's cart must already contain items (add some via `cart-service`), otherwise the order has zero lines and a total of 0.

### Kafka integration tests

`internal/checkout/kafka_integration_test.go` (build tag `integration`) publishes a real event and reads it back, and checks that `PlaceOrder` still succeeds within the bound with the broker down. It needs a throwaway single-node Kafka reachable at `KAFKA_TEST_BROKERS` (default `127.0.0.1:19094`):

```bash
docker run -d --rm --name d1-kafka --hostname d1-kafka --memory 768m \
  -e KAFKA_NODE_ID=1 -e KAFKA_PROCESS_ROLES=broker,controller \
  -e KAFKA_LISTENERS=PLAINTEXT://:9092,CONTROLLER://:9093 -e KAFKA_ADVERTISED_LISTENERS=PLAINTEXT://127.0.0.1:19094 \
  -e KAFKA_CONTROLLER_LISTENER_NAMES=CONTROLLER -e KAFKA_LISTENER_SECURITY_PROTOCOL_MAP=CONTROLLER:PLAINTEXT,PLAINTEXT:PLAINTEXT \
  -e KAFKA_CONTROLLER_QUORUM_VOTERS=1@localhost:9093 -e KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR=1 \
  -e KAFKA_TRANSACTION_STATE_LOG_REPLICATION_FACTOR=1 -e KAFKA_TRANSACTION_STATE_LOG_MIN_ISR=1 \
  -e KAFKA_GROUP_INITIAL_REBALANCE_DELAY_MS=0 -e KAFKA_NUM_PARTITIONS=3 -e KAFKA_HEAP_OPTS="-Xms256m -Xmx384m" \
  -p 127.0.0.1:19094:9092 apache/kafka:4.3.2     # ~40 s to start
go test -tags integration -race -count=1 ./internal/checkout/ -run TestKafka
docker rm -f d1-kafka
```

The unit tests (`events_test.go`) use a fake publisher and need nothing.

## Project structure

```
├── cmd/server/main.go         # entrypoint: tracing, profiling, gRPC server, downstream dials, metrics server
├── internal/checkout/         # PlaceOrder orchestration (service.go), order storage (orders.go), stock saga (inventory.go), idempotency (idempotency.go), order events (events.go, Kafka producer in kafka.go)
├── internal/money/            # Money arithmetic (Sum, MultiplySlow)
├── proto/                     # checkout.proto + generated Go; orders/ and inventory/ = vendored contracts of order- and inventory-service
├── Dockerfile
├── go.mod / go.sum
└── docs/                      # empty placeholder
```

## Known limitations

- **Compensation is best effort:** if shipping fails after the charge, one `Refund` is attempted (no retry). If it fails, the customer stays charged until someone refunds the transaction id found in the error log (the message says the refund is pending). A `Charge` that times out without a response cannot be refunded (no transaction id). Without an `idempotency_key` (and order storage) `PlaceOrder` is not idempotent: a client retry charges again.
- `Refund` needs a payment-service that implements it (branch `feat/decline-and-refund`); an older payment answers `UNIMPLEMENTED` and the order ends with "refund is pending".
- The order-service integration was tested only against an in-process fake order-service (it re-implements the total rule with `big.Int`); it was **not run** against the real order-service or in the compose stack.
- **STOCK NOT COMMITTED:** if `Commit` fails 3 times (3 s per attempt, pauses of 1 s and 3 s) after charge and shipment, the order still succeeds and `STOCK NOT COMMITTED order=<id>` is logged at error level. The reservation then expires by the TTL of inventory-service (default 15 m) and that stock can be sold a second time. Nothing re-tries the commit later.
- **No reconciliation:** nothing compares orders, payments and reservations afterwards. Orders stuck `PENDING`, charges whose refund is pending, reservations that were never released or committed (`STOCK NOT RELEASED`) need manual follow-up from the logs. A reservation can also expire (TTL) while the card is being charged; `Commit` then fails and the stock may be oversold.
- **Idempotency across replicas needs order-service with `created`:** the in-memory guard against a concurrent double submit is per process. Across checkout replicas the race (both look the order up, both miss, both create) is closed by `Order.created` (`optional bool`, field 20) in the `CreateOrder` answer: only the call that inserted the row gets `true`, and checkout stops on `false` (PENDING -> `ABORTED`, PLACED or later -> stored result, FAILED -> `FAILED_PRECONDITION`) without reserving, charging or shipping. This holds only against an order-service that sends the field (branch `feat/created-flag` of order-service, local only: not merged, built or deployed yet). Against an older order-service the field is unset, checkout treats that as unknown and falls back to the GetOrder lookup alone, so two replicas receiving the same key within milliseconds could both charge. Still open with the new order-service: an order stuck `PENDING` (crash, failing `ConfirmOrder` or `FailOrder`) answers `ABORTED` for its key forever (no reconciliation), and the guarantee was tested only with fakes (two `Service` instances sharing one fake order-service, and the real order-service's `created` against Postgres separately), never with two real checkout replicas. A sequential retry is always safe (it goes through the stored order). Without `ORDER_SERVICE_ADDR` there is no idempotency.
- The inventory integration was tested only against in-process fake order-, payment-, shipping- and inventory-services; it was **not run** against the real inventory-service or in the compose stack.
- **ORDER NOT CONFIRMED:** `ConfirmOrder` is tried 3 times (3 s per attempt, pauses of 0.5 s and 2 s, detached context). If all fail, `ORDER NOT CONFIRMED order=<id>` is logged at error level, the customer still gets the success (charged and shipped) and the order stays `PENDING` in order-service: it is not in the customer's order history, its key answers `ABORTED`, and nothing re-tries or reconciles it later; it needs manual follow-up from the log.
- **EVENT NOT PUBLISHED (no outbox):** with `KAFKA_BROKERS` set the `orders.placed` event is published inside the request, best effort, with a 5 s bound. If the broker is down or slow the order still succeeds, `EVENT NOT PUBLISHED order=<id>` is logged at error level and nothing publishes the event later. Consequences: order-service's consumer cannot reconcile that order, and since checkout does not call email-service when Kafka is on, the customer gets **no confirmation email**. A crash between the commit of the stock and the publish loses the event the same way. A timed-out publish may still reach the broker later (at-least-once; consumers are idempotent by `event_id`).
- The Kafka producer was tested with a fake publisher and against a single throwaway `apache/kafka:4.3.2` node (publish and read back, same key on one partition, broker unreachable); it was **not run** in the compose stack, against a broker that goes away mid-run, with TLS/SASL (the compose broker is plaintext without auth) or with several checkout replicas.
- Health is static; readiness does not reflect downstream availability.
- Image is `amd64`-only and runs as root unless the pod overrides it.
- Retries exist only for `Commit`, `Release` and `ConfirmOrder` (3 attempts each; the Kafka publish is one attempt chain bounded by 5 s); every other call is a single attempt with no timeout beyond the incoming request context (except the ones named in the docs).
