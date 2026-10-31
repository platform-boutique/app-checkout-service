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
12. **Only with order storage:** `order-service.ConfirmOrder(order_id, tracking_id, transaction_id)`; the order becomes `PLACED`. 3 attempts (3 s each, pauses of 0.5 s then 2 s) on a detached context; a final answer (`FAILED_PRECONDITION`, `NOT_FOUND`, `INVALID_ARGUMENT`) is not retried. If every attempt fails the order goes on, `ORDER NOT CONFIRMED order=<id>` is logged at error level and the order stays `PENDING` (known gap).
13. `cart-service.EmptyCart` - error ignored.
14. `email-service.SendOrderConfirmation` - error logged, **never fails the order**.

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
| `METRICS_PORT` | `9090` | Prometheus (+pprof) HTTP port |
| `PRODUCT_CATALOG_SERVICE_ADDR` | `product-catalog-service:3550` | `host:port` of product-catalog (gRPC) |
| `CART_SERVICE_ADDR` | `cart-service:7070` | `host:port` of cart |
| `CURRENCY_SERVICE_ADDR` | `currency-service:7000` | `host:port` of currency |
| `SHIPPING_SERVICE_ADDR` | `shipping-service:50051` | `host:port` of shipping |
| `PAYMENT_SERVICE_ADDR` | `payment-service:50051` | `host:port` of payment |
| `EMAIL_SERVICE_ADDR` | `email-service:8080` | `host:port` of email |
| `ORDER_SERVICE_ADDR` | unset (orders are not stored) | `host:port` of order-service. Set: the order is stored before the card is charged and `idempotency_key` works (see Order storage) |
| `INVENTORY_SERVICE_ADDR` | unset (no stock reservation) | `host:port` of inventory-service, e.g. `inventory-service:8087`. Set: stock is reserved before the charge and committed after the shipment, fail closed (see Stock reservation) |
| `OTEL_EXPORTER_OTLP_ENDPOINT` | unset (telemetry off) | OTLP/**gRPC** endpoint for **traces, metrics and logs** (scheme required), e.g. `http://alloy.monitoring.svc.cluster.local:4317`. Legacy fallback: `COLLECTOR_SERVICE_ADDR` (`host:port`). Telemetry is on iff an endpoint is set |
| `ENABLE_TRACING` | — | Legacy switch: `0` forces all telemetry off even if an endpoint is set (it is no longer needed to turn telemetry on) |
| `OTEL_METRIC_EXPORT_INTERVAL` | `15000` | Milliseconds between metric pushes (Go also accepts durations such as `15s`) |
| `OTEL_SERVICE_NAME` | `checkout-service` | `service.name` — becomes the Prometheus `job` and the Loki `service_name`; keep the default, dashboards depend on it |
| `PYROSCOPE_ADDR` | `http://pyroscope:4040` | Profiles use the Pyroscope SDK push protocol (**not** OTLP). Point it at Alloy (`http://alloy.monitoring.svc.cluster.local:4040`), which forwards to Pyroscope, or directly at Pyroscope |
| `ENABLE_PROFILING` | off | `1` starts the Pyroscope SDK **and** exposes `/debug/pprof/*` on the metrics port |

Notes:

- All downstream addresses are **plain `host:port`** values (no scheme). Defaults are short names that only resolve if everything is in the same namespace; across namespaces use `svc.<namespace>.svc.cluster.local:<port>`.
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

The user's cart must already contain items (add some via `cart-service`), otherwise the order has zero lines and a total of 0.

## Project structure

```
├── cmd/server/main.go         # entrypoint: tracing, profiling, gRPC server, downstream dials, metrics server
├── internal/checkout/         # PlaceOrder orchestration (service.go), order storage (orders.go), stock saga (inventory.go), idempotency (idempotency.go)
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
- Health is static; readiness does not reflect downstream availability.
- Image is `amd64`-only and runs as root unless the pod overrides it.
- Retries exist only for `Commit`, `Release` and `ConfirmOrder` (3 attempts each); every other call is a single attempt with no timeout beyond the incoming request context (except the ones named in the docs).
