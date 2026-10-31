# Configuration

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
| `KAFKA_BROKERS` | unset (no event) | Comma separated seed brokers of Kafka, e.g. `kafka:9092`. Set: `orders.placed` is published after Ship and Commit (best effort, 5 s bound) and checkout no longer calls email-service; empty: today's behaviour with the direct email call. The broker may be down at startup (lazy connect). See Order events |
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
