# Configuration

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
