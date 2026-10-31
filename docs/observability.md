# Observability

| Signal | Implementation | Destination |
|---|---|---|
| **Traces** | OpenTelemetry (`otelgrpc`/`otelhttp` handlers), OTLP/gRPC exporter, `AlwaysSample`, W3C TraceContext + Baggage; the trace context is propagated to every downstream call | Alloy `:4317` → Tempo |
| **Metrics** | The Prometheus default registry (`grpc_server_handled_total`, `grpc_server_handling_seconds_*`, `grpc_server_started_total`, `grpc_server_msg_*`, `checkout_order_events_total{result}`, `checkout_order_event_publish_seconds`, Go runtime/process) is bridged into OpenTelemetry and pushed over OTLP/gRPC every 15 s — metric names are unchanged. `/metrics` is still served for debugging | Alloy → Mimir |
| **Logs** | `logrus` JSON to stdout **and** an OTLP/gRPC hook (`otellogrus`) (the `internal/checkout` logger is hooked too) | Alloy → Loki |
| **Profiles** | Pyroscope Go SDK (CPU, alloc/inuse objects & space) plus pprof HTTP when profiling is on | Alloy `:4040` → Pyroscope |

Every series carries `job="checkout-service"` (from `service.name`) and `instance=<pod name>` (`service.instance.id`), which is what the Grafana dashboards select on. Logs land in Loki with `service_name="checkout-service"` and trace/span ids, so a log line links to its trace.
