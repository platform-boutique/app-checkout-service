# Operations

## Health

- `grpc.health.v1.Health/Check` on the gRPC port (`5050`), empty service name. The status is set to `SERVING` at start and never changed: it says the process is up, not that the downstream services are reachable (the connections are lazy).
- Container probe: `/app/checkout-service healthcheck` calls the local health service (`127.0.0.1:$PORT`, 3 s deadline) and exits 0 only on `SERVING`.

```bash
grpcurl -plaintext localhost:5050 grpc.health.v1.Health/Check
curl -s localhost:9090/metrics | grep grpc_server
```

## Shutdown

On SIGTERM or SIGINT (`cmd/server/main.go`):

1. the server logs `shutdown signal received, draining` and calls `GracefulStop`, so in-flight `PlaceOrder` calls can finish;
2. after 15 s (`drainTimeout`) the server is cut off with `Stop`;
3. if Kafka is on, the producer is flushed and closed, bounded by 4 s (`obs.DefaultShutdownTimeout`);
4. telemetry and profiles are flushed, bounded by the same 4 s.

Because of the 15 s drain, give the pod a termination grace period of at least 25 s (worst case 15 + 4 + 4 s). A measured `docker stop` was not run for this page. The Helm chart (platform-demo-helm `checkout-service/values.yaml`, rendered by `platform-common`) sets `terminationGracePeriodSeconds: 30`, which is at least the 25 s needed (read 2026-11-22; no per-environment override found under `envs/`).

## Scaling and replicas

- Stateless. Several replicas are safe in principle; each outbound gRPC connection is pinned to one backend pod (`pick_first`), and so is each inbound connection from the frontend.
- Idempotency (`idempotency_key`) works across replicas only against an order-service that sends `Order.created` in the `CreateOrder` answer; see [Known limitations](limitations.md).
- Resources used in the local stack are in [Kubernetes](kubernetes.md). More than one replica was not run.

## Failure behaviour

| Failure | Behaviour |
|---|---|
| A downstream service is down at start | the service starts and reports `SERVING`; calls fail per request |
| Cart, catalog, currency or shipping quote fails or times out (3 s) | `PlaceOrder` fails before the charge, nothing to undo |
| `ORDER_SERVICE_ADDR` set and `CreateOrder` fails | `UNAVAILABLE` (or `INTERNAL` for a contract error), nothing charged |
| `INVENTORY_SERVICE_ADDR` set and `Reserve` fails | `UNAVAILABLE` or `FAILED_PRECONDITION` (out of stock), order marked `FAILED`, nothing charged |
| `Charge` fails | stock released, order marked `FAILED`, nothing shipped |
| `ShipOrder` fails | one `Refund`, then `Release`, then `FailOrder`; the message says whether the refund went through |
| `Commit` fails 3 times | order goes on; error log `STOCK NOT COMMITTED order=<id>` |
| `ConfirmOrder` fails 3 times | order goes on and stays `PENDING`; error log `ORDER NOT CONFIRMED order=<id>` |
| Kafka down or slow | order goes on; error log `EVENT NOT PUBLISHED order=<id>`; no confirmation mail (email-service is not called when Kafka is on) |
| Handler panic | turned into `INTERNAL` by a recovery interceptor, the process keeps running |
| Collector unreachable | telemetry export fails in the background; start-up does not wait for it |

## Runbook

| Symptom | Check |
|---|---|
| `UNAVAILABLE ... temporarily unavailable` | the named service (`cart`, `product`, `currency`, `shipping`, `payment`, `order`, `stock check`); its health and the `*_ADDR` variable |
| Customer charged, shipping failed, "refund is pending" | the error log line with the transaction id; refund it in payment-service by hand |
| Order stuck `PENDING` in order-service | search the log for `ORDER NOT CONFIRMED order=<id>`; its idempotency key answers `ABORTED` until fixed by hand |
| Stock not released or committed | search for `STOCK NOT RELEASED` / `STOCK NOT COMMITTED`; the reservation expires with inventory-service's TTL |
| No confirmation mail with Kafka on | search for `EVENT NOT PUBLISHED`; nothing re-publishes the event |
| Process exits at start | one JSON error line, for example `failed to listen on port ...` |

There is no dashboard or alert rule for this service in the repository yet (the dashboards are an open task).
