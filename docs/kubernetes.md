# Kubernetes

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
