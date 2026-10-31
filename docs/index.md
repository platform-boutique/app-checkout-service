# checkout-service

<div class="pd-hero" markdown>
<span class="pd-kicker">service · platform-demo</span>

A gRPC service that orchestrates order placement for the platform-demo e-commerce platform. A single `PlaceOrder` call fans out to six other services — cart, product catalog, currency, shipping, payment and email — to price the order, charge the card, ship it and confirm it. It is the most connected service in the system and holds no state of its own.
</div>

<div class="pd-facts" markdown>
<div><span>Runtime</span><strong>Go 1.25</strong><small>Go</small></div>
<div><span>Listens on</span><strong>5050</strong><small>gRPC</small></div>
<div><span>State</span><strong>Stateless</strong></div>
<div><span>Image</span><strong>checkout-service</strong><small>mladenovskistefan/</small></div>
</div>

## Where it sits

Called by `frontend`. It calls `cart-service`, `currency-service`, `email-service`, `payment-service`, `product-catalog-service`, `shipping-service`.

```mermaid
flowchart LR
  checkout_service["<b>checkout-service</b><br/>5050 (gRPC)"]:::me
  frontend["frontend"] --> checkout_service
  checkout_service --> cart_service["cart-service"]
  checkout_service --> currency_service["currency-service"]
  checkout_service --> email_service["email-service"]
  checkout_service --> payment_service["payment-service"]
  checkout_service --> product_catalog_service["product-catalog-service"]
  checkout_service --> shipping_service["shipping-service"]
  classDef me stroke-width:3px;
```

## At a glance

| | |
|---|---|
| **Language / runtime** | Go 1.25 (static binary, `CGO_ENABLED=0`) |
| **Image** | `mladenovskistefan/checkout-service` |
| **Protocol / port** | gRPC (HTTP/2, plaintext) on `5050` |
| **Metrics** | Pushed over OTLP/gRPC (Prometheus registry bridged into OpenTelemetry); `/metrics` on `9090` is still served for debugging |
| **Health** | Standard `grpc.health.v1.Health/Check` on `5050` — static `SERVING`, does **not** reflect downstream health |
| **State** | None |
| **Depends on** | `product-catalog-service`, `cart-service`, `currency-service`, `shipping-service`, `payment-service`, `email-service`; optional: `order-service`, `inventory-service`, Kafka (`KAFKA_BROKERS`, produces `orders.placed`) |
| **Called by** | `frontend` |
| **gRPC reflection** | Enabled — `grpcurl` works without `-proto` |

## Find it elsewhere

<div class="pd-cards" markdown>
<div><h4><a href="https://argocd.platform-demo.click/applications/argocd/dev-checkout-service">Argo CD</a></h4><p>Sync and health of <code>dev-checkout-service</code>.</p></div>
<div><h4><a href="https://grafana.platform-demo.click/d/checkout-service-overview">Grafana</a></h4><p>Latency, traffic and errors (<code>checkout-service-overview</code>).</p></div>
<div><h4><a href="https://github.com/platform-boutique/app-checkout-service">Repository</a></h4><p>Source, pull requests and releases of <code>app-checkout-service</code>.</p></div>
</div>

!!! info "Part of the platform-demo platform"
    Cross-cutting facts (service wiring, observability contract, Kubernetes translation, CI/CD) are in the
    [master reference](https://github.com/platform-boutique/platform-demo-docs). This site only describes checkout-service.
