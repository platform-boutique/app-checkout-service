# 2. The proto package stays `hipstershop`

Status: accepted (owner decision 2026-11-14, application standards section 14).

## Context

The proto package of checkout-service, product-catalog-service and shipping-service is the legacy `hipstershop`. The standard for every other service is `hipstershop.<name>`. The package name is part of the gRPC service name on the wire (`/hipstershop.CheckoutService/PlaceOrder`), so renaming it is a breaking change for every client (the frontend and the load tooling).

## Decision

Keep the package `hipstershop` for `checkout.proto`. Changes stay additive (new fields or RPCs, no renumbering, no removals), server first. The vendored contracts of order-service and inventory-service live in their own Go packages (`proto/orders`, `proto/inventory`), because `Money`, `Address` and `OrderItem` would clash with `hipstershop`.

## Consequences

- The package name does not follow the pattern of the newer services. This is a recorded exception, not an oversight.
- A rename would need a new package version and a deprecation window (the standards' breaking-change rule).
