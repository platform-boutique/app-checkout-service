# 1. The order is a saga with best-effort compensation

Status: accepted (reconstructed from the code and the earlier README).

## Context

`PlaceOrder` touches services that each own their data: cart, catalog, currency, shipping, payment and, optionally, order-service and inventory-service. There is no shared transaction. The card is charged once and cannot be taken back by a database rollback.

## Decision

Everything that can fail without a side effect (validation, cart, prices, quote) runs first. After that the steps run in a fixed order: create order, reserve stock, charge, ship, commit stock, publish the event, confirm the order. When a step fails, the earlier ones are undone by compensating calls (`Release`, `Refund`, `FailOrder`) on a context detached from the caller. Compensation is tried once (`Refund`) or a few times (`Commit`, `Release`, `ConfirmOrder` retry 3 times); a failure is logged at error level with a fixed marker and never replaces the error returned to the customer.

## Consequences

- A refund, a stock release or an order confirmation can be lost; nothing reconciles afterwards. The log markers `STOCK NOT COMMITTED`, `STOCK NOT RELEASED`, `ORDER NOT CONFIRMED` and the pending-refund message are the only record (see [Known limitations](../limitations.md)).
- The optional steps keep the service usable with fewer collaborators (unset `ORDER_SERVICE_ADDR`, `INVENTORY_SERVICE_ADDR` or `KAFKA_BROKERS` skips the step).
- A durable outbox or a reconciliation job would remove the lost-event cases; neither exists.
