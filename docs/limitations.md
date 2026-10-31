# Known limitations

- **Compensation is best effort:** if shipping fails after the charge, one `Refund` is attempted (no retry). If it fails, the customer stays charged until someone refunds the transaction id found in the error log (the message says the refund is pending). A `Charge` that times out without a response cannot be refunded (no transaction id). Without an `idempotency_key` (and order storage) `PlaceOrder` is not idempotent: a client retry charges again.
- `Refund` needs a payment-service that implements it (branch `feat/decline-and-refund`); an older payment answers `UNIMPLEMENTED` and the order ends with "refund is pending".
- The order-service integration was tested only against an in-process fake order-service (it re-implements the total rule with `big.Int`); it was **not run** against the real order-service or in the compose stack.
- **STOCK NOT COMMITTED:** if `Commit` fails 3 times after charge and shipment, the order still succeeds and `STOCK NOT COMMITTED order=<id>` is logged at error level. The reservation then expires by the TTL of inventory-service (default 15 m) and that stock can be sold a second time. Nothing re-tries the commit later.
- **No reconciliation:** nothing compares orders, payments and reservations afterwards. Orders stuck `PENDING`, charges whose refund is pending, reservations that were never released or committed (`STOCK NOT RELEASED`) need manual follow-up from the logs. A reservation can also expire (TTL) while the card is being charged; `Commit` then fails and the stock may be oversold.
- **Idempotency is per process:** the in-memory guard against a concurrent double submit does not span checkout replicas. `CreateOrder` does not say whether it created the order, so two replicas receiving the same key within milliseconds could both charge. A sequential retry is always safe (it goes through the stored order). Without `ORDER_SERVICE_ADDR` there is no idempotency.
- The inventory integration was tested only against in-process fake order-, payment-, shipping- and inventory-services; it was **not run** against the real inventory-service or in the compose stack.
- Health is static; readiness does not reflect downstream availability.
- Image is `amd64`-only and runs as root unless the pod overrides it.
- Single-attempt calls: no retries or timeouts beyond the incoming request context.
