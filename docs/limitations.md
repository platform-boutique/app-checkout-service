# Known limitations

- **Compensation is best effort:** if shipping fails after the charge, one `Refund` is attempted (no retry). If it fails, the customer stays charged until someone refunds the transaction id found in the error log (the message says the refund is pending). A `Charge` that times out without a response cannot be refunded (no transaction id). `PlaceOrder` is not idempotent: a client retry charges again.
- `Refund` needs a payment-service that implements it (branch `feat/decline-and-refund`); an older payment answers `UNIMPLEMENTED` and the order ends with "refund is pending".
- The order-service integration was tested only against an in-process fake order-service (it re-implements the total rule with `big.Int`); it was **not run** against the real order-service or in the compose stack.
- Health is static; readiness does not reflect downstream availability.
- Image is `amd64`-only and runs as root unless the pod overrides it.
- Single-attempt calls: no retries or timeouts beyond the incoming request context.
