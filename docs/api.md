# API

Package `hipstershop`, service `CheckoutService` ([`proto/checkout.proto`](https://github.com/platform-boutique/app-checkout-service/blob/main/proto/checkout.proto)). The proto file also contains client-side copies of the downstream service definitions.

| Method | Request | Response |
|---|---|---|
| `PlaceOrder` | `PlaceOrderRequest{ user_id, user_currency, Address address, email, CreditCardInfo credit_card, account_id, idempotency_key }` | `PlaceOrderResponse{ OrderResult order }` (`order_id`, `shipping_tracking_id`, `shipping_cost`, `shipping_address`, `items[]`) |

### What `PlaceOrder` does (in order)

1. Reject requests without `user_id`, `user_currency`, address or credit card, with an `account_id` over 128 characters, or with an `idempotency_key` that is not 0-64 characters of `[A-Za-z0-9._-]` (`INVALID_ARGUMENT`), before any downstream call.
2. Order id: a random v4 UUID, or, with an `idempotency_key` and order storage, `UUIDv5(namespace, account-or-cart-key NUL key)`; **with a key the stored order is looked up first** (`order-service.GetOrder`) and a PLACED order is returned as stored, see [Idempotency](#idempotency). It is the `order_id` of the `OrderResult` (and the id of the stock reservation).
3. `cart-service.GetCart(user_id)`; an empty cart is `FAILED_PRECONDITION`.
4. For every cart line, **in parallel**: `product-catalog-service.GetProduct` then `currency-service.Convert` (USD -> `user_currency`).
5. `shipping-service.GetQuote` (USD) then `currency-service.Convert` to `user_currency`.
6. Sum items x quantity + shipping.
7. **Only if `ORDER_SERVICE_ADDR` is set:** `order-service.CreateOrder` stores the order as `PENDING` (see [Order storage](#order-storage)). If that fails, nothing is charged.
8. **Only if `INVENTORY_SERVICE_ADDR` is set:** `inventory-service.Reserve(order_id, lines)` holds the stock, all or nothing (see [Stock reservation](#stock-reservation)). A shortage is `FAILED_PRECONDITION`, an unreachable inventory is `UNAVAILABLE`; either way the order is marked `FAILED` and nothing is charged.
9. `payment-service.Charge(total, credit_card)` - failure aborts the order: `Release` the stock, mark the order `FAILED` (nothing is shipped, emptied or mailed).
10. `shipping-service.ShipOrder`. If it fails, the charge is **compensated** in this order: `payment-service.Refund(transaction_id, total)` is called once (on a context detached from the caller, 10 s timeout), then `Release` of the stock, then `FailOrder`; the shipping error is returned with the note "your payment has been refunded". If the refund itself fails, the original error is kept, the failure is logged at error level with the transaction id, and the message says the refund is pending (with the transaction id as reference).
11. **Only with inventory:** `inventory-service.Commit(order_id)`, 3 attempts (3 s each, pauses of 1 s then 3 s), on a detached context. If it still fails the order goes on and `STOCK NOT COMMITTED order=<id>` is logged at error level (known gap).
12. **Only if `KAFKA_BROKERS` is set:** publish the `orders.placed` event (key = order id, `acks=all`, idempotent producer, 5 s bound, see [Order events](#order-events-kafka)). It runs after Ship and Commit and before `ConfirmOrder`. A failure or timeout is logged (`EVENT NOT PUBLISHED order=<id>`, error level) and **never fails or delays the order** beyond the bound (known gap: no outbox).
13. **Only with order storage:** `order-service.ConfirmOrder(order_id, tracking_id, transaction_id)`; the order becomes `PLACED`. 3 attempts (3 s each, pauses of 0.5 s then 2 s) on a detached context; a final answer (`FAILED_PRECONDITION`, `NOT_FOUND`, `INVALID_ARGUMENT`) is not retried. If every attempt fails the order goes on, `ORDER NOT CONFIRMED order=<id>` is logged at error level and the order stays `PENDING` (known gap).
14. `cart-service.EmptyCart` - error ignored.
15. `email-service.SendOrderConfirmation` - error logged, **never fails the order**. **Skipped when `KAFKA_BROKERS` is set**: the email consumer sends the confirmation from the event.

Everything that can fail without side effects (steps 1-6) runs before the card is charged. Release, Refund and FailOrder are best effort: their own failure is logged and never replaces the error `PlaceOrder` returns.

### Error codes returned by `PlaceOrder`

Downstream gRPC codes are passed through; the message is `<prefix>: <downstream message>`; payment's machine-readable `reason:` prefix is stripped. Raw details of other failures stay in the server log.

| Downstream code (cart, product, currency, shipping, payment) | `PlaceOrder` code | Message |
|---|---|---|
| `INVALID_ARGUMENT` | `INVALID_ARGUMENT` | `<service>: <message>`; payment: `card rejected: <message>`; e.g. `shipping: invalid address: country "X" is not ...` |
| `FAILED_PRECONDITION` | `FAILED_PRECONDITION` | `<service>: <message>`; payment: `card declined: <message>` |
| `NOT_FOUND` | `NOT_FOUND` | `<service>: <message>`, e.g. `product: ...` |
| `UNAVAILABLE`, `DEADLINE_EXCEEDED`, `RESOURCE_EXHAUSTED` | `UNAVAILABLE` | `<service> service is temporarily unavailable, please retry` |
| `CANCELED` | `CANCELED` | `request canceled` |
| anything else (`UNKNOWN`, `INTERNAL`, `UNIMPLEMENTED`, ...) | `INTERNAL` | `internal error while placing the order` |

`<service>` is `cart`, `product`, `currency` or `shipping`. Own validation: missing user, currency, address or card -> `INVALID_ARGUMENT` (`order: ...`); empty cart -> `FAILED_PRECONDITION` (`order: the cart is empty`).

Errors of the saga and idempotency (own messages, not passed through from a downstream):

| Situation | `PlaceOrder` code | Message |
|---|---|---|
| `Reserve` answered `reserved = false` | `FAILED_PRECONDITION` | `out of stock: "Wool Scarf" (only 2 left, you asked for 3), "Tote Bag" (sold out)` (every short product by name, in cart order, at most 10 listed then `and N more`) |
| `Reserve` RPC fails (any error, inventory down) | `UNAVAILABLE` | `stock check unavailable, try again` |
| invalid `idempotency_key` | `INVALID_ARGUMENT` | `order: idempotency_key must be at most 64 characters of A-Z a-z 0-9 . _ -` |
| same key, stored order still `PENDING`, or the same key in flight in this process | `ABORTED` | `this order is already being processed` |
| same key, stored order `FAILED` | `FAILED_PRECONDITION` | `this attempt failed, start the checkout again` |
| same key, order-service lookup fails | `UNAVAILABLE` / `INTERNAL` | as for `CreateOrder` |

### Order storage

Optional, switched on by `ORDER_SERVICE_ADDR` (for example `order-service:8085`). Contract: `order-service` `proto/orders.proto`, copied to [`proto/orders/orders.proto`](proto/orders/orders.proto) in its own Go package `checkout-service/proto/orders` (only `go_package` differs; `Money`, `Address` and `OrderItem` would clash with `hipstershop` otherwise).

- **Unset:** `PlaceOrder` behaves as before, no order is stored, order-service is never contacted (the old stack keeps working). The server logs that order storage is disabled.
- **Set:** storing the order is mandatory. The connection is lazy, so checkout starts even if order-service is down, but every `PlaceOrder` then fails with `UNAVAILABLE` **before** the card is charged.

| Situation (with `ORDER_SERVICE_ADDR`) | Calls | `PlaceOrder` result |
|---|---|---|
| Everything works | `CreateOrder`, `Charge`, `ShipOrder`, `ConfirmOrder`, empty cart, email (or the event, see Order events) | success, `order.order_id` is the order-service id |
| Validation / cart / product / price / quote fails | none to order-service | as without order storage |
| Email, address or cart outside the order-service limits (see below) | none to order-service | `INVALID_ARGUMENT` (`order: ...`) |
| `CreateOrder` `UNAVAILABLE` / `DEADLINE_EXCEEDED` / `RESOURCE_EXHAUSTED` / `CANCELED` | `CreateOrder` only | `UNAVAILABLE` (`order service is temporarily unavailable, please retry`), nothing charged |
| `CreateOrder` any other code (`INVALID_ARGUMENT`, `ALREADY_EXISTS`, ...) | `CreateOrder` only | `INTERNAL`; logged at error level with the detail (a checkout bug), nothing charged |
| Card declined or any `Charge` error | `CreateOrder`, `Charge`, `FailOrder` | the mapped payment error; `FailOrder` reason = that message |
| `ShipOrder` fails | `CreateOrder`, `Charge`, `ShipOrder`, `Refund`, `FailOrder` | the mapped shipping error + refund note; reason = that message |
| `ConfirmOrder` fails after charge and shipment | `CreateOrder`, `Charge`, `ShipOrder`, `ConfirmOrder` x3 | **success**; `ConfirmOrder` is tried 3 times, then `ORDER NOT CONFIRMED order=<id>` is logged at error level (with transaction id and tracking id); the order stays `PENDING` for manual follow-up |
| `FailOrder` fails | as above | the original error is returned unchanged; the `FailOrder` error is logged (the order stays `PENDING`) |

Details:

- `CreateOrder` carries `session_id` = the request `user_id` (the cart key) and `user_id` = the request `account_id` (empty for an anonymous order), the email, the address field for field, one item per cart line with `product_id`, `name`, `picture` and the unit price in the **user's currency** (the converted price), `shipping_cost` and `total`. The total is the amount that is charged: the same `money` package arithmetic (integer units and nanos, no floats) over the same numbers, so it equals `sum(quantity x unit_price) + shipping_cost` exactly, in one currency. `CreateOrder` has a 5 s timeout and runs on the request context.
- `FailOrder` and `ConfirmOrder` run on a context detached from the caller (10 s timeout), so a caller that gave up does not leave an order `PENDING` after money moved. The `FailOrder` reason is the message of the status returned to the caller (it can contain the last four card digits that payment-service puts in its decline message, never the card number or CVV).
- A `ConfirmOrder` failure is not retried and not turned into an error: the money is taken and the parcel is ordered, so the customer gets the success.
- Limits checked up front (they are order-service's): email 3-254 characters and a plain address; `user_id` at most 128; street 1-200, city 1-100, state at most 100, country 1-100, zip not negative; 1-100 cart lines, quantity 1-1000. A product name over 200 characters is cut, a picture over 500 characters is left out of the snapshot.
- `account_id` (field 7, optional, max 128 characters) is the id of a signed-in buyer (user-service). It is trimmed and passed as `CreateOrder.user_id` so the order belongs to the account from the start; empty means an anonymous order exactly as before. `user_id` keeps its meaning (cart key: the cart is read and emptied under it, and it is the order's `session_id`). Checkout does not verify the account id (it trusts its caller, the frontend, like it trusts `user_id`) and with `ORDER_SERVICE_ADDR` unset the field is validated but has no effect. Orders placed anonymously are handed to an account later by order-service's `ClaimOrders`, which the frontend calls after login (checkout does not need that RPC, so its copy of `orders.proto` is unchanged).
- `PlaceOrder` is idempotent only with an `idempotency_key`, see [Idempotency](#idempotency); without one every call creates a new order id.

### Stock reservation

Optional, switched on by `INVENTORY_SERVICE_ADDR` (for example `inventory-service:8087`). Contract: `inventory-service` `proto/inventory.proto`, copied to [`proto/inventory/inventory.proto`](proto/inventory/inventory.proto) in its own Go package `checkout-service/proto/inventory` (only `go_package` differs).

- **Unset:** no reserve, commit or release, inventory-service is never contacted: today's behaviour. The server logs that stock reservation is disabled.
- **Set:** a reservation is mandatory (fail closed, never sell without one). The connection is lazy: with inventory-service down every `PlaceOrder` fails with `UNAVAILABLE` before the card is charged. It works with or without `ORDER_SERVICE_ADDR`.
- The reservation lines are the cart lines, one per product (quantities of repeated lines are added up); the reservation id is the order id.

| Situation (with `INVENTORY_SERVICE_ADDR` and `ORDER_SERVICE_ADDR`) | Calls | `PlaceOrder` result |
|---|---|---|
| Everything works | `CreateOrder`, `Reserve`, `Charge`, `ShipOrder`, `Commit`, `ConfirmOrder`, empty cart, email | success |
| `Reserve` short | `CreateOrder`, `Reserve`, `FailOrder` | `FAILED_PRECONDITION` `out of stock: ...`; nothing charged, nothing held so no `Release` |
| `Reserve` RPC error | `CreateOrder`, `Reserve`, `Release`, `FailOrder` | `UNAVAILABLE`; nothing charged; `Release` because the request may have arrived although the answer did not |
| `Charge` fails | `CreateOrder`, `Reserve`, `Charge`, `Release`, `FailOrder` | the mapped payment error, no refund |
| `ShipOrder` fails | `CreateOrder`, `Reserve`, `Charge`, `ShipOrder`, `Refund`, `Release`, `FailOrder` | the mapped shipping error + refund note |
| `Commit` fails 3 times | `..., ShipOrder`, `Commit` x3, `ConfirmOrder` | **success**; `STOCK NOT COMMITTED order=<id>` logged at error level |
| `Release` fails | as above | the original error, unchanged; `STOCK NOT RELEASED` logged (3 attempts; the hold then expires by the TTL of inventory-service) |

`Release` (5 s per attempt) and `Commit` (3 s per attempt) run on a context detached from the caller, so a customer who closed the tab while the card was being charged does not leave stock held (or sold but uncommitted). `Reserve` runs on the request context with a 5 s timeout.

### Idempotency

`PlaceOrderRequest.idempotency_key` (field 8, optional) makes a retried or double-submitted `PlaceOrder` safe. It needs order storage (the stored order is the record of what happened); without `ORDER_SERVICE_ADDR` the key is validated and otherwise ignored (random order id, as before).

- The order id is `UUIDv5(fixed namespace, owner + "\x00" + key)`, owner = `account_id` when set, else `user_id` (the cart key), so two visitors using the same key never collide.
- Before anything else `GetOrder(order_id)` is called. Not found: a new attempt, the normal flow (`CreateOrder` is idempotent by order id). `PLACED` or later (`PROCESSING`, `SHIPPED`, `DELIVERED`): the stored result is returned (`order_id`, tracking id, shipping cost and address, lines with quantity and unit price) **without** charging, reserving, shipping, emptying the cart or sending the email again; it does not depend on the cart (which was emptied). `PENDING`: `ABORTED` `this order is already being processed`. `FAILED`: `FAILED_PRECONDITION` `this attempt failed, start the checkout again`; a new attempt needs a **new key**.
- Two replicas receiving the same key at the same moment: both lookups can miss, but `CreateOrder` (order-service with `Order.created`) answers `created = false` to all but the one that inserted the row; that call takes the same branches as the lookup above (`PENDING` -> `ABORTED`, `PLACED` or later -> stored result, `FAILED` -> `FAILED_PRECONDITION`) and reserves, charges and ships nothing. An order-service that does not send the field (unset = unknown) leaves this race open, see [Known limitations](#known-limitations).
- The same key arriving twice at once in one checkout process: the second call gets `ABORTED` immediately (in-memory guard), exactly one charge. After the first finished, the same key replays the stored order.
- A lookup failure (order-service down) is `UNAVAILABLE`: checkout does not guess.
- An order stuck `PENDING` (a crash, `FailOrder` or `ConfirmOrder` failing) answers `ABORTED` for its key forever; the customer needs a new key and the order needs manual follow-up.

### Order events (Kafka)

Optional, switched on by `KAFKA_BROKERS` (comma separated seed brokers, for example `kafka:9092`). The topic is `orders.placed` (3 partitions, replication 1, auto-created on first use by the broker config of the compose stack). Client: `github.com/twmb/franz-go` (`pkg/kgo`), pure Go.

- **Unset or empty:** no event, Kafka is never contacted, and `PlaceOrder` behaves as before **including the direct `email-service.SendOrderConfirmation` call**.
- **Set:** after `ShipOrder` and `Commit` succeeded and before checkout's own `ConfirmOrder`, one record is produced: key = the order id, headers `event-type=order.placed`, `content-type=application/json`, `schema-version=1`, `acks=all`, idempotent producer, a 5 s delivery timeout (the whole step is bounded by 5 s, also for a client that hangs). Checkout then **no longer calls email-service**: the email consumer sends the confirmation from the event. Order-service's consumer uses the same event to confirm an order whose `ConfirmOrder` was missed.
- The producer is created at startup; franz-go connects lazily, so a broker that is down at startup does not stop checkout and produce errors are per call. On SIGTERM/SIGINT the gRPC server drains (at most 15 s), then the producer is flushed (at most 5 s) and closed.
- A publish failure (broker down, timeout, ...) is logged at error level (`EVENT NOT PUBLISHED order=<id>` with transaction id and tracking id) and never fails the order: the customer is charged and the parcel is ordered. Metrics: `checkout_order_events_total{result="published|failed"}`, `checkout_order_event_publish_seconds`. The payload (email, address) is never logged.
- Failed orders (declined, out of stock, shipping failed) publish nothing. An idempotent replay of a stored `PLACED` order (same `idempotency_key`) **never republishes**.
- Delivery is at-least-once; consumers must be idempotent by `event_id` (`UUIDv5(UUIDv5(URL namespace, "everly/orders.placed"), order_id)`, so a republish of the same order would carry the same id) or `order_id`.

Value (JSON, UTF-8, schema version 1; every field is always present, an unknown one is `""` or `0`):

```json
{
  "event_id": "f9ebed3d-a36c-57ea-87c0-f0572344656e",
  "event_type": "order.placed",
  "schema_version": 1,
  "occurred_at": "2026-10-31T18:00:00.123Z",
  "order_id": "0d4c1a54-7a52-4a58-9a5e-0e5b5b1f3a11",
  "user_id": "account id or empty string",
  "email": "customer@example.com",
  "shipping_address": {"street_address": "1 Main St", "city": "Testville", "state": "NY", "country": "US", "zip_code": 10001},
  "items": [{"product_id": "prod-1", "name": "Test Widget", "quantity": 2, "unit_price": {"currency_code": "USD", "units": 12, "nanos": 990000000}, "picture": "/static/x.jpg"}],
  "shipping_cost": {"currency_code": "USD", "units": 5, "nanos": 0},
  "total": {"currency_code": "USD", "units": 30, "nanos": 980000000},
  "shipping_tracking_id": "TRK-1",
  "payment_transaction_id": "TX-1"
}
```

Money is the same integer units + nanos (in the user's currency) that were charged and stored in order-service. `user_id` is the account id (empty for an anonymous order), **never** the cart/session key; card data and CVV are never in the event.

Known gaps: there is **no outbox**. Publishing is best effort and happens in the request: if the broker is down or slow the event is lost (only the error log remembers it), the order-service consumer does not learn of the order (checkout's own `ConfirmOrder` still runs) and, because checkout no longer calls email-service when Kafka is on, **that customer gets no confirmation email**. Nothing re-publishes later.
