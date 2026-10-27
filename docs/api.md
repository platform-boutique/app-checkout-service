# API

Package `hipstershop`, service `CheckoutService` ([`proto/checkout.proto`](https://github.com/platform-boutique/app-checkout-service/blob/main/proto/checkout.proto)). The proto file also contains client-side copies of the downstream service definitions.

| Method | Request | Response |
|---|---|---|
| `PlaceOrder` | `PlaceOrderRequest{ user_id, user_currency, Address address, email, CreditCardInfo credit_card }` | `PlaceOrderResponse{ OrderResult order }` (`order_id`, `shipping_tracking_id`, `shipping_cost`, `shipping_address`, `items[]`) |

### What `PlaceOrder` does (in order)

1. Reject requests without `user_id`, `user_currency`, address or credit card (`INVALID_ARGUMENT`), before any downstream call.
2. Generate the order UUID (random v4). It is the `order_id` of the `OrderResult`.
3. `cart-service.GetCart(user_id)`; an empty cart is `FAILED_PRECONDITION`.
4. For every cart line, **in parallel**: `product-catalog-service.GetProduct` then `currency-service.Convert` (USD → `user_currency`).
5. `shipping-service.GetQuote` (USD) then `currency-service.Convert` to `user_currency`.
6. Sum items × quantity + shipping.
7. **Only if `ORDER_SERVICE_ADDR` is set:** `order-service.CreateOrder` stores the order as `PENDING` (see [Order storage](#order-storage)). If that fails, nothing is charged.
8. `payment-service.Charge(total, credit_card)` — failure aborts the order (nothing is shipped, emptied or mailed; with order storage the order is marked `FAILED`).
9. `shipping-service.ShipOrder`. If it fails, the charge is **compensated**: `payment-service.Refund(transaction_id, total)` is called once (on a context detached from the caller, 10 s timeout), then the shipping error is returned with the note "your payment has been refunded". If the refund itself fails, the original error is kept, the failure is logged at error level with the transaction id, and the message says the refund is pending (with the transaction id as reference).
   With order storage the order is then marked `FAILED` too (after the refund).
10. **Only with order storage:** `order-service.ConfirmOrder(order_id, tracking_id, transaction_id)`; the order becomes `PLACED`.
11. `cart-service.EmptyCart` — error ignored.
12. `email-service.SendOrderConfirmation` — error logged, **never fails the order**.

Everything that can fail without side effects (steps 1-6) runs before the card is charged.

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

### Order storage

Optional, switched on by `ORDER_SERVICE_ADDR` (for example `order-service:8085`). Contract: `order-service` `proto/orders.proto`, copied to [`proto/orders/orders.proto`](proto/orders/orders.proto) in its own Go package `checkout-service/proto/orders` (only `go_package` differs; `Money`, `Address` and `OrderItem` would clash with `hipstershop` otherwise).

- **Unset:** `PlaceOrder` behaves as before, no order is stored, order-service is never contacted (the old stack keeps working). The server logs that order storage is disabled.
- **Set:** storing the order is mandatory. The connection is lazy, so checkout starts even if order-service is down, but every `PlaceOrder` then fails with `UNAVAILABLE` **before** the card is charged.

| Situation (with `ORDER_SERVICE_ADDR`) | Calls | `PlaceOrder` result |
|---|---|---|
| Everything works | `CreateOrder`, `Charge`, `ShipOrder`, `ConfirmOrder`, empty cart, email | success, `order.order_id` is the order-service id |
| Validation / cart / product / price / quote fails | none to order-service | as without order storage |
| Email, address or cart outside the order-service limits (see below) | none to order-service | `INVALID_ARGUMENT` (`order: ...`) |
| `CreateOrder` `UNAVAILABLE` / `DEADLINE_EXCEEDED` / `RESOURCE_EXHAUSTED` / `CANCELED` | `CreateOrder` only | `UNAVAILABLE` (`order service is temporarily unavailable, please retry`), nothing charged |
| `CreateOrder` any other code (`INVALID_ARGUMENT`, `ALREADY_EXISTS`, ...) | `CreateOrder` only | `INTERNAL`; logged at error level with the detail (a checkout bug), nothing charged |
| Card declined or any `Charge` error | `CreateOrder`, `Charge`, `FailOrder` | the mapped payment error; `FailOrder` reason = that message |
| `ShipOrder` fails | `CreateOrder`, `Charge`, `ShipOrder`, `Refund`, `FailOrder` | the mapped shipping error + refund note; reason = that message |
| `ConfirmOrder` fails after charge and shipment | `CreateOrder`, `Charge`, `ShipOrder`, `ConfirmOrder` | **success**; logged at error level (`ORDER NOT CONFIRMED`) with order id, transaction id and tracking id; the order stays `PENDING` for manual follow-up |
| `FailOrder` fails | as above | the original error is returned unchanged; the `FailOrder` error is logged (the order stays `PENDING`) |

Details:

- `CreateOrder` carries `session_id` = the request `user_id`, the email, the address field for field, one item per cart line with `product_id`, `name`, `picture` and the unit price in the **user's currency** (the converted price), `shipping_cost` and `total`. The total is the amount that is charged: the same `money` package arithmetic (integer units and nanos, no floats) over the same numbers, so it equals `sum(quantity x unit_price) + shipping_cost` exactly, in one currency. `CreateOrder` has a 5 s timeout and runs on the request context.
- `FailOrder` and `ConfirmOrder` run on a context detached from the caller (10 s timeout), so a caller that gave up does not leave an order `PENDING` after money moved. The `FailOrder` reason is the message of the status returned to the caller (it can contain the last four card digits that payment-service puts in its decline message, never the card number or CVV).
- A `ConfirmOrder` failure is not retried and not turned into an error: the money is taken and the parcel is ordered, so the customer gets the success.
- Limits checked up front (they are order-service's): email 3-254 characters and a plain address; `user_id` at most 128; street 1-200, city 1-100, state at most 100, country 1-100, zip not negative; 1-100 cart lines, quantity 1-1000. A product name over 200 characters is cut, a picture over 500 characters is left out of the snapshot.
- `PlaceOrder` is still not idempotent: each call creates a new order id.
