# Develop & test

## Running locally

Requirements: Go 1.25+, Docker, [`grpcurl`](https://github.com/fullstorydev/grpcurl), and the six downstream services (or stand-ins) for a *successful* order.

```bash
go build ./... && go run ./cmd/server

docker build -t checkout-service:local .
docker run --rm -p 5050:5050 -p 9090:9090 \
  -e CART_SERVICE_ADDR=host.docker.internal:7070 \
  -e PRODUCT_CATALOG_SERVICE_ADDR=host.docker.internal:3550 \
  checkout-service:local

go test ./...
```

The simplest full setup is the compose file in `platform-demo-observability/`.

### Manual gRPC testing (reflection is on)

```bash
grpcurl -plaintext -d '{
  "user_id": "test-user",
  "user_currency": "USD",
  "address": {"street_address": "123 Main St", "city": "New York", "state": "NY", "country": "US", "zip_code": 10001},
  "email": "test@example.com",
  "credit_card": {"credit_card_number": "4111111111111111", "credit_card_cvv": 123,
                  "credit_card_expiration_year": 2030, "credit_card_expiration_month": 12}
}' localhost:5050 hipstershop.CheckoutService/PlaceOrder

grpcurl -plaintext localhost:5050 grpc.health.v1.Health/Check
curl -s localhost:9090/metrics | grep grpc_server
```

The user's cart must already contain items (add some via `cart-service`), otherwise the order has zero lines and a total of 0.

### Kafka integration tests

`internal/checkout/kafka_integration_test.go` (build tag `integration`) publishes a real event and reads it back, and checks that `PlaceOrder` still succeeds within the bound with the broker down. It needs a throwaway single-node Kafka reachable at `KAFKA_TEST_BROKERS` (default `127.0.0.1:19094`):

```bash
docker run -d --rm --name d1-kafka --hostname d1-kafka --memory 768m \
  -e KAFKA_NODE_ID=1 -e KAFKA_PROCESS_ROLES=broker,controller \
  -e KAFKA_LISTENERS=PLAINTEXT://:9092,CONTROLLER://:9093 -e KAFKA_ADVERTISED_LISTENERS=PLAINTEXT://127.0.0.1:19094 \
  -e KAFKA_CONTROLLER_LISTENER_NAMES=CONTROLLER -e KAFKA_LISTENER_SECURITY_PROTOCOL_MAP=CONTROLLER:PLAINTEXT,PLAINTEXT:PLAINTEXT \
  -e KAFKA_CONTROLLER_QUORUM_VOTERS=1@localhost:9093 -e KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR=1 \
  -e KAFKA_TRANSACTION_STATE_LOG_REPLICATION_FACTOR=1 -e KAFKA_TRANSACTION_STATE_LOG_MIN_ISR=1 \
  -e KAFKA_GROUP_INITIAL_REBALANCE_DELAY_MS=0 -e KAFKA_NUM_PARTITIONS=3 -e KAFKA_HEAP_OPTS="-Xms256m -Xmx384m" \
  -p 127.0.0.1:19094:9092 apache/kafka:4.3.2     # ~40 s to start
go test -tags integration -race -count=1 ./internal/checkout/ -run TestKafka
docker rm -f d1-kafka
```

The unit tests (`events_test.go`) use a fake publisher and need nothing.

## Project structure

```
├── cmd/server/main.go         # entrypoint: tracing, profiling, gRPC server, downstream dials, metrics server
├── internal/checkout/         # PlaceOrder orchestration (service.go)
├── internal/money/            # Money arithmetic (Sum, MultiplySlow)
├── proto/                     # checkout.proto + generated Go (checkout.pb.go, checkout_grpc.pb.go)
├── Dockerfile
├── go.mod / go.sum
└── docs/                      # empty placeholder
```
