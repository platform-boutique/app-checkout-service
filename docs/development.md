# Develop & test

## Checks and commands

Requirements: Go 1.26 (`GOTOOLCHAIN=auto` fetches the `go.mod` version; the host has Go 1.22), Docker for the image and the Kafka integration test, Python 3.12 for the docs check. The commands below are the ones the shared pipeline (`platform-demo-workflows`) runs, so a failure can be reproduced without pushing. `Running locally` above has the run commands.

| Step | Command | Result (2026-11-22 unless noted) |
|---|---|---|
| Format | `gofmt -l .` | run, no files listed |
| Vet and unit tests | `go vet ./... && go test -race -count=1 ./...` (also `go vet -tags integration` and `-tags e2e`) | run, passed (5 packages with tests; coverage: checkout 92.8%, obs 88.2%, healthcheck 87.5%, money 100%) |
| Lint | `golangci-lint run ./...` (CI pins golangci-lint v2.14.0) | run with a v2.14.0 binary built from source: 0 issues without a tag, with `--build-tags integration` and with `--build-tags e2e` |
| Vulnerabilities and security | `govulncheck ./...`, `gosec -exclude-generated ./...` | run: no vulnerabilities found; gosec 0 issues (24 files, 3148 lines) |
| Kafka integration test | see [Kafka integration tests](#kafka-integration-tests): `go test -tags integration -race -count=1 ./internal/checkout/ -run TestKafka` | run against a throwaway `apache/kafka:4.3.2`: the 5 `TestKafka_*` tests pass with `-race`, and `-tags integration ./...` passes |
| End-to-end test | `go test -tags e2e -race -count=1 ./tests/e2e/` (in-process fake downstream servers, no stack needed) | run, passed (4 tests) |
| Proto | `protoc -I proto --go_out=proto --go_opt=paths=source_relative --go-grpc_out=proto --go-grpc_opt=paths=source_relative proto/checkout.proto` (protoc 3.21.12, protoc-gen-go 1.36.11, protoc-gen-go-grpc 1.6.1, the versions in the generated files). The vendored `proto/orders` and `proto/inventory` are copies of the other services' contracts with their own `go_package` | not run. Changes are additive only, server first |
| Image | `docker build -t platform-demo/checkout-service:local .` | not run for this version |
| Image scan | `trivy image --severity CRITICAL,HIGH --exit-code 1 platform-demo/checkout-service:local` | not run for this version |
| Secret scan | `pre-commit run --all-files` (`.pre-commit-config.yaml`, gitleaks v8.30.1) or `gitleaks git --redact` | run with the gitleaks binary of the pre-commit cache (`gitleaks git --redact`, full history, 79 commits, 2026-11-22): no leaks found |
| Workflow lint | `actionlint` | run (actionlint 1.7.12), no findings |
| Docs | `mkdocs build --strict` (mkdocs 1.6.1 + mkdocs-techdocs-core 1.7.1, Python 3.12 venv) and the link check `python3 -I tools/check-md-links.py .` from `platform-demo-workflows` (relative links in `README.md` and `docs/`) | run, both passed |

## Running locally

Requirements: Go 1.26 (`GOTOOLCHAIN=auto` fetches the `go.mod` version), Docker, [`grpcurl`](https://github.com/fullstorydev/grpcurl), and the six downstream services (or stand-ins) for a *successful* order.

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

The tests (`TestKafka_*`): `PlaceOrder_PublishesRealEvent_ReadBack` (record, headers, JSON), `SameKeySamePartition`, `TopicPrefix_RecordLandsInPrefixedTopic` (`KAFKA_TOPIC_PREFIX`), `FirstPublishCreatesTheTopic` (the topic does not exist, the first publish creates it with the broker's 3 partitions, a second creation request is success) and `BrokerDown_PlaceOrderStillSucceedsWithinBound`. Use any free port for the broker (`19194` was used on 2026-11-22: change both `KAFKA_ADVERTISED_LISTENERS` and the `-p` mapping and set `KAFKA_TEST_BROKERS=127.0.0.1:19194`).

The unit tests (`events_test.go`, `kafka_topic_test.go`) use a fake publisher / fake broker and need nothing.

## Project structure

```
├── cmd/server/main.go         # entrypoint: tracing, profiling, gRPC server, downstream dials, metrics server
├── internal/healthcheck/healthcheck.go  # `healthcheck` subcommand (container HEALTHCHECK): grpc.health.v1 Check on the own port
├── internal/checkout/         # PlaceOrder orchestration (service.go)
├── internal/money/            # Money arithmetic (Sum, MultiplySlow)
├── proto/                     # checkout.proto + generated Go (checkout.pb.go, checkout_grpc.pb.go)
├── Dockerfile
├── go.mod / go.sum
└── docs/                      # TechDocs pages (mkdocs.yml in the root), docs/adr/ = decisions
```
