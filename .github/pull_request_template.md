## What and why

<!-- What changes, and why. Link the ticket / board item. -->

## How it was tested

<!-- Commands you ran and their result. Anything not run: write "not run". -->

## Checklist

- [ ] Tests added or updated (unit; integration where a datastore or broker is involved), and they pass locally
- [ ] Docs updated: README, `docs/` pages and `catalog-info.yaml` match the code (env vars, ports, API)
- [ ] No secrets, tokens or real personal data in the diff, tests or logs
- [ ] Telemetry contract kept: on only with `OTEL_EXPORTER_OTLP_ENDPOINT`, `OTEL_SDK_DISABLED=true` forces off, profiling only with `PYROSCOPE_ADDR`
- [ ] New dependency (if any): reason in the commit message, pinned, newest release at least 7 days old
