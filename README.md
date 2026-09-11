# SignageOS, simple demo of telemetry pipeline

Devices send JSON events over TCP, ingest validates and publishes them to RabbitMQ, and processing workers persist history and per-device state in MongoDB.

## Prerequisites

- Node.js 20+
- [pnpm](https://pnpm.io/) 10+
- Docker (optional, necessary for infra or full stack)

## Local development

Start MongoDB and RabbitMQ on the host (ports 27017, 5672). If those ports are already open, `infra:start` just prints `Infra ready`:

```bash
pnpm infra:start
```

Run services in separate terminals:

```bash
pnpm dev:ingest
pnpm dev:processing
pnpm dev:emulator
```

Optional load balancer in front of ingest:

```bash
pnpm dev:ingest-proxy
```

Stop infra when done:

```bash
pnpm infra:stop
```

Environment variables for the emulator are read from `.env` (`DEVICE_COUNT`, `EVENT_INTERVAL_MS`).

## Docker (full stack)

Runs all services with internal MongoDB and RabbitMQ — no host ports except what you add yourself:

```bash
pnpm start
pnpm stop
```

`pnpm start` runs in the foreground and streams logs. Stop with **Ctrl+C** in that terminal, or run **`pnpm stop`** from another terminal — both are fine.

### Scaling

**From scratch with 2 ingest + 2 processing** (build + start):

```bash
pnpm start:scale
```

**While the stack is already running** — change only what you need; other services keep their current replica count:

```bash
pnpm scale -- --scale ingest=3          # only ingest
pnpm scale -- --scale processing=2      # only processing
pnpm scale -- --scale ingest=2 --scale processing=4   # both, any counts
```

`pnpm scale` does not rebuild images (`--no-recreate`). For a full rebuild, stop and use `pnpm start` or `pnpm start:scale` again.

## Tests

Unit tests:

```bash
pnpm test
```

Integration tests (TCP → ingest → RabbitMQ → MongoDB). Requires `pnpm infra:start`:

```bash
pnpm test:integration
```

## Project layout

| Path                      | Role                                       |
| ------------------------- | ------------------------------------------ |
| `apps/device-emulator`    | Simulates devices sending telemetry        |
| `apps/ingest-proxy`       | TCP round-robin load balancer              |
| `apps/ingest-service`     | TCP server, validation, RabbitMQ publisher |
| `apps/processing-service` | RabbitMQ consumer, MongoDB store           |
| `packages/shared`         | Event schema (Zod) and types               |
