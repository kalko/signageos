# SignageOS, simple demo of telemetry pipeline

Devices send JSON events over TCP, ingest validates and publishes them to RabbitMQ, and processing workers persist history and per-device state in MongoDB.

## Prerequisites

- Node.js 20+
- [pnpm](https://pnpm.io/) 10+
- Docker (optional, necessary for infra or full stack)

## Two modes (pick one)

| Mode            | Command                           | MongoDB / RabbitMQ     |
| --------------- | --------------------------------- | ---------------------- |
| **Local dev**   | `pnpm infra:start` + `pnpm dev:*` | Host ports 27017, 5672 |
| **Full Docker** | `pnpm start`                      | Internal to the stack  |

Do not run `pnpm infra:start` and `pnpm start` together — you would have two separate MongoDB and RabbitMQ instances. For day-to-day coding use local dev; for a full demo use Docker only.

## Local development

Start MongoDB, RabbitMQ, and HAProxy (port 4000):

```bash
pnpm infra:start
```

Run ingest on the host — up to **5** instances on ports **4001–4005** (HAProxy auto-detects which are up):

```bash
pnpm dev:ingest                              # :4001
INGEST_PORT=4002 pnpm dev:ingest             # :4002  (another terminal)
INGEST_PORT=4005 pnpm dev:ingest             # :4005
```

Processing and emulator:

```bash
pnpm dev:processing
pnpm dev:emulator        # localhost:4000 → HAProxy → running ingest instances
```

Stop infra when done:

```bash
pnpm infra:stop
```

Environment variables for the emulator are read from `.env` (`DEVICE_COUNT`, `EVENT_INTERVAL_MS`).

## Docker (full stack)

Self-contained — internal MongoDB, RabbitMQ, ingest, and HAProxy. Stop local infra first (`pnpm infra:stop`) if it is running:

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
pnpm scale --scale ingest=3          # only ingest
pnpm scale --scale processing=2      # only processing
pnpm scale --scale ingest=2 --scale processing=4   # both, any counts
```

`pnpm scale` does not rebuild images (`--no-recreate`). For a full rebuild, stop and use `pnpm start` or `pnpm start:scale` again.

## HAProxy configs

| File | Used by | Backends |
| ---- | ------- | -------- |
| `config/haproxy.cfg` | `pnpm start` (full stack) | Docker service `ingest:4000`, up to 100 replicas via DNS |
| `config/haproxy.dev.cfg` | `pnpm infra:start` | Host `host.docker.internal:4001–4005`, TCP check picks running instances |

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
| `config/haproxy.cfg`      | HAProxy for full Docker stack              |
| `config/haproxy.dev.cfg`  | HAProxy for local dev                      |
| `apps/device-emulator`    | Simulates devices sending telemetry        |
| `apps/ingest-service`     | TCP server, validation, RabbitMQ publisher |
| `apps/processing-service` | RabbitMQ consumer, MongoDB store           |
| `packages/shared`         | Event schema (Zod) and types               |
