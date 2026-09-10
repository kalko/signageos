import { startEmulator } from './emulator.js'

const stop = startEmulator({
  deviceCount: Number(process.env['DEVICE_COUNT'] ?? 5),
  ingestHost: process.env['INGEST_HOST'] ?? '127.0.0.1',
  ingestPort: Number(process.env['INGEST_PORT'] ?? 4000),
  eventIntervalMs: Number(process.env['EVENT_INTERVAL_MS'] ?? 4000),
  reconnectDelayMs: Number(process.env['RECONNECT_DELAY_MS'] ?? 4000),
})

process.on('SIGINT', () => {
  console.log('Stopping emulator...')

  stop()

  console.log('Emulator stopped successfully.')
  process.exit(0)
})