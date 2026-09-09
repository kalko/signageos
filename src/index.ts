import { startEmulator } from "./emulator.js"

startEmulator({
  deviceCount: 10,
  ingestHost: '127.0.0.1',
  ingestPort: 4444,
  eventIntervalMs: 10000
})
