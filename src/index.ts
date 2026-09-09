import { startEmulator, stopEmulator } from './emulator.js';

const runningDevices = startEmulator({
  deviceCount: 5,
  ingestHost: '127.0.0.1',
  ingestPort: 4444,
  eventIntervalMs: 4000
})

process.on('SIGINT', () => {
  console.log('Stopping emulator...')

  stopEmulator(runningDevices)

  console.log('Emulator stopped successfully.')
  process.exit(0)
})