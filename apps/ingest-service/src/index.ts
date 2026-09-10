import { startIngestServer } from "./server.js"


const stop = startIngestServer({
  host: '127.0.0.1',
  port: 4444,
})

process.on("SIGINT", () => {
  console.log('Stopping ingest server...')

  stop()

  console.log('Ingest server stopped successfully.')
  process.exit(0)
})
