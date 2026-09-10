import { createPublisher } from "./publisher.js"
import { startIngestServer } from "./server.js"

const publisher = await createPublisher({
  url: 'amqp://127.0.0.1:5672',
  queue: 'signageos.events',
})

const stop = startIngestServer({
  host: '127.0.0.1',
  port: 4444,
  publish: publisher.publish,
})

process.on("SIGINT", () => {
  console.log('Stopping ingest server...')

  stop()

  console.log('Ingest server stopped successfully.')
  process.exit(0)
})
