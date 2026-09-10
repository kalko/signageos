import { closePublisher, publishEvent, startPublisher } from "./publisher.js"
import { startIngestServer } from "./server.js"

const publisher = await startPublisher({
  url: process.env["AMQP_URL"] ?? "amqp://127.0.0.1:5672",
  queue: process.env["AMQP_QUEUE"] ?? "signageos.events",
})

const stop = startIngestServer({
  host: process.env["INGEST_HOST"] ?? "0.0.0.0",
  port: Number(process.env["INGEST_PORT"] ?? 4000),
  publish: (event) => publishEvent(publisher, event),
})

process.on("SIGINT", async () => {
  console.log('Stopping ingest server...')

  stop()
  await closePublisher(publisher)

  console.log('Ingest server stopped successfully.')
  process.exit(0)
})
