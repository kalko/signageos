import { closeConsumer, startConsumer } from "./consumer.js"

const consumer = await startConsumer({
  url: process.env["AMQP_URL"] ?? "amqp://127.0.0.1:5672",
  mongodbUrl: process.env["MONGODB_URL"] ?? "mongodb://127.0.0.1:27017",
  mongodbDatabase: process.env["MONGODB_DATABASE"] ?? "signageos",
  queue: process.env["AMQP_QUEUE"] ?? "signageos.events",
})

process.on("SIGINT", async () => {
  console.log("Stopping processing service...")

  await closeConsumer(consumer)

  console.log("Processing service stopped successfully.")
  process.exit(0)
})
