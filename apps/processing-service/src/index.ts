import { startConsumer } from "./consumer.js"

const consumer = await startConsumer({
  rabbitmqUrl: 'amqp://127.0.0.1:5672',
  mongodbUrl: 'mongodb://127.0.0.1:27017',
})

process.on("SIGINT", async () => {
  console.log('Stopping processing service...')

  await consumer.close()

  console.log('Processing service stopped successfully.')
  process.exit(0)
})
