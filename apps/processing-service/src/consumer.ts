import amqp, { type Channel, type ChannelModel, type ConsumeMessage } from "amqplib"
import type { MyEvent } from "signageos-shared"
import {
  closeMongoStore,
  connectMongoStore,
  initMongoStore,
  type MongoStore,
  saveDeviceState,
  saveEvent,
} from "./store.js"

export type ConsumerConfig = {
  url: string
  mongodbUrl: string
  mongodbDatabase?: string
  queue?: string
  reconnectDelayMs?: number
}

export type Consumer = {
  url: string
  queue: string
  reconnectDelayMs: number
  closed: boolean
  reconnectTimer?: NodeJS.Timeout
  connection?: ChannelModel
  channel?: Channel
  consumerTag?: string
  mongoStore: MongoStore
}

export function initConsumer(config: ConsumerConfig): Consumer {
  return {
    url: config.url,
    queue: config.queue ?? "signageos.events",
    reconnectDelayMs: config.reconnectDelayMs ?? 2000,
    closed: false,
    mongoStore: initMongoStore({
      url: config.mongodbUrl,
      database: config.mongodbDatabase,
    }),
  }
}

export async function connectConsumer(consumer: Consumer): Promise<void> {
  try {
    await connect(consumer)
    await startConsuming(consumer)
  } catch (error) {
    const message = error instanceof Error ? error.message : String(error)
    console.warn(`RabbitMQ not available at startup, will reconnect: ${message}`)
    scheduleReconnect(consumer)
  }
}

export async function startConsumer(config: ConsumerConfig): Promise<Consumer> {
  const consumer = initConsumer(config)

  await connectMongoStore(consumer.mongoStore)
  await connectConsumer(consumer)

  return consumer
}

export async function closeConsumer(consumer: Consumer): Promise<void> {
  consumer.closed = true

  if (consumer.reconnectTimer) {
    clearTimeout(consumer.reconnectTimer)
    consumer.reconnectTimer = undefined
  }

  if (consumer.channel && consumer.consumerTag) {
    await consumer.channel.cancel(consumer.consumerTag)
  }

  await consumer.channel?.close()
  await consumer.connection?.close()

  consumer.channel = undefined
  consumer.connection = undefined
  consumer.consumerTag = undefined

  await closeMongoStore(consumer.mongoStore)
}

async function connect(consumer: Consumer): Promise<void> {
  if (consumer.closed || consumer.channel) return

  const connection = await amqp.connect(consumer.url)
  const channel = await connection.createChannel()
  await channel.assertQueue(consumer.queue, { durable: true })
  await channel.prefetch(1)

  consumer.connection = connection
  consumer.channel = channel

  connection.on("error", (error) => {
    console.error("RabbitMQ connection error:", error.message)
  })

  connection.on("close", () => {
    if (consumer.closed) return
    console.warn("RabbitMQ connection closed, will reconnect...")
    consumer.channel = undefined
    consumer.connection = undefined
    consumer.consumerTag = undefined
    scheduleReconnect(consumer)
  })

  console.log(`Connected to RabbitMQ queue "${consumer.queue}"`)
}

function scheduleReconnect(consumer: Consumer): void {
  if (consumer.closed || consumer.reconnectTimer) return

  consumer.reconnectTimer = setTimeout(() => {
    consumer.reconnectTimer = undefined
    void connect(consumer)
      .then(() => startConsuming(consumer))
      .catch((error) => {
        const message = error instanceof Error ? error.message : String(error)
        console.error(`RabbitMQ reconnect failed: ${message}`)
        scheduleReconnect(consumer)
      })
  }, consumer.reconnectDelayMs)
}

async function startConsuming(consumer: Consumer): Promise<void> {
  if (!consumer.channel) {
    throw new Error("RabbitMQ channel is not connected")
  }

  const reply = await consumer.channel.consume(consumer.queue, (incoming) => {
    if (!incoming) return

    void handleMessage(consumer, incoming).catch((error) => {
      const message = error instanceof Error ? error.message : String(error)
      console.error("Failed to handle message:", message)
    })
  })

  consumer.consumerTag = reply.consumerTag
  console.log(`Processing consumer listening on queue "${consumer.queue}"`)
}

async function handleMessage(consumer: Consumer, incoming: ConsumeMessage): Promise<void> {
  if (!consumer.channel) return

  const channel = consumer.channel
  let parsed: unknown

  try {
    parsed = JSON.parse(incoming.content.toString())
  } catch {
    console.error("Invalid message JSON, dropping")
    channel.ack(incoming)
    return
  }

  const event = parsed as MyEvent

  try {
    const result = await saveEvent(consumer.mongoStore, event)
    if (result === "duplicate") {
      console.warn(`Duplicate event ${event.eventId}, skipping insert`)
    }

    await saveDeviceState(consumer.mongoStore, event)
    channel.ack(incoming)
  } catch (error) {
    const message = error instanceof Error ? error.message : String(error)
    console.error("Failed to process event:", message)
    channel.nack(incoming, false, true)
  }
}
