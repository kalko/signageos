import amqp, { type Channel, type ChannelModel } from "amqplib"
import type { MyEvent } from "signageos-shared"

export type PublisherConfig = {
  url: string
  queue?: string
  maxBufferSize?: number
  reconnectDelayMs?: number
}

export type Publisher = {
  url: string
  queue: string
  maxBufferSize: number
  reconnectDelayMs: number
  buffer: MyEvent[]
  connection?: ChannelModel
  channel?: Channel
  closed: boolean
  flushing: boolean
  reconnectTimer?: NodeJS.Timeout
}

export function initPublisher(config: PublisherConfig): Publisher {
  return {
    url: config.url,
    queue: config.queue ?? "signageos.events",
    maxBufferSize: config.maxBufferSize ?? 10_000,
    reconnectDelayMs: config.reconnectDelayMs ?? 2000,
    buffer: [],
    closed: false,
    flushing: false,
  }
}

export async function connectPublisher(publisher: Publisher): Promise<void> {
  try {
    await connect(publisher)
  } catch (error) {
    const message = error instanceof Error ? error.message : String(error)
    console.warn(`RabbitMQ not available at startup, buffering until reconnect: ${message}`)
    scheduleReconnect(publisher)
  }
}

export async function startPublisher(config: PublisherConfig): Promise<Publisher> {
  const publisher = initPublisher(config)
  await connectPublisher(publisher)
  return publisher
}

export async function publishEvent(publisher: Publisher, event: MyEvent): Promise<void> {
  if (publisher.closed) {
    throw new Error("Publisher is closed")
  }

  if (publisher.buffer.length >= publisher.maxBufferSize) {
    publisher.buffer.shift()
    console.warn(`Publisher buffer full (${publisher.maxBufferSize}), dropped oldest event`)
  }

  publisher.buffer.push(event)
  void flush(publisher)
}

export async function closePublisher(publisher: Publisher): Promise<void> {
  publisher.closed = true

  if (publisher.reconnectTimer) {
    clearTimeout(publisher.reconnectTimer)
    publisher.reconnectTimer = undefined
  }

  await publisher.channel?.close()
  await publisher.connection?.close()

  publisher.channel = undefined
  publisher.connection = undefined

  if (publisher.buffer.length > 0) {
    console.warn(`Publisher closed with ${publisher.buffer.length} buffered event(s) not delivered`)
  }
}

async function connect(publisher: Publisher): Promise<void> {
  if (publisher.closed || publisher.channel) return

  const connection = await amqp.connect(publisher.url)
  const channel = await connection.createChannel()
  await channel.assertQueue(publisher.queue, { durable: true })

  publisher.connection = connection
  publisher.channel = channel

  connection.on("error", (error) => {
    console.error("RabbitMQ connection error:", error.message)
  })

  connection.on("close", () => {
    if (publisher.closed) return
    console.warn("RabbitMQ connection closed, will reconnect...")
    publisher.channel = undefined
    publisher.connection = undefined
    scheduleReconnect(publisher)
  })

  console.log(`Connected to RabbitMQ queue "${publisher.queue}"`)
}

function scheduleReconnect(publisher: Publisher): void {
  if (publisher.closed || publisher.reconnectTimer) return

  publisher.reconnectTimer = setTimeout(() => {
    publisher.reconnectTimer = undefined
    void connect(publisher)
      .then(() => flush(publisher))
      .catch((error) => {
        const message = error instanceof Error ? error.message : String(error)
        console.error(`RabbitMQ reconnect failed: ${message}`)
        scheduleReconnect(publisher)
      })
  }, publisher.reconnectDelayMs)
}

async function flush(publisher: Publisher): Promise<void> {
  if (publisher.flushing || publisher.closed) return
  publisher.flushing = true

  try {
    while (publisher.buffer.length > 0 && !publisher.closed) {
      if (!publisher.channel) {
        try {
          await connect(publisher)
        } catch (error) {
          const message = error instanceof Error ? error.message : String(error)
          console.error(`RabbitMQ unavailable, buffering ${publisher.buffer.length} event(s): ${message}`)
          scheduleReconnect(publisher)
          return
        }
      }

      const event = publisher.buffer[0]
      if (!event) break

      try {
        await sendOne(publisher, event)
        publisher.buffer.shift()
      } catch (error) {
        const message = error instanceof Error ? error.message : String(error)
        console.error(`Failed to publish event, keeping in buffer (${publisher.buffer.length}): ${message}`)
        publisher.channel = undefined
        publisher.connection = undefined
        scheduleReconnect(publisher)
        return
      }
    }
  } finally {
    publisher.flushing = false
  }
}

async function sendOne(publisher: Publisher, event: MyEvent): Promise<void> {
  if (!publisher.channel) {
    throw new Error("RabbitMQ channel is not connected")
  }

  const body = Buffer.from(JSON.stringify(event))
  const options = {
    persistent: true,
    contentType: "application/json",
  }

  await new Promise<void>((resolve, reject) => {
    const attempt = () => {
      if (!publisher.channel) {
        reject(new Error("RabbitMQ channel is not connected"))
        return
      }

      try {
        const accepted = publisher.channel.sendToQueue(publisher.queue, body, options)
        if (accepted) {
          resolve()
          return
        }
        publisher.channel.once("drain", attempt)
      } catch (error) {
        reject(error)
      }
    }

    attempt()
  })
}
