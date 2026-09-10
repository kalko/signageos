
import amqp, { type Channel, type ChannelModel } from "amqplib"
import type { MyEvent } from 'signageos-shared'

export type Config = {
  url: string
  queue?: string
}

export type Publisher = {
  publish: (event: MyEvent) => Promise<void>
  close: () => Promise<void>
}

export async function createPublisher(config: Config): Promise<Publisher> {
  const queue = config.queue ?? 'signageos.events'
  const connection: ChannelModel = await amqp.connect(config.url)
  const channel: Channel = await connection.createChannel()

  await channel.assertQueue(queue, { durable: true })

  return {
    async publish(event: MyEvent): Promise<void> {
      const body = Buffer.from(JSON.stringify(event))
      const accepted = channel.sendToQueue(queue, body, {
        persistent: true,
        contentType: "application/json"
      })

      if (!accepted) {
        await new Promise<void>((resolve) => channel.once("drain", resolve))
      }
    },

    async close(): Promise<void> {
      await channel.close()
      await connection.close()
    }
  }
}
