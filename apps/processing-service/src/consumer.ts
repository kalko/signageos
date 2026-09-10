import amqp, { type Channel, type ChannelModel, type ConsumeMessage } from "amqplib"
import { MongoClient, type Collection } from "mongodb"
import { type MyEvent } from "signageos-shared"


export type Config = {
    rabbitmqUrl: string
    mongodbUrl: string
    queue?: string
}

export type Consumer = {
    close: () => Promise<void>
}

export async function startConsumer(config: Config): Promise<Consumer> {
    const queue = config.queue ?? 'signageos.events'
    const mongo = new MongoClient(config.mongodbUrl)
    await mongo.connect()

    const db = mongo.db('signageos')
    const eventsTable = db.collection("events")

    const connection: ChannelModel = await amqp.connect(config.rabbitmqUrl)
    const channel: Channel = await connection.createChannel()

    await channel.assertQueue(queue, { durable: true })

    await channel.consume(queue, (message) => {
        if (!message) {
            return
        }
        void handleMessage(channel, message, { eventsTable }).catch((error) => {
            console.error("Failed to handle message:", error)
        })
    })

    console.log(`Processing consumer listening on queue "${queue}"`)

    return {
        async close(): Promise<void> {
            await channel.close()
            await connection.close()
            await mongo.close()
        }
    }
}

async function handleMessage(
    channel: Channel,
    message: ConsumeMessage,
    collections: {
        eventsTable: Collection
    }
): Promise<void> {
    let parsed: unknown

    try {
        parsed = JSON.parse(message.content.toString())
    } catch {
        console.error("Invalid message JSON, dropping")
        channel.ack(message)
        return
    }

    try {
        await collections.eventsTable.insertOne({
            ...(parsed as MyEvent),
            receivedAt: new Date()
        })
        channel.ack(message)
    } catch (error) {
        const messageText = error instanceof Error ? error.message : "Processing failed"
        console.error("Failed to process event:", messageText)
        channel.nack(message, false, true)
    }
}
