import net from "node:net"
import amqp from "amqplib"
import { MongoClient, type Db } from "mongodb"
import type { MyEvent } from "signageos-shared"
import { afterAll, beforeAll, beforeEach, describe, expect, it } from "vitest"
import { closePublisher, publishEvent, startPublisher } from "../../ingest-service/src/publisher.js"
import { startIngestServer } from "../../ingest-service/src/server.js"
import { closeConsumer, startConsumer } from "./consumer.js"

const AMQP_URL = process.env["AMQP_URL"] ?? "amqp://127.0.0.1:5672"
const MONGODB_URL = process.env["MONGODB_URL"] ?? "mongodb://127.0.0.1:27017"
const RUN_ID = `${Date.now()}-${Math.random().toString(36).slice(2, 8)}`
const TEST_DB = `signageos_integration_${RUN_ID}`
const TEST_QUEUE = `signageos.events.integration.${RUN_ID}`
const TEST_INGEST_PORT = 4010

function testEvent(overrides: Partial<MyEvent> & Pick<MyEvent, "eventId" | "emittedAt">): MyEvent {
  return {
    deviceId: "integration-device-1",
    type: "testEvent",
    payload: { temperatureC: 21, cpuPercent: 10, ramPercent: 40 },
    ...overrides,
  }
}

async function isInfraAvailable(): Promise<boolean> {
  try {
    const rabbit = await amqp.connect(AMQP_URL)
    await rabbit.close()

    const mongo = new MongoClient(MONGODB_URL)
    await mongo.connect()
    await mongo.close()
    return true
  } catch {
    return false
  }
}

function sendTcpEvent(port: number, event: MyEvent): Promise<void> {
  return new Promise((resolve, reject) => {
    const socket = net.connect(port, "127.0.0.1")
    socket.on("error", reject)
    socket.on("connect", () => {
      socket.write(`${JSON.stringify(event)}\n`, (error) => {
        if (error) reject(error)
        else socket.end()
      })
    })
    socket.on("close", () => resolve())
  })
}

async function waitFor<T>(load: () => Promise<T | null>, timeoutMs = 10_000): Promise<T> {
  const deadline = Date.now() + timeoutMs
  while (Date.now() < deadline) {
    const value = await load()
    if (value !== null) return value
    await new Promise((resolve) => setTimeout(resolve, 100))
  }
  throw new Error("Timed out waiting for condition")
}

async function publishAll(publisher: Awaited<ReturnType<typeof startPublisher>>, events: MyEvent[]): Promise<void> {
  for (const event of events) {
    await publishEvent(publisher, event)
  }

  const deadline = Date.now() + 5_000
  while (publisher.buffer.length > 0 && Date.now() < deadline) {
    await new Promise((resolve) => setTimeout(resolve, 50))
  }

  if (publisher.buffer.length > 0) {
    throw new Error(`Publisher buffer not drained (${publisher.buffer.length} event(s) left)`)
  }
}

async function purgeQueue(queue: string): Promise<void> {
  const connection = await amqp.connect(AMQP_URL)
  const channel = await connection.createChannel()
  await channel.assertQueue(queue, { durable: true })
  await channel.purgeQueue(queue)
  await channel.close()
  await connection.close()
}

const infraAvailable = await isInfraAvailable()

describe.skipIf(!infraAvailable)("pipeline integration", () => {
  let stopIngest: (() => void) | undefined
  let publisher: Awaited<ReturnType<typeof startPublisher>>
  let mongoClient: MongoClient
  let db: Db

  beforeAll(async () => {
    publisher = await startPublisher({ url: AMQP_URL, queue: TEST_QUEUE })

    mongoClient = new MongoClient(MONGODB_URL)
    await mongoClient.connect()
    db = mongoClient.db(TEST_DB)

    stopIngest = startIngestServer({
      host: "127.0.0.1",
      port: TEST_INGEST_PORT,
      publish: (event) => publishEvent(publisher, event),
    })
  })

  afterAll(async () => {
    stopIngest?.()
    await closePublisher(publisher)
    await db.dropDatabase()
    await mongoClient.close()
  })

  describe("single consumer — end-to-end pipeline", () => {
    let consumer: Awaited<ReturnType<typeof startConsumer>>

    beforeAll(async () => {
      consumer = await startConsumer({
        url: AMQP_URL,
        queue: TEST_QUEUE,
        mongodbUrl: MONGODB_URL,
        mongodbDatabase: TEST_DB,
      })
    })

    afterAll(async () => {
      await closeConsumer(consumer)
    })

    it("stores an event sent over TCP through RabbitMQ into MongoDB", async () => {
      const event = testEvent({
        eventId: "11111111-1111-4111-8111-111111111111",
        emittedAt: "2026-01-01T10:00:00.000Z",
      })

      await sendTcpEvent(TEST_INGEST_PORT, event)

      const stored = await waitFor(async () => {
        const doc = await db.collection("events").findOne({ eventId: event.eventId })
        return doc ?? null
      })

      expect(stored?.["deviceId"]).toBe(event.deviceId)

      const device = await db.collection("devices").findOne({ deviceId: event.deviceId })
      expect(device?.["lastEventId"]).toBe(event.eventId)
      expect(device?.["emittedAt"]).toBe(event.emittedAt)
    })

    it("ignores duplicate delivery of the same eventId", async () => {
      const event = testEvent({
        eventId: "22222222-2222-4222-8222-222222222222",
        emittedAt: "2026-01-01T11:00:00.000Z",
      })

      await sendTcpEvent(TEST_INGEST_PORT, event)
      await waitFor(async () => {
        const doc = await db.collection("events").findOne({ eventId: event.eventId })
        return doc ?? null
      })

      await sendTcpEvent(TEST_INGEST_PORT, event)
      await new Promise((resolve) => setTimeout(resolve, 500))

      const count = await db.collection("events").countDocuments({ eventId: event.eventId })
      expect(count).toBe(1)
    })

    it("keeps newer device state when older events are processed later", async () => {
      const deviceId = "integration-device-order"
      const newer = testEvent({
        deviceId,
        eventId: "33333333-3333-4333-8333-333333333333",
        emittedAt: "2026-01-01T12:05:00.000Z",
        payload: { temperatureC: 30, cpuPercent: 20, ramPercent: 50 },
      })
      const older = testEvent({
        deviceId,
        eventId: "44444444-4444-4444-8444-444444444444",
        emittedAt: "2026-01-01T12:00:00.000Z",
        payload: { temperatureC: 10, cpuPercent: 5, ramPercent: 15 },
      })

      await publishEvent(publisher, newer)
      await publishEvent(publisher, older)

      const device = await waitFor(async () => {
        const doc = await db.collection("devices").findOne({ deviceId })
        if (doc?.["lastEventId"] === newer.eventId) return doc
        return null
      })

      expect(device?.["emittedAt"]).toBe(newer.emittedAt)
      expect(device?.["payload"]).toEqual(newer.payload)
      expect(await db.collection("events").countDocuments({ deviceId })).toBe(2)
    })
  })

  describe("scaled processing — two consumers on one queue", () => {
    let consumerA: Awaited<ReturnType<typeof startConsumer>>
    let consumerB: Awaited<ReturnType<typeof startConsumer>>

    beforeEach(async () => {
      await purgeQueue(TEST_QUEUE)
    })

    beforeAll(async () => {
      consumerA = await startConsumer({
        url: AMQP_URL,
        queue: TEST_QUEUE,
        mongodbUrl: MONGODB_URL,
        mongodbDatabase: TEST_DB,
      })
      consumerB = await startConsumer({
        url: AMQP_URL,
        queue: TEST_QUEUE,
        mongodbUrl: MONGODB_URL,
        mongodbDatabase: TEST_DB,
      })
    })

    afterAll(async () => {
      await closeConsumer(consumerA)
      await closeConsumer(consumerB)
    })

    it("processes different devices in parallel without cross-device conflicts", async () => {
      const deviceA = "parallel-device-a"
      const deviceB = "parallel-device-b"
      const eventA = testEvent({
        deviceId: deviceA,
        eventId: "55555555-5555-4555-8555-555555555555",
        emittedAt: "2026-01-01T13:00:00.000Z",
        payload: { temperatureC: 11, cpuPercent: 11, ramPercent: 11 },
      })
      const eventB = testEvent({
        deviceId: deviceB,
        eventId: "66666666-6666-4666-8666-666666666666",
        emittedAt: "2026-01-01T13:01:00.000Z",
        payload: { temperatureC: 22, cpuPercent: 22, ramPercent: 22 },
      })

      await publishAll(publisher, [eventA, eventB])

      await waitFor(async () => {
        const count = await db.collection("events").countDocuments({
          eventId: { $in: [eventA.eventId, eventB.eventId] },
        })
        return count === 2 ? count : null
      })

      const stateA = await db.collection("devices").findOne({ deviceId: deviceA })
      const stateB = await db.collection("devices").findOne({ deviceId: deviceB })

      expect(stateA?.["lastEventId"]).toBe(eventA.eventId)
      expect(stateA?.["payload"]).toEqual(eventA.payload)
      expect(stateB?.["lastEventId"]).toBe(eventB.eventId)
      expect(stateB?.["payload"]).toEqual(eventB.payload)
    })

    it("keeps correct state for one device when two consumers process conflicting events concurrently", async () => {
      const deviceId = "parallel-device-race"
      const newer = testEvent({
        deviceId,
        eventId: "77777777-7777-4777-8777-777777777777",
        emittedAt: "2026-01-01T14:05:00.000Z",
        payload: { temperatureC: 99, cpuPercent: 99, ramPercent: 99 },
      })
      const older = testEvent({
        deviceId,
        eventId: "88888888-8888-4888-8888-888888888888",
        emittedAt: "2026-01-01T14:00:00.000Z",
        payload: { temperatureC: 1, cpuPercent: 1, ramPercent: 1 },
      })

      await publishAll(publisher, [newer, older])

      const device = await waitFor(async () => {
        const doc = await db.collection("devices").findOne({ deviceId })
        if (doc?.["lastEventId"] === newer.eventId) return doc
        return null
      })

      expect(device?.["emittedAt"]).toBe(newer.emittedAt)
      expect(device?.["payload"]).toEqual(newer.payload)
      expect(await db.collection("events").countDocuments({ deviceId })).toBe(2)
    })

    it("inserts a duplicate eventId only once when two consumers receive it concurrently", async () => {
      const event = testEvent({
        deviceId: "parallel-device-dedup",
        eventId: "99999999-9999-4999-8999-999999999999",
        emittedAt: "2026-01-01T15:00:00.000Z",
      })

      await publishAll(publisher, [event, event, event, event])

      await waitFor(async () => {
        const count = await db.collection("events").countDocuments({ eventId: event.eventId })
        return count === 1 ? count : null
      })

      const device = await db.collection("devices").findOne({ deviceId: event.deviceId })
      expect(device?.["lastEventId"]).toBe(event.eventId)
    })

    it("distributes events across two consumer instances", async () => {
      const devices = [`scale-device-1-${RUN_ID}`, `scale-device-2-${RUN_ID}`, `scale-device-3-${RUN_ID}`]
      const eventIds = [
        ["aaaaaaaa-aaaa-4aaa-8aaa-000000000001", "aaaaaaaa-aaaa-4aaa-8aaa-000000000002"],
        ["bbbbbbbb-bbbb-4bbb-8bbb-000000000001", "bbbbbbbb-bbbb-4bbb-8bbb-000000000002"],
        ["cccccccc-cccc-4ccc-8ccc-000000000001", "cccccccc-cccc-4ccc-8ccc-000000000002"],
      ] as const
      const events = devices.flatMap((deviceId, index) => {
        const ids = eventIds[index]!
        const hour = String(10 + index * 10).padStart(2, "0")
        return [
          testEvent({
            deviceId,
            eventId: ids[0],
            emittedAt: `2026-01-01T16:${hour}:00.000Z`,
            payload: { temperatureC: 10 + index, cpuPercent: 10, ramPercent: 10 },
          }),
          testEvent({
            deviceId,
            eventId: ids[1],
            emittedAt: `2026-01-01T16:${hour}:05.000Z`,
            payload: { temperatureC: 20 + index, cpuPercent: 20, ramPercent: 20 },
          }),
        ]
      })

      await publishAll(publisher, events)

      await waitFor(async () => {
        const count = await db.collection("events").countDocuments({
          eventId: { $in: events.map((event) => event.eventId) },
        })
        if (count !== events.length) return null

        for (const [index, deviceId] of devices.entries()) {
          const expected = events[index * 2 + 1]?.eventId
          const state = await db.collection("devices").findOne({ deviceId })
          if (state?.["lastEventId"] !== expected) return null
        }

        return count
      })

      for (const [index, deviceId] of devices.entries()) {
        const state = await db.collection("devices").findOne({ deviceId })
        expect(state?.["lastEventId"]).toBe(events[index * 2 + 1]?.eventId)
        expect(state?.["emittedAt"]).toBe(events[index * 2 + 1]?.emittedAt)
      }
    })
  })
})
