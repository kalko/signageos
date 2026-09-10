import type { Collection } from "mongodb"
import type { MyEvent } from "signageos-shared"
import { describe, expect, it } from "vitest"
import {
  initMongoStore,
  saveDeviceState,
  saveEvent,
  type MongoStore,
} from "./store.js"

function testEventId(digit: 1 | 2 | 3 | 4 | 5 | 6 | 7 | 8 | 9): string {
  const d = String(digit)
  return `${d.repeat(8)}-${d.repeat(4)}-4${d.repeat(3)}-8${d.repeat(3)}-${d.repeat(12)}`
}

function event(
  overrides: Partial<MyEvent> & Pick<MyEvent, "eventId" | "emittedAt">
): MyEvent {
  return {
    deviceId: "device-1",
    type: "testEvent",
    payload: { temperatureC: 20, cpuPercent: 10, ramPercent: 30 },
    ...overrides,
  }
}

function createInMemoryStore(): {
  store: MongoStore
  events: Map<string, MyEvent & { receivedAt: Date }>
  devices: Map<string, Record<string, unknown>>
} {
  const events = new Map<string, MyEvent & { receivedAt: Date }>()
  const devices = new Map<string, Record<string, unknown>>()

  const store = initMongoStore({ url: "memory://test", database: "test" })

  store.events = {
    async insertOne(doc: MyEvent & { receivedAt: Date }) {
      if (events.has(doc.eventId)) {
        const error = Object.assign(new Error("duplicate key"), { code: 11000 })
        throw error
      }
      events.set(doc.eventId, doc)
      return { acknowledged: true, insertedId: doc.eventId }
    },
  } as unknown as Collection

  store.devices = {
    async updateOne(
      filter: { deviceId: string; $or: Array<Record<string, unknown>> },
      update: { $set: Record<string, unknown> }
    ) {
      const existing = devices.get(filter.deviceId)
      const incomingEmittedAt = update.$set["emittedAt"] as string

      const isNewer =
        !existing ||
        !("emittedAt" in existing) ||
        (existing["emittedAt"] as string) < incomingEmittedAt

      if (isNewer) {
        devices.set(filter.deviceId, update.$set)
        return { matchedCount: existing ? 1 : 0, modifiedCount: 1, upsertedCount: existing ? 0 : 1 }
      }

      return { matchedCount: 0, modifiedCount: 0, upsertedCount: 0 }
    },
  } as unknown as Collection

  return { store, events, devices }
}

describe("saveEvent", () => {
  it("inserts a unique event once", async () => {
    const { store, events } = createInMemoryStore()
    const e = event({ eventId: testEventId(1), emittedAt: "2026-01-01T10:00:00.000Z" })

    expect(await saveEvent(store, e)).toBe("inserted")
    expect(events.size).toBe(1)
  })

  it("returns duplicate for the same eventId without a second insert", async () => {
    const { store, events } = createInMemoryStore()
    const e = event({ eventId: testEventId(2), emittedAt: "2026-01-01T10:00:00.000Z" })

    expect(await saveEvent(store, e)).toBe("inserted")
    expect(await saveEvent(store, e)).toBe("duplicate")
    expect(events.size).toBe(1)
  })
})

describe("saveDeviceState", () => {
  it("keeps the newer state when events arrive out of order", async () => {
    const { store, devices } = createInMemoryStore()
    const newer = event({
      eventId: testEventId(3),
      emittedAt: "2026-01-01T10:05:00.000Z",
      payload: { temperatureC: 25, cpuPercent: 50, ramPercent: 60 },
    })
    const older = event({
      eventId: testEventId(4),
      emittedAt: "2026-01-01T10:00:00.000Z",
      payload: { temperatureC: 10, cpuPercent: 5, ramPercent: 15 },
    })

    await saveDeviceState(store, newer)
    await saveDeviceState(store, older)

    const state = devices.get("device-1")
    expect(state?.["lastEventId"]).toBe(newer.eventId)
    expect(state?.["emittedAt"]).toBe(newer.emittedAt)
    expect(state?.["payload"]).toEqual(newer.payload)
  })

  it("does not apply a duplicate replay of the same event", async () => {
    const { store, devices } = createInMemoryStore()
    const e = event({
      eventId: testEventId(5),
      emittedAt: "2026-01-01T10:00:00.000Z",
      payload: { temperatureC: 30, cpuPercent: 40, ramPercent: 50 },
    })

    await saveDeviceState(store, e)
    await saveDeviceState(store, e)

    const state = devices.get("device-1")
    expect(state?.["lastEventId"]).toBe(e.eventId)
    expect(state?.["payload"]).toEqual(e.payload)
  })

  it("reflects logical order of unique events from the device", async () => {
    const { store, devices } = createInMemoryStore()
    const first = event({
      eventId: testEventId(6),
      emittedAt: "2026-01-01T10:00:00.000Z",
      payload: { temperatureC: 11, cpuPercent: 12, ramPercent: 13 },
    })
    const second = event({
      eventId: testEventId(7),
      emittedAt: "2026-01-01T10:01:00.000Z",
      payload: { temperatureC: 21, cpuPercent: 22, ramPercent: 23 },
    })

    await saveDeviceState(store, first)
    await saveDeviceState(store, second)

    const state = devices.get("device-1")
    expect(state?.["lastEventId"]).toBe(second.eventId)
    expect(state?.["emittedAt"]).toBe(second.emittedAt)
    expect(state?.["payload"]).toEqual(second.payload)
  })
})

describe("duplicate delivery has no duplicate business effect", () => {
  it("skips duplicate insert and leaves device state unchanged on replay", async () => {
    const { store, events, devices } = createInMemoryStore()
    const e = event({
      eventId: testEventId(8),
      emittedAt: "2026-01-01T10:00:00.000Z",
      payload: { temperatureC: 42, cpuPercent: 7, ramPercent: 9 },
    })

    expect(await saveEvent(store, e)).toBe("inserted")
    await saveDeviceState(store, e)

    expect(await saveEvent(store, e)).toBe("duplicate")
    await saveDeviceState(store, e)

    expect(events.size).toBe(1)
    const state = devices.get("device-1")
    expect(state?.["lastEventId"]).toBe(e.eventId)
    expect(state?.["payload"]).toEqual(e.payload)
  })
})
