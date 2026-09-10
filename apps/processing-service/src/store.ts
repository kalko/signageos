import { MongoClient, type Collection, type MongoServerError } from "mongodb"
import type { MyEvent } from "signageos-shared"

export type StoreConfig = {
  url: string
  database?: string
}

export type MongoStore = {
  url: string
  database: string
  client?: MongoClient
  events?: Collection
  devices?: Collection
  closed: boolean
}

export function initMongoStore(config: StoreConfig): MongoStore {
  return {
    url: config.url,
    database: config.database ?? "signageos",
    closed: false,
  }
}

export async function connectMongoStore(store: MongoStore): Promise<void> {
  if (store.closed || store.client) return

  const client = new MongoClient(store.url)
  await client.connect()

  const db = client.db(store.database)
  store.client = client
  store.events = db.collection("events")
  store.devices = db.collection("devices")

  await store.events.createIndex({ eventId: 1 }, { unique: true })
  await store.devices.createIndex({ deviceId: 1 }, { unique: true })

  console.log(`Connected to MongoDB database "${store.database}"`)
}

export async function closeMongoStore(store: MongoStore): Promise<void> {
  store.closed = true

  await store.client?.close()

  store.client = undefined
  store.events = undefined
  store.devices = undefined
}

export async function saveEvent(store: MongoStore, event: MyEvent): Promise<"inserted" | "duplicate"> {
  if (!store.events) {
    throw new Error("MongoDB events collection is not connected")
  }

  try {
    await store.events.insertOne({
      ...event,
      receivedAt: new Date(),
    })
    return "inserted"
  } catch (error) {
    if (isDuplicateKeyError(error)) {
      return "duplicate"
    }
    throw error
  }
}

export async function saveDeviceState(store: MongoStore, event: MyEvent): Promise<void> {
  if (!store.devices) {
    throw new Error("MongoDB devices collection is not connected")
  }

  await store.devices.updateOne(
    {
      deviceId: event.deviceId,
      $or: [{ emittedAt: { $exists: false } }, { emittedAt: { $lt: event.emittedAt } }],
    },
    {
      $set: {
        deviceId: event.deviceId,
        lastEventId: event.eventId,
        lastEventType: event.type,
        emittedAt: event.emittedAt,
        payload: event.payload,
        updatedAt: new Date(),
      },
    },
    { upsert: true }
  )
}

function isDuplicateKeyError(error: unknown): boolean {
  return (
    typeof error === "object" &&
    error !== null &&
    "code" in error &&
    (error as MongoServerError).code === 11000
  )
}
