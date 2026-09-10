export type MyEvent = {
  deviceId: string
  eventId: string
  emittedAt: string
  type: string
  payload: Record<string, unknown>
}
