import { MyEventSchema, type MyEvent } from "signageos-shared"

export type ParseEventResult =
  | { ok: true; event: MyEvent }
  | { ok: false; reason: "empty" | "invalid_json" | "invalid_event" }

export function parseEventMessage(line: string): ParseEventResult {
  const trimmed = line.trim()
  if (trimmed.length === 0) {
    return { ok: false, reason: "empty" }
  }

  let parsed: unknown
  try {
    parsed = JSON.parse(trimmed)
  } catch {
    return { ok: false, reason: "invalid_json" }
  }

  const result = MyEventSchema.safeParse(parsed)
  if (!result.success) {
    return { ok: false, reason: "invalid_event" }
  }

  return { ok: true, event: result.data }
}
