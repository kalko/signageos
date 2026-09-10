import { z } from "zod"

export const MyEventSchema = z.object({
  deviceId: z.string().min(1),
  eventId: z.string().uuid(),
  emittedAt: z.string().datetime(),
  type: z.string().min(1),
  payload: z.object({
    temperatureC: z.number(),
    cpuPercent: z.number(),
    ramPercent: z.number(),
  }),
})

export type MyEvent = z.infer<typeof MyEventSchema>
