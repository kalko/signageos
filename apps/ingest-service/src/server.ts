import net from 'node:net'
import type { MyEvent } from 'signageos-shared'
import { parseEventMessage } from './message.js'

export type Config = {
    host: string
    port: number
    publish: (event: MyEvent) => Promise<void>
}

function clientLabel(deviceId: string | undefined, remoteAddress: string | undefined, remotePort: number | undefined): string {
    if (deviceId) return `device ${deviceId}`
    return `${remoteAddress ?? 'unknown'}:${remotePort ?? '?'}`
}

export function startIngestServer(config: Config): () => void {
    const server = net.createServer((socket: net.Socket) => {
        let deviceId: string | undefined
        let buffer = ''

        console.log(`Client connected (${clientLabel(deviceId, socket.remoteAddress, socket.remotePort)})`)

        socket.on('data', (data: Buffer) => {
            buffer += data.toString()

            let newlineIndex: number
            while ((newlineIndex = buffer.indexOf('\n')) !== -1) {
                const line = buffer.slice(0, newlineIndex).trim()
                buffer = buffer.slice(newlineIndex + 1)

                if (line.length === 0) continue

                console.log(line)

                const result = parseEventMessage(line)
                if (!result.ok) {
                    if (result.reason === 'invalid_json') {
                        console.error('Invalid JSON, dropping message')
                    } else if (result.reason === 'invalid_event') {
                        console.error('Invalid event')
                    }
                    continue
                }

                const event = result.event
                deviceId ??= event.deviceId
                void config.publish(event).catch((error) => {
                    console.error('Failed to publish event:', error)
                })
            }
        })

        socket.on('error', (error: Error) => {
            console.error(error)
        })

        socket.on('close', () => {
            console.log(`Client disconnected (${clientLabel(deviceId, socket.remoteAddress, socket.remotePort)})`)
        })
    })
    server.listen(config.port, config.host, () => {
        console.log(`Ingest listening on ${config.host}:${config.port}`)
    })
    return () => {
        server.close()
    }
}