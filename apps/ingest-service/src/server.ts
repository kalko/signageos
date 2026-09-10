import net from 'node:net'
import type { MyEvent } from 'signageos-shared'

export type Config = {
    host: string
    port: number
    publish: (event: MyEvent) => Promise<void>
}

export function startIngestServer(config: Config): () => void {
    const server = net.createServer((socket: net.Socket) => {
        console.log('Client connected')
        let buffer = ''

        socket.on('data', (data: Buffer) => {
            buffer += data.toString()

            let newlineIndex: number
            while ((newlineIndex = buffer.indexOf('\n')) !== -1) {
                const line = buffer.slice(0, newlineIndex).trim()
                buffer = buffer.slice(newlineIndex + 1)

                if (line.length === 0) continue

                console.log(line)

                try {
                    const event = JSON.parse(line) as MyEvent
                    void config.publish(event).catch((error) => {
                        console.error('Failed to publish event:', error)
                    })
                } catch (error) {
                    console.error('Failed to parse event:', error)
                }
            }
        })

        socket.on('error', (error: Error) => {
            console.error(error)
        })

        socket.on('close', () => {
            console.log('Client disconnected')
        })
    })
    server.listen(config.port, config.host, () => {
        console.log(`Ingest listening on ${config.host}:${config.port}`)
    })
    return () => {
        server.close()
    }
}