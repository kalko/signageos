import net from 'node:net'

export type Config = {
    host: string
    port: number
}

export function startIngestServer(config: Config): () => void {
    const server = net.createServer((socket: net.Socket) => {
        socket.on('data', (data: Buffer) => {
            console.log(data.toString())
        })
    })
    server.listen(config.port, config.host)
    console.log(`Ingest server listening on ${config.host}:${config.port}`)
    return () => {
        server.close()
        console.log(`Ingest server closed`)
    }
}