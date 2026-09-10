import net from "node:net"

export type Backend = {
  host: string
  port: number
}

export type ProxyConfig = {
  listenHost: string
  listenPort: number
  backends: Backend[]
}

export function startTcpProxy(config: ProxyConfig): () => void {
  if (config.backends.length === 0) {
    throw new Error("At least one backend is required")
  }

  let nextBackend = 0

  const server = net.createServer((clientSocket) => {
    const backend = config.backends[nextBackend % config.backends.length]!
    nextBackend += 1

    const label = `${backend.host}:${backend.port}`
    console.log(`Proxy: routing client to ${label}`)

    const backendSocket = net.connect({ host: backend.host, port: backend.port })

    backendSocket.on("connect", () => {
      clientSocket.pipe(backendSocket)
      backendSocket.pipe(clientSocket)
    })

    backendSocket.on("error", (error) => {
      console.error(`Proxy: backend ${label} error: ${error.message}`)
      clientSocket.destroy()
    })

    clientSocket.on("error", (error) => {
      console.error(`Proxy: client error: ${error.message}`)
      backendSocket.destroy()
    })

    clientSocket.on("close", () => {
      backendSocket.end()
    })

    backendSocket.on("close", () => {
      clientSocket.end()
    })
  })

  server.listen(config.listenPort, config.listenHost, () => {
    const backendList = config.backends.map((b) => `${b.host}:${b.port}`).join(", ")
    console.log(`TCP proxy listening on ${config.listenHost}:${config.listenPort}`)
    console.log(`Backends: ${backendList}`)
  })

  return () => {
    server.close()
  }
}

export function parseBackends(value: string): Backend[] {
  return value.split(",").map((entry) => {
    const trimmed = entry.trim()
    const colonIndex = trimmed.lastIndexOf(":")
    if (colonIndex === -1) {
      throw new Error(`Invalid backend "${entry}", expected host:port`)
    }

    const host = trimmed.slice(0, colonIndex)
    const port = Number(trimmed.slice(colonIndex + 1))
    if (!host || !Number.isInteger(port) || port <= 0) {
      throw new Error(`Invalid backend "${entry}", expected host:port`)
    }

    return { host, port }
  })
}
