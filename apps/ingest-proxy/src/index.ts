import { parseBackends, startTcpProxy } from "./proxy.js"

const stop = startTcpProxy({
  listenHost: process.env["PROXY_HOST"] ?? "0.0.0.0",
  listenPort: Number(process.env["PROXY_PORT"] ?? 4000),
  backends: parseBackends(process.env["PROXY_BACKENDS"] ?? "127.0.0.1:4445,127.0.0.1:4446"),
})

process.on("SIGINT", () => {
  console.log("Stopping TCP proxy...")
  stop()
  process.exit(0)
})
