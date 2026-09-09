import net from 'node:net'

type EmulatorConfig = {
    deviceCount: number
    ingestHost: string
    ingestPort: number
    eventIntervalMs: number
}

type Device = {
    readonly deviceId: string
    readonly config: EmulatorConfig
    eventTimer?: NodeJS.Timeout
    socket?: net.Socket
}

type MyEvent = {
    deviceId: string
    eventId: string
    emittedAt: string
    type: string
    payload: Record<string, unknown>
}

export function startEmulator(config: EmulatorConfig): () => void {
    console.log(`Connecting ${config.deviceCount} device(s), ingest: ${config.ingestHost}:${config.ingestPort}`)

    const devices: Device[] = Array.from({ length: config.deviceCount }, (_, i) => ({
        deviceId: `device-${i + 1}`,
        config,
        eventTimer: undefined
    }))

    for (const device of devices) {
        connectDevice(device)
    }

    return () => {
        for (const device of devices) {
            stopDevice(device)
        }
    }
}

export function stopEmulator(runningDevices: Device[]) {
    for (const device of runningDevices) {
        console.log(`Stopping device ${device.deviceId}`)

        clearInterval(device.eventTimer)
    }
}

function connectDevice(device: Device): void {
    console.log(`Connecting device ${device.deviceId} to ${device.config.ingestHost}:${device.config.ingestPort}`)

    const socket = net.createConnection({
        host: device.config.ingestHost,
        port: device.config.ingestPort
    })

    socket.on('connect', () => {
        console.log(`Device ${device.deviceId} connected to ${device.config.ingestHost}:${device.config.ingestPort}`)
        startDevice(device)
    })

    socket.on('error', (error) => {
        console.error(`Device ${device.deviceId} error: ${error}`)
    })

    socket.on('close', () => {
        console.log(`Device ${device.deviceId} closed`)
    })

    device.socket = socket
}


function startDevice(device: Device) {
    console.log(`Starting device ${device.deviceId}`)

    device.eventTimer = setInterval(() => {
        const event = createEvent(device.deviceId, new Date(), 'testEvent')
        console.log(`Device ${device.deviceId} event: ${JSON.stringify(event)}`)
        device.socket?.write(`event: ${JSON.stringify(event)}\n`)
    }, device.config.eventIntervalMs)
}

function stopDevice(device: Device) {
    console.log(`Stopping device ${device.deviceId}`)

    clearInterval(device.eventTimer)
    device.socket?.end()
}

function createEvent(
    deviceId: string,
    now = new Date(),
    type: string
): MyEvent {
    return {
        deviceId,
        eventId: crypto.randomUUID(),
        emittedAt: now.toISOString(),
        type,
        payload: createPayload()
    }
}

function createPayload() {
    return {
        temperatureC: Math.random() * 100,
        cpuPercent: Math.random() * 100,
        ramPercent: Math.random() * 100,
    }
}