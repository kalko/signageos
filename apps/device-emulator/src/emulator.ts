import net from 'node:net'
import type { MyEvent } from 'signageos-shared'

type EmulatorConfig = {
    deviceCount: number
    ingestHost: string
    ingestPort: number
    eventIntervalMs: number
    reconnectDelayMs?: number
}

type Device = {
    readonly deviceId: string
    readonly config: EmulatorConfig
    eventTimer?: NodeJS.Timeout
    reconnectTimer?: NodeJS.Timeout
    socket?: net.Socket
    stopped: boolean
}

export function startEmulator(config: EmulatorConfig): () => void {
    console.log(`Connecting ${config.deviceCount} device(s), ingest: ${config.ingestHost}:${config.ingestPort}`)

    const devices: Device[] = Array.from({ length: config.deviceCount }, (_, i) => ({
        deviceId: `device-${i + 1}`,
        config,
        stopped: false,
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
    if (device.stopped) return

    console.log(`Connecting device ${device.deviceId} to ${device.config.ingestHost}:${device.config.ingestPort}`)

    const socket = net.createConnection({
        host: device.config.ingestHost,
        port: device.config.ingestPort
    })

    device.socket = socket

    socket.on('connect', () => {
        console.log(`Device ${device.deviceId} connected to ${device.config.ingestHost}:${device.config.ingestPort}`)
        startDevice(device)
    })

    socket.on('error', (error) => {
        console.error(`Device ${device.deviceId} error: ${error.message}`)
    })

    socket.on('close', () => {
        stopDeviceEvents(device)
        device.socket = undefined

        if (device.stopped) {
            console.log(`Device ${device.deviceId} stopped`)
            return
        }

        const delayMs = device.config.reconnectDelayMs ?? 4000
        console.log(`Device ${device.deviceId} disconnected, reconnecting in ${delayMs}ms...`)
        device.reconnectTimer = setTimeout(() => {
            device.reconnectTimer = undefined
            connectDevice(device)
        }, delayMs)
    })
}

function startDevice(device: Device) {
    stopDeviceEvents(device)
    console.log(`Starting device ${device.deviceId}`)

    device.eventTimer = setInterval(() => {
        const event = createEvent(device.deviceId, new Date(), 'testEvent')
        console.log(`Device ${device.deviceId} event: ${JSON.stringify(event)}`)
        device.socket?.write(`${JSON.stringify(event)}\n`)
    }, device.config.eventIntervalMs)
}

function stopDeviceEvents(device: Device) {
    if (device.eventTimer) {
        clearInterval(device.eventTimer)
        device.eventTimer = undefined
    }
}

function stopDevice(device: Device) {
    console.log(`Stopping device ${device.deviceId}`)

    device.stopped = true

    if (device.reconnectTimer) {
        clearTimeout(device.reconnectTimer)
        device.reconnectTimer = undefined
    }

    stopDeviceEvents(device)
    device.socket?.destroy()
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