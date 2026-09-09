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
}

export function startEmulator(config: EmulatorConfig): Device[] {
    console.log(`Starting ${config.deviceCount} device(s), ingest: ${config.ingestHost}:${config.ingestPort}`)

    const devices: Device[] = Array.from({ length: config.deviceCount }, (_, i) => ({
        deviceId: `device-${i + 1}`,
        config,
        eventTimer: undefined
    }))

    for (const device of devices) {
        startDevice(device)
    }

    return devices
}

export function stopEmulator(runningDevices: Device[]) {
    for (const device of runningDevices) {
        console.log(`Stopping device ${device.deviceId}`)

        clearInterval(device.eventTimer)
    }

}

function startDevice(device: Device) {
    console.log(`Starting device ${device.deviceId}`)

    device.eventTimer = setInterval(() => {
        console.log(`Device ${device.deviceId} event: ${new Date().toISOString()}`)
    }, device.config.eventIntervalMs)
}
