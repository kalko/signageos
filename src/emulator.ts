type EmulatorConfig = {
    deviceCount: number
    ingestHost: string
    ingestPort: number
    eventIntervalMs: number
}

export function startEmulator(config: EmulatorConfig) {
    console.log(config)
}