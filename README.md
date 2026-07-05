# topik

Typed pub/sub topics for Rust.

## What it looks like

Define your topics:

```rust
use topik::Topic;

#[derive(Topic)]
#[topic(segments("factory", "v2", device_id, kind))]
pub struct SensorReading {
    pub device_id: u64,
    pub kind: SensorKind,
    #[payload]
    pub data: TemperaturePayload,
}

#[derive(TopicSegment)]
pub enum SensorKind {
    Temperature,
    Humidity,
    Battery,
}
```

Publish with full type safety:

```rust
client.publish(SensorReading {
    device_id: 42,
    kind: SensorKind::Temperature,
    data: TemperaturePayload { value: 23.5 },
}).await?;
```

Subscribe with exhaustive pattern matching — no string parsing:

```rust
client.subscribe::<SensorReading>(|msg| async move {
    println!("device {} sent {}", msg.device_id, msg.data.value);
}).await?;
```

Wrong type is a compile error. Wrong topic structure is a compile error.
String parsing is gone entirely.

## Designed for legacy systems

Topik does not require you to rewrite everything at once. Start by mapping
your existing topics as-is, including the messy ones:

```rust
#[derive(Topic)]
#[topic(segments("legacy", "v1", device_id, "raw", kind, "data"))]
pub struct LegacySensor {
    pub device_id: u64,
    pub kind: SensorKind,
    #[payload]
    pub data: RawBytes,  // don't know the schema yet, that's fine
}
```

Gradually type the payload, clean up the topic structure. When ready, swap the backend from MQTT to NATS without touching application code.

## Status

Early development. The core traits and derive macros are being designed.
Not ready for production use.

Contributions and feedback on the API design are welcome.

## Backends

| Backend | Crate | Status |
|---------|-------|--------|
| In-memory (testing) | `topik-inmemory` | planned |
| NATS | `topik-nats` | planned |
| MQTT | `topik-mqtt` | planned |
| Redis | `topik-redis` | planned |

## License

Licensed under either of

- MIT license ([LICENSE-MIT](LICENSE-MIT))
- Apache License, Version 2.0 ([LICENSE-APACHE](LICENSE-APACHE))

at your option.
