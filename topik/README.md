# topik

<p align="center">
  <img src="https://raw.githubusercontent.com/railforge/topik/main/assets/topik-logo.png" alt="topik logo" width="200"/>
</p>

[![CI](https://github.com/railforge/topik/workflows/CI/badge.svg)](https://github.com/railforge/topik/actions)
[![Crates.io](https://img.shields.io/crates/v/topik.svg)](https://crates.io/crates/topik)
[![docs.rs](https://docs.rs/topik/badge.svg)](https://docs.rs/topik)
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](LICENSE)

You know your topics. Now your compiler can too.

Topik brings compile-time type safety to pub/sub. Define your messaging infrastructure as a versioned Rust crate. One source of truth that grows with your system. New topics, new protocols, new services. Every change tracked in code, every type checked at compile time.

---

## Quick start

A raw MQTT topic:

```
factory/sensors/42/temperature → f32
```

Becomes a typed Rust definition:

```rust
#[derive(Topic)]
#[topic(segments("factory", "sensors", device_id, "temperature"), encoding = F32Encoding)]
pub struct TemperatureReading {
    pub device_id: u64,
    #[payload]
    pub data: f32,
}
```

Publish:

```rust
client.publish(TemperatureReading { device_id: 42, data: 23.5 }).await?;
```

Subscribe:

```rust
let mut sub = client.subscribe::<TemperatureReading>().await?;
while let Some(msg) = sub.next().await {
    println!("device {}: {}°C", msg.device_id, msg.data);
}
```

---

## Client

Create a client by choosing a protocol. The protocol defines the separator and wildcard conventions used throughout.

```rust
use topik::TopikClient;
use topik::transport::InMemoryTransport;
use topik::protocol::Mqtt;

let client = TopikClient::new(InMemoryTransport::<Mqtt>::new());
```

Swap the transport to connect to a real broker. The rest of your code stays the same:

```rust
// coming in v0.2.0
let client = TopikClient::new(MqttTransport::new("mqtt://localhost:1883").await?);
```

Use `InMemoryTransport` in tests. No broker needed.

```rust
#[tokio::test]
async fn test_reading() {
    let client = TopikClient::new(InMemoryTransport::<Mqtt>::new());
    // same API as production
}
```

## Publishing

The topic string is rendered automatically. No separators in your code.

```rust
client.publish(TemperatureReading {
    device_id: 42,
    data: 23.5,
}).await?;
```

See what gets sent to the broker:

```rust
println!("{}", client.display(&reading));
// MQTT  -> "factory/sensors/42/temperature"
// NATS  -> "factory.sensors.42.temperature"
```

## Subscribing

Subscribe to all messages of a topic type:

```rust
let mut sub = client.subscribe::<TemperatureReading>().await?;

while let Some(msg) = sub.next().await {
    println!("device {} → {}°C", msg.device_id, msg.data);
}
```

Subscribe to a specific device only:

```rust
let mut sub = client
    .subscribe::<TemperatureReading>()
    .pin(|b| b.device_id(42))
    .await?;
```

Check the subscription pattern:

```rust
println!("{}", sub.pattern());
// MQTT -> "factory/sensors/+/temperature"
```

## Multiple topic types

Group related topics into an enum to handle them through a single subscriber.

```rust
#[derive(TopicEnum)]
enum SensorTopics {
    Temperature(TemperatureReading),
    Humidity(HumidityReading),
    Reboot(RebootCommand),
}

let mut sub = client.subscribe_many::<SensorTopics>().await?;

while let Some(event) = sub.next().await {
    match event {
        SensorTopics::Temperature(msg) => println!("{}°C", msg.data),
        SensorTopics::Humidity(msg) => println!("{}%", msg.data),
        SensorTopics::Reboot(msg) => println!("reboot {}", msg.device_id),
    }
}
```

The compiler enforces exhaustive matching. Missing a variant is a compile error.

`InMemoryTransport` also works as a typed in-process event bus — no broker needed:

```rust
let transport = InMemoryTransport::<Mqtt>::new();

let producer = TopikClient::new(transport.clone());
let consumer = TopikClient::new(transport.clone());

tokio::spawn(async move {
    producer.publish(TemperatureReading { device_id: 1, data: 23.5 }).await?;
});

let mut sub = consumer.subscribe_many::<SensorTopics>().await?;
while let Some(event) = sub.next().await {
    match event { ... }
}
```

## Defining topics

A topic is a Rust struct with `#[derive(Topic)]`.

```rust
#[derive(Topic)]
#[topic(segments("factory", "v2", device_id, kind), encoding = F32Encoding)]
pub struct SensorReading {
    pub device_id: u64,
    pub kind: SensorKind,
    #[payload]
    pub data: f32,
}
```

**Segments** define the topic path in order. Two kinds:

- **String literals**: fixed parts of the path: `"factory"`, `"v2"`
- **Field names**: dynamic typed segments: `device_id`, `kind`

**Payload** is marked with `#[payload]`. One field per topic. Never appears in the topic path, only in the message body.

**Encoding** defines how the payload is serialized on the wire.

Segments are listed in order as comma-separated arguments.

Any topic structure is expressible, including messy legacy ones:

```rust
#[derive(Topic)]
#[topic(segments("legacy", "v1", device_id, "raw", kind, "data"), encoding = RawEncoding)]
pub struct LegacySensor {
    pub device_id: u64,
    pub kind: String,
    #[payload]
    pub data: Bytes,
}
```

## Encodings

The `encoding` attribute defines how the payload is serialized on the wire.

| Encoding | Payload type |
|----------|-------------|
| `RawEncoding` | `Bytes` |
| `StringEncoding` | `String` |
| `BoolEncoding<R>` | `bool` |
| `U8Encoding` – `U64Encoding` | unsigned integers |
| `I32Encoding`, `I64Encoding` | signed integers |
| `F32Encoding`, `F64Encoding` | floats |

`BoolEncoding` is configurable. Choose how `true` and `false` are encoded:

```rust
// "1" / "0" common in legacy systems
type ActiveFlag = BoolSegment<OneZero>;

// "on" / "off" common in IoT
type SwitchState = BoolSegment<OnOff>;
```

Custom encodings implement the `Encoding` trait directly.

## Legacy systems

Inheriting a messy MQTT codebase? Start by mapping existing topics as-is.

```rust
#[derive(Topic)]
#[topic(segments("legacy", "v1", device_id, "raw", kind), encoding = RawEncoding)]
pub struct LegacySensor {
    pub device_id: u64,
    pub kind: String,
    #[payload]
    pub data: Bytes,
}
```

Then migrate incrementally:

1. Map existing topics with `RawEncoding`
2. Type the payload once you understand the schema
3. Clean up the topic structure
4. Swap the backend from MQTT to NATS

Each step is independent. No big rewrites. The compiler tracks your progress.

## Roadmap

- [x] Typed topic definitions via `#[derive(Topic)]`
- [x] Compile-time segment and payload type checking
- [x] Protocol-agnostic separators and wildcards (MQTT, NATS, Redis)
- [x] `InMemoryTransport` as typed in-process pub/sub bus
- [x] Pinned subscriptions
- [x] `TopicEnum` for grouping multiple topic types
- [x] `subscribe_many`: unified subscription over multiple topic types
- [ ] `MqttTransport`: real MQTT broker support
- [ ] `NatsTransport`: NATS support
- [ ] `JsonEncoding`: serde JSON payloads
- [ ] `ProtobufEncoding`: prost protobuf payloads
- [ ] Cross-language schema export from topic definitions

## Examples

```bash
cargo run --example basic          # publish, subscribe, wildcard matching
cargo run --example typed_payload  # numeric and float payloads
cargo run --example topic_enum     # multiple topic types with subscribe_many
```

See [`topik/examples/`](topik/examples/) for the full source.

## Status

Early development `v0.1.x`. Core traits, derive macros, and in-memory transport are working and published on crates.io.

Real broker transports are in progress. API may change before `v1.0`.

Contributions and feedback welcome. Open an issue or PR on [GitHub](https://github.com/railforge/topik).

## License

MIT see [LICENSE](LICENSE).
