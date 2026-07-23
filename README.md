# topik

Typed pub/sub topics for Rust.

Pub/sub systems like MQTT, NATS, and Redis treat topics as raw strings.
There is no compile-time guarantee that a producer and consumer agree on
what flows over a topic. Schema drift causes silent runtime bugs. The only
way to understand a running system is to subscribe to everything and parse
strings manually.

Topik brings compile-time type safety to pub/sub — define your topics once,
get guarantees everywhere.

## What it looks like

Define your topics once in a shared crate:

```rust
use topik::Topic;
use topik::encoding::F32Encoding;

#[derive(Topic)]
#[topic(segments("sensors", device_id, "temperature"), encoding = F32Encoding)]
pub struct TemperatureReading {
    pub device_id: u64,
    #[payload]
    pub data: f32,
}
```

The topic structure renders correctly for every backend:

```text
MQTT  → "sensors/{device_id}/temperature"
NATS  → "sensors.{device_id}.temperature"
Redis → "sensors:{device_id}:temperature"
```

Publish with full type safety — wrong type is a compile error:

```rust
client.publish(TemperatureReading {
    device_id: 42,
    data: 23.5,
}).await?;
```

Subscribe to all devices:

```rust
let mut sub = client.subscribe::<TemperatureReading>().await?;

while let Some(msg) = sub.next().await {
    println!("device {} sent {}°C", msg.device_id, msg.data);
}
```

Or subscribe to a specific device:

```rust
let mut sub = client
    .subscribe::<TemperatureReading>()
    .pin(|builder| builder.device_id(42))
    .await?;
```

## Designed for legacy systems

Topik does not require you to rewrite everything at once. Start by mapping
your existing topics as-is, including the messy ones:

```rust
use topik::encoding::RawEncoding;
use bytes::Bytes;

#[derive(Topic)]
#[topic(segments("legacy", "v1", device_id, "raw", kind, "data"), encoding = RawEncoding)]
pub struct LegacySensor {
    pub device_id: u64,
    pub kind: String,
    #[payload]
    pub data: Bytes,  // don't know the schema yet — that's fine
}
```

Gradually type the payload, clean up the topic structure, and when ready
swap the backend from MQTT to NATS without touching application code.

## Testing without a broker

Use `InMemoryTransport` to test your pub/sub logic without spinning up
a real broker or testcontainers:

```rust
use topik::TopikClient;
use topik::transport::InMemoryTransport;
use topik::protocol::Mqtt;

#[tokio::test]
async fn test_temperature_alert() {
    let client = TopikClient::new(InMemoryTransport::<Mqtt>::new());

    let mut sub = client.subscribe::<TemperatureReading>().await.unwrap();

    client.publish(TemperatureReading {
        device_id: 42,
        data: 95.0,
    }).await.unwrap();

    let msg = sub.next().await.unwrap();
    assert!(msg.data > 90.0, "should trigger high temp alert");
}
```

Same code as production — just swap the transport.

## Encodings

| Encoding | Type | Notes |
|----------|------|-------|
| `RawEncoding` | `Bytes` | No serialization — for unknown payloads |
| `StringEncoding` | `String` | UTF-8 strings |
| `BoolEncoding<R>` | `bool` | Configurable: `"1"`/`"0"`, `"on"`/`"off"` etc |
| `U8`..`U64Encoding` | numeric | Raw integer payloads |
| `I32`, `I64Encoding` | numeric | Signed integer payloads |
| `F32`, `F64Encoding` | float | Float payloads |

## Protocols

| Protocol | Separator | Single wildcard | Multi wildcard |
|----------|-----------|-----------------|----------------|
| MQTT | `/` | `+` | `#` |
| NATS | `.` | `*` | `>` |
| Redis | `:` | `*` | `*` |

## Transport support

| Transport | Status |
|-----------|--------|
| `InMemoryTransport` | available — use for testing |
| `MqttTransport` | coming soon |
| `NatsTransport` | coming soon |
| `RedisTransport` | coming soon |

## Examples

```bash
cargo run --example basic
cargo run --example typed_payload
```

## Status

Early development. Core traits, derive macros, and in-memory transport
are working. Real broker transports are in progress.

Not ready for production use. Contributions and feedback welcome.

## License

Licensed under the MIT license ([LICENSE](LICENSE)).
