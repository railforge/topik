//! Wildcard subscriptions: controlling subscription scope with pinned segments.
//!
//! Demonstrates how to subscribe to a strict topic, progressively wider
//! wildcard patterns, and a multi-level catch-all using a topic with
//! two dynamic segments.
//!
//! Requires a running MQTT broker on localhost:1883.
//!
//! Start one with Docker:
//!   docker run -it -p 1883:1883 eclipse-mosquitto
//!
//! Run with:
//!   cargo run --example wildcards --features rumqttc

#[cfg(feature = "rumqttc")]
mod example {
    use topik::prelude::*;

    /// Topic: `sensors/{factory_id}/{device_id}/temperature`
    #[derive(Topic, Debug)]
    #[topic(segments("sensors", factory_id, device_id, "temperature"))]
    pub struct TemperatureReading {
        pub factory_id: u64,
        pub device_id: u64,
        #[payload]
        pub data: f32,
    }

    pub async fn run() {
        let sub_transport = MqttTransport::builder()
            .url("localhost", 1883)
            .client_id("topik-wildcards-sub")
            .build()
            .await
            .unwrap();

        let pub_transport = MqttTransport::builder()
            .url("localhost", 1883)
            .client_id("topik-wildcards-pub")
            .build()
            .await
            .unwrap();

        let sub_client = TopikClient::new(sub_transport);
        let pub_client = TopikClient::new(pub_transport);

        // Publish readings across two factories and two devices each.
        let readings = [
            TemperatureReading {
                factory_id: 1,
                device_id: 10,
                data: 21.0,
            },
            TemperatureReading {
                factory_id: 1,
                device_id: 20,
                data: 22.5,
            },
            TemperatureReading {
                factory_id: 2,
                device_id: 10,
                data: 19.0,
            },
            TemperatureReading {
                factory_id: 2,
                device_id: 20,
                data: 23.1,
            },
        ];
        for r in &readings {
            pub_client
                .publish(TemperatureReading { ..*r })
                .await
                .unwrap();
        }

        // Exact: only factory 1, device 10 -> Pattern: sensors/1/10/temperature
        println!("Exact sensors/1/10/temperature");
        let mut sub = sub_client
            .subscribe::<TemperatureReading>()
            .pin(|b| b.factory_id(1).device_id(10))
            .await
            .unwrap();
        let msg = sub.next().await.unwrap();
        println!(
            "   factory {} device {} -> {:.1}°C",
            msg.factory_id, msg.device_id, msg.data
        );
        sub.unsubscribe().await.unwrap();

        // All devices in factory 1 -> Pattern: sensors/1/+/temperature
        println!("\nAll devices in factory 1: sensors/1/+/temperature");
        let mut sub = sub_client
            .subscribe::<TemperatureReading>()
            .pin(|b| b.factory_id(1))
            .await
            .unwrap();
        for _ in 0..2 {
            let msg = sub.next().await.unwrap();
            println!(
                "   factory {} device {} -> {:.1}°C",
                msg.factory_id, msg.device_id, msg.data
            );
        }
        sub.unsubscribe().await.unwrap();

        // All factories, all devices -> Pattern: sensors/+/+/temperature
        println!(
            "\nAll factories, all devices: {}",
            sub_client.pattern::<TemperatureReading>()
        );
        let mut sub = sub_client.subscribe::<TemperatureReading>().await.unwrap();
        for _ in 0..4 {
            let msg = sub.next().await.unwrap();
            println!(
                "   factory {} device {} -> {:.1}°C",
                msg.factory_id, msg.device_id, msg.data
            );
        }
        sub.unsubscribe().await.unwrap();
    }
}

#[tokio::main]
async fn main() {
    #[cfg(feature = "rumqttc")]
    example::run().await;

    #[cfg(not(feature = "rumqttc"))]
    println!("Run with --features rumqttc to enable this example.");
}
