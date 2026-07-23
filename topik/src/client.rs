use std::future::{Future, IntoFuture};
use std::marker::PhantomData;
use std::pin::Pin;
use topik_core::__private::{SubscribeBuilder, TopicWire, Transport};
use topik_core::protocol::Protocol;
use topik_core::{Encoding, TopikError};

use crate::subscriber::Subscriber;

pub struct TopikClient<T: Transport> {
    pub(crate) transport: T,
}

impl<T: Transport> TopikClient<T> {
    pub fn new(transport: T) -> Self {
        TopikClient { transport }
    }

    pub async fn publish<M: TopicWire>(&self, topic: M) -> Result<(), TopikError> {
        let topic_str = topic.render(T::Protocol::SEPARATOR);
        let payload = M::Encoding::encode(topic.payload())?;
        self.transport.publish(topic_str, payload).await
    }

    /// Subscribe to messages matching this topic type.
    ///
    /// # Example
    ///
    /// ```rust
    /// // all devices
    /// let mut sub = client.subscribe::<TemperatureReading>().await?;
    ///
    /// // only device 42
    /// let mut sub = client.subscribe::<TemperatureReading>()
    ///     .pin(|builder| builder.device_id(42))
    ///     .await?;
    ///
    /// // multiple segments pinned
    /// let mut sub = client.subscribe::<FactoryReading>()
    ///     .pin(|builder| builder.device_id(42).kind("temperature".to_string()))
    ///     .await?;
    /// ```
    pub fn subscribe<M: TopicWire>(&self) -> TopikSubscribeBuilder<'_, T, M>
    where
        T: Clone,
    {
        TopikSubscribeBuilder {
            client: self,
            inner: M::subscribe_builder(),
        }
    }

    pub fn display<M: TopicWire>(&self, topic: &M) -> String {
        topic.render(T::Protocol::SEPARATOR)
    }
}

impl<T: Transport + Clone> Clone for TopikClient<T> {
    fn clone(&self) -> Self {
        TopikClient {
            transport: self.transport.clone(),
        }
    }
}

/// A builder for typed subscriptions.
pub struct TopikSubscribeBuilder<'a, T: Transport + Clone, M: TopicWire> {
    client: &'a TopikClient<T>,
    inner: M::SubscribeBuilder,
}

impl<'a, T: Transport + Clone, M: TopicWire> TopikSubscribeBuilder<'a, T, M> {
    /// Pin specific segment values before subscribing.
    ///
    /// Unpinned segments become wildcards in the subscription pattern.
    pub fn pin<F>(mut self, f: F) -> Self
    where
        F: FnOnce(M::SubscribeBuilder) -> M::SubscribeBuilder,
    {
        self.inner = f(self.inner);
        self
    }
}

impl<'a, T: Transport + Clone, M: TopicWire> IntoFuture for TopikSubscribeBuilder<'a, T, M> {
    type Output = Result<Subscriber<T, M>, TopikError>;
    type IntoFuture = Pin<Box<dyn Future<Output = Self::Output> + Send + 'a>>;

    fn into_future(self) -> Self::IntoFuture {
        Box::pin(async move {
            let pattern = self
                .inner
                .build_pattern(T::Protocol::SEPARATOR, T::Protocol::SINGLE_WILDCARD);
            let stream = self.client.transport.subscribe(pattern.clone()).await?;
            Ok(Subscriber {
                stream,
                pattern,
                transport: self.client.transport.clone(),
                _topic: PhantomData,
            })
        })
    }
}
