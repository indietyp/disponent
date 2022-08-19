#[cfg(feature = "rabbitmq")]
mod rabbitmq;

use error_stack::Result;
use futures::Stream;
use time::{Duration, OffsetDateTime};

#[async_trait::async_trait]
pub trait Task: erased_serde::Serialize + 'static {
    type Ok;
    type Err;

    fn delay(&self) -> Option<Duration>;
    fn at(&self) -> Option<OffsetDateTime>;
    fn every(&self) -> Option<Duration>;

    fn priority(&self) -> Option<u8>;

    async fn exec(self) -> Result<Self::Ok, Self::Err>;
}

#[async_trait::async_trait]
pub trait Queue {
    type Err;
    type StreamFut: Stream<Item = Box<dyn Task>>;

    async fn enqueue<T: Task>(&mut self, task: T) -> Result<(), Self::Err>;

    fn stream(mut self) -> Self::StreamFut;
}

// send and receive

struct Scheduler {}

struct Sender {}
