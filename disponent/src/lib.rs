#[cfg(feature = "rabbitmq")]
mod rabbitmq;

use error_stack::Result;
use futures::Stream;
use time::{Duration, OffsetDateTime};

#[async_trait::async_trait]
pub trait Task {
    type Ok;
    type Err;

    fn delay(&self) -> Option<Duration>;
    fn at(&self) -> Option<OffsetDateTime>;
    fn every(&self) -> Option<Duration>;

    fn priority(&self) -> Option<u8>;

    async fn exec(self) -> Result<Self::Ok, Self::Err>;
}

pub trait Queue {
    type StreamFut: Stream<Item = Box<dyn Task>>;

    fn enqueue(&mut self, task: impl Task);

    fn stream(mut self) -> Self::StreamFut;
}

// send and receive

struct Scheduler {}

struct Sender {}
