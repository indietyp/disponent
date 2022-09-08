#[cfg(feature = "rabbitmq")]
mod rabbitmq;

use std::any::TypeId;

use error_stack::Result;
use futures::Stream;
use time::{Duration, OffsetDateTime};
use uuid::Uuid;

enum Event {
    Retry,
}

#[derive(serde::Serialize, serde::Deserialize)]
enum When {
    Moment(OffsetDateTime),
    Delay(Duration),
    Every(Duration),
}

#[derive(serde::Serialize, serde::Deserialize)]
struct Task {
    id: Uuid,

    when: When,
    priority: u8,
    retries: u8,

    /// This is the path we're using when looking up which task to execute
    exec: String,
}

#[derive(serde::Serialize, serde::Deserialize)]
struct TaskResult {
    id: Uuid,

    value: Vec<u8>,
}

pub trait Function {
    const ID: &'static str;

    type Ok: serde::Serialize;
    type Err: std::error::Error;

    fn call(&self) -> Result<Self::Ok, Self::Err>;
}

#[async_trait::async_trait]
pub trait Queue {
    type Err: std::error::Error;
    type StreamFut: Stream<Item = Task>;

    async fn create(&mut self);

    async fn schedule(&mut self, task: Task) -> Result<(), Self::Err>;

    fn consume(mut self) -> Self::StreamFut;
}

// send and receive

struct Layer<L, R> {
    left: L,
    right: R,
}

trait Invoke {
    fn call(&self, id: &str) -> Option<()>;
}

struct Never;
impl Invoke for Never {
    fn call(&self, _: &str) -> Option<()> {
        None
    }
}

impl<T: Function> Invoke for T {
    fn call(&self, id: &str) -> Option<()> {
        todo!()
    }
}

impl<L: Invoke, R: Invoke> Invoke for Layer<L, R> {
    fn call(&self, id: &str) -> Option<()> {
        todo!()
    }
}

struct Scheduler<Q, F> {
    queue: Q,

    functions: F,
}
