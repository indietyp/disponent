#[cfg(feature = "rabbitmq")]
mod rabbitmq;

use std::any::TypeId;

use error_stack::Result;
use futures::Stream;
use rmpv::Value;
use time::{Duration, OffsetDateTime};
use uuid::Uuid;

enum Event {
    Retry,
}

#[derive(serde::Serialize, serde::Deserialize)]
enum When {
    Now,
    Moment(OffsetDateTime),
    Delay(Duration),
    Every(Duration),
}

impl When {
    pub fn now() -> Self {
        Self::Now
    }

    pub fn at(time: OffsetDateTime) -> Self {
        Self::Moment(time)
    }

    pub fn delay(duration: Duration) -> Self {
        Self::Delay(duration)
    }

    pub fn every(duration: Duration) -> Self {
        Self::Every(duration)
    }
}

#[derive(serde::Serialize, serde::Deserialize)]
struct TaskRequest {
    id: Uuid,

    when: When,
    priority: u8,
    retries: u8,

    /// This is the path we're using when looking up which task to execute
    exec: String,
}

impl TaskRequest {
    pub fn new<F: Task>() -> TaskRequest {
        TaskRequest {
            id: Uuid::new_v4(),
            when: When::Now,
            priority: 0,
            retries: 0,
            exec: F::ID.to_string(),
        }
    }

    pub fn with_retries(mut self, retries: u8) -> Self {
        self.retries = retries;

        self
    }

    pub fn with_priority(mut self, priority: u8) -> Self {
        self.priority = priority;

        self
    }

    pub fn with_when(mut self, when: When) -> Self {
        self.when = when;

        self
    }

    async fn apply() {}
}

#[derive(serde::Serialize, serde::Deserialize)]
struct TaskResult {
    id: Uuid,

    value: Vec<u8>,
}

pub trait Task {
    const ID: &'static str;

    type Ok: serde::Serialize;
    type Err: std::error::Error;

    fn call<Q: Queue, F: Invoke>(&self, scheduler: &Scheduler<Q, F>)
    -> Result<Self::Ok, Self::Err>;
}

#[async_trait::async_trait]
pub trait Queue {
    type Err: std::error::Error;
    type StreamFut: Stream<Item = TaskRequest>;

    async fn create(&mut self);

    async fn schedule(&mut self, task: TaskRequest) -> Result<(), Self::Err>;

    fn consume(mut self) -> Self::StreamFut;
}

// send and receive

struct Layer<L, R> {
    left: L,
    right: R,
}

trait Invoke {
    fn call<Q: Queue, F: Invoke>(&self, id: &str, scheduler: &Scheduler<Q, F>) -> Option<Vec<u8>>;
}

struct Never;
impl Invoke for Never {
    fn call<Q: Queue, F: Invoke>(&self, _: &str, _: &Scheduler<Q, F>) -> Option<Vec<u8>> {
        None
    }
}

impl<L: Invoke, R: Invoke> Invoke for Layer<L, R> {
    fn call<Q: Queue, F: Invoke>(&self, id: &str, scheduler: &Scheduler<Q, F>) -> Option<Vec<u8>> {
        todo!()
    }
}

impl<T: Task> Invoke for T {
    fn call<Q: Queue, F: Invoke>(&self, id: &str, scheduler: &Scheduler<Q, F>) -> Option<Vec<u8>> {
        (id == T::ID)
            .then(|| Task::call(self, scheduler))
            .map(|res| res.map_err(|err| format!("{err:?}")))
            .map(|res| rmp_serde::encode::to_vec(&res).expect("This should probably not panic"))
    }
}

struct Scheduler<Q, F> {
    queue: Q,
    tasks: F,
}

impl Scheduler<(), ()> {
    pub fn new() -> Scheduler<(), Never> {
        Scheduler {
            queue: (),
            tasks: Never,
        }
    }
}

impl<F: Invoke> Scheduler<(), F> {
    pub fn with_queue<Q: Queue>(self, queue: Q) -> Scheduler<Q, F> {
        Scheduler {
            queue,
            tasks: self.tasks,
        }
    }
}

impl<Q: Queue, F: Invoke> Scheduler<Q, F> {
    pub async fn apply(&self, task: TaskRequest) {}

    pub fn execute(&self) {}

    pub fn run(self) {}
}
