#![feature(result_flattening)]

#[cfg(feature = "rabbitmq")]
mod rabbitmq;
#[cfg(feature = "redis")]
mod redis;

use std::any::TypeId;
use std::sync::Arc;

use error_stack::Result;
use futures::channel::{mpsc, oneshot};
use futures::{Stream, StreamExt};
use rmpv::Value;
use time::{Duration, OffsetDateTime};
use tracing::error;
use uuid::Uuid;

struct QueueName(String);

impl QueueName {
    fn normal(&self) -> String {
        self.0.clone()
    }

    fn delay(&self) -> String {
        let mut name = self.0.clone();
        name.push_str(".DQ");
        name
    }

    fn dead_letter(&self) -> String {
        let mut name = self.0.clone();
        name.push_str(".XQ");
        name
    }
}

impl Default for QueueName {
    fn default() -> Self {
        Self("default".to_owned())
    }
}

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

#[derive(serde::Serialize, serde::Deserialize, Debug, Clone)]
struct TaskRequest {
    id: Uuid,

    when: When,
    priority: u8,
    retries: u8,

    /// This is the path we're using when looking up which task to execute
    exec: String,

    // for now this isn't exposed
    queue: Option<QueueName>,
}

impl TaskRequest {
    pub fn new<F: Task>() -> TaskRequest {
        TaskRequest {
            id: Uuid::new_v4(),
            when: When::Now,
            priority: 0,
            retries: 0,
            exec: F::ID.to_string(),
            queue: None,
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

struct ExecutionError;

#[derive(serde::Serialize, serde::Deserialize)]
struct TaskResult {
    id: Uuid,

    value: Vec<u8>,
}

#[async_trait::async_trait]
pub trait Task {
    const ID: &'static str;

    type Ok: serde::Serialize;

    async fn call<Q: Queue, C: Cache, F: Invoke>(&self, scheduler: &Scheduler<Q, C, F>)
    -> Self::Ok;
}

#[async_trait::async_trait]
pub trait Cache {
    type Err: std::error::Error;

    async fn insert<T: serde::Serialize>(
        &self,
        key: &str,
        value: T,
        ttl: Option<Duration>,
    ) -> Result<(), Self::Err>;
    async fn get<T: serde::de::Deserialize>(&self, key: &str) -> Result<Option<T>, Self::Err>;
}

#[async_trait::async_trait]
pub trait Queue {
    type Err: std::error::Error;

    async fn create(&mut self) -> Result<(), Self::Err>;

    async fn schedule(&mut self, task: TaskRequest) -> Result<(), Self::Err>;

    fn consume<C: Cache>(
        mut self,
        cache: &C,
        requests: mpsc::Sender<(TaskRequest, oneshot::Sender<TaskResult>)>,
    ) -> Result<(), Self::Err>;
}

// send and receive

struct Layer<L, R> {
    left: L,
    right: R,
}

#[async_trait::async_trait]
trait Invoke {
    async fn call<Q: Queue, C: Cache, F: Invoke>(
        &self,
        id: &str,
        scheduler: &Scheduler<Q, C, F>,
    ) -> Option<Vec<u8>>;
}

struct Never;

#[async_trait::async_trait]
impl Invoke for Never {
    async fn call<Q: Queue, C: Cache, F: Invoke>(
        &self,
        _: &str,
        _: &Scheduler<Q, C, F>,
    ) -> Option<Vec<u8>> {
        None
    }
}

#[async_trait::async_trait]
impl<L: Invoke, R: Invoke> Invoke for Layer<L, R> {
    async fn call<Q: Queue, C: Cache, F: Invoke>(
        &self,
        id: &str,
        scheduler: &Scheduler<Q, C, F>,
    ) -> Option<Vec<u8>> {
        todo!()
    }
}

#[async_trait::async_trait]
impl<T: Task> Invoke for T {
    async fn call<Q: Queue, C: Cache, F: Invoke>(
        &self,
        id: &str,
        scheduler: &Scheduler<Q, C, F>,
    ) -> Option<Vec<u8>> {
        (id == T::ID)
            .then(|| Task::call(self, scheduler))
            .map(|res| rmp_serde::encode::to_vec(&res).expect("This should probably not panic"))
    }
}

struct Scheduler<Q, C, F> {
    queue: Q,
    cache: C,
    tasks: F,
}

impl Scheduler<(), (), ()> {
    pub fn new() -> Scheduler<(), (), Never> {
        Scheduler {
            queue: (),
            cache: (),
            tasks: Never,
        }
    }
}

impl<F: Invoke, C> Scheduler<(), C, F> {
    pub fn with_queue<Q: Queue>(self, queue: Q) -> Scheduler<Q, C, F> {
        Scheduler {
            queue,
            cache: self.cache,
            tasks: self.tasks,
        }
    }
}

impl<F: Invoke, Q> Scheduler<Q, (), F> {
    pub fn with_cache<C: Cache>(self, cache: C) -> Scheduler<Q, C, F> {
        Scheduler {
            queue: self.queue,
            cache,
            tasks: self.tasks,
        }
    }
}

struct SchedulerError;

impl<Q: Queue, C: Cache, F: Invoke> Scheduler<Q, C, F> {
    pub async fn apply(&self, task: TaskRequest) {}

    pub fn execute(&self) {}

    pub async fn run(self) -> Result<(), SchedulerError> {
        let (tx, mut rx) = mpsc::channel(32);

        #[cfg(feature = "tokio")]
        {
            tokio::spawn(async move {
                if let Err(err) = self
                    .queue
                    .consume(&self.cache, tx)
                    .await
                    .change_context(SchedulerError)
                {
                    error!(err);
                };
            });
        }

        while let Some((req, tx)) = rx.next().await {
            #[cfg(feature = "tokio")]
            {
                tokio::spawn(async move {
                    let result = self.tasks.call(&req.exec, &self).await;
                    match result {
                        None => error!("Could not find task for specific exec name"),
                        Some(value) => {
                            if let Err(_) = tx.send(TaskResult { id: req.id, value }) {
                                error!("oneshot return was closed");
                            }
                        }
                    }
                })
            }
        }

        Ok(())
    }
}
