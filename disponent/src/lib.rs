#![feature(result_flattening)]

#[cfg(feature = "rabbitmq")]
mod rabbitmq;
#[cfg(feature = "redis")]
mod redis;

use std::error::Error;
use std::fmt::{Display, Formatter};
use std::sync::Arc;

use error_stack::{Result, ResultExt};
use futures::channel::{mpsc, oneshot};
use futures::StreamExt;
use time::{Duration, OffsetDateTime};
use tracing::{error, info};
use uuid::Uuid;

#[derive(serde::Serialize, serde::Deserialize, Debug, Clone)]
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

#[derive(serde::Serialize, serde::Deserialize, Debug, Clone, Copy)]
pub enum When {
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
pub struct TaskRequest {
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
pub struct TaskResult {
    id: Uuid,

    value: Vec<u8>,
}

#[async_trait::async_trait]
pub trait Task: Send + Sync + 'static {
    const ID: &'static str;

    type Ok: serde::Serialize;

    async fn call<Q: Queue, C: Cache, F: Service>(
        &self,
        scheduler: &Scheduler<Q, C, F>,
    ) -> Self::Ok;
}

#[async_trait::async_trait]
pub trait Cache: Send + Sync + 'static {
    type Err: Error;

    async fn insert<T: serde::Serialize + Send>(
        &self,
        key: &str,
        value: T,
        ttl: Option<Duration>,
    ) -> Result<(), Self::Err>;
    async fn get<T: serde::de::DeserializeOwned>(&self, key: &str) -> Result<Option<T>, Self::Err>;
}

#[async_trait::async_trait]
pub trait Queue: Send + Sync + 'static {
    type Err: Error;

    async fn create(&self) -> Result<(), Self::Err>;

    async fn schedule(&self, task: TaskRequest) -> Result<(), Self::Err>;

    async fn consume<C: Cache>(
        &self,
        cache: &C,
        requests: mpsc::Sender<(TaskRequest, oneshot::Sender<TaskResult>)>,
    ) -> Result<(), Self::Err>;
}

// send and receive

pub struct Layer<L, R> {
    left: L,
    right: R,
}

#[async_trait::async_trait]
pub trait Service: Send + Sync + 'static {
    async fn call<Q: Queue, C: Cache, F: Service>(
        &self,
        id: &str,
        scheduler: &Scheduler<Q, C, F>,
    ) -> Option<Vec<u8>>;
}

pub struct Never;

#[async_trait::async_trait]
impl Service for Never {
    async fn call<Q: Queue, C: Cache, F: Service>(
        &self,
        _: &str,
        _: &Scheduler<Q, C, F>,
    ) -> Option<Vec<u8>> {
        None
    }
}

#[async_trait::async_trait]
impl<L: Service, R: Service> Service for Layer<L, R> {
    async fn call<Q: Queue, C: Cache, F: Service>(
        &self,
        id: &str,
        scheduler: &Scheduler<Q, C, F>,
    ) -> Option<Vec<u8>> {
        if let Some(result) = self.left.call(id, scheduler).await {
            Some(result)
        } else {
            self.right.call(id, scheduler).await
        }
    }
}

#[async_trait::async_trait]
impl<T: Task> Service for T {
    async fn call<Q: Queue, C: Cache, F: Service>(
        &self,
        id: &str,
        scheduler: &Scheduler<Q, C, F>,
    ) -> Option<Vec<u8>> {
        if id == T::ID {
            let out = Task::call(self, scheduler).await;

            let out = rmp_serde::encode::to_vec(&out).expect("This should probably not panic");
            Some(out)
        } else {
            None
        }
    }
}

pub struct Scheduler<Q, C, F> {
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

impl<F: Service, C> Scheduler<(), C, F> {
    pub fn with_queue<Q: Queue>(self, queue: Q) -> Scheduler<Q, C, F> {
        Scheduler {
            queue,
            cache: self.cache,
            tasks: self.tasks,
        }
    }
}

impl<F: Service, Q> Scheduler<Q, (), F> {
    pub fn with_cache<C: Cache>(self, cache: C) -> Scheduler<Q, C, F> {
        Scheduler {
            queue: self.queue,
            cache,
            tasks: self.tasks,
        }
    }
}

#[derive(Debug)]
struct SchedulerError;

impl Display for SchedulerError {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.write_str("error while scheduling data")
    }
}

impl Error for SchedulerError {}

impl<Q: Queue, C: Cache, F: Service> Scheduler<Q, C, F> {
    pub fn attach_task<T>(self, task: T) -> Scheduler<Q, C, Layer<T, F>> {
        Scheduler {
            queue: self.queue,
            tasks: Layer {
                left: task,
                right: self.tasks,
            },
            cache: self.cache,
        }
    }

    pub async fn apply(&self, task: TaskRequest) -> Result<(), SchedulerError> {
        // we definitely need to change the return type here _somehow_
        self.queue
            .schedule(task)
            .await
            .change_context(SchedulerError)?;

        println!("Ok!");
        Ok(())
    }

    pub fn execute(&self) {}

    pub async fn run(self: Arc<Self>) -> Result<(), SchedulerError> {
        self.queue.create().await.change_context(SchedulerError)?;

        info!("Starting up!");

        let (tx, mut rx) = mpsc::channel::<(TaskRequest, oneshot::Sender<TaskResult>)>(32);

        #[cfg(feature = "tokio")]
        {
            let this = Arc::clone(&self);

            tokio::spawn(async move {
                if let Err(err) = this
                    .queue
                    .consume(&this.cache, tx)
                    .await
                    .change_context(SchedulerError)
                {
                    error!(?err);
                };
            });
        }

        while let Some((req, tx)) = rx.next().await {
            #[cfg(feature = "tokio")]
            {
                let this = Arc::clone(&self);

                tokio::spawn(async move {
                    let result = this.tasks.call(&req.exec, &this).await;
                    match result {
                        None => error!("Could not find task for specific exec name"),
                        Some(value) => {
                            if let Err(_) = tx.send(TaskResult { id: req.id, value }) {
                                error!("oneshot return was closed");
                            }
                        }
                    }
                });
            }
        }

        Ok(())
    }
}

#[cfg(test)]
mod test {
    use std::sync::Arc;

    use deadpool::Runtime;
    use lapin::ConnectionProperties;

    use crate::rabbitmq::RabbitMq;
    use crate::redis::Redis;
    use crate::{Cache, Queue, Scheduler, Service, Task, TaskRequest};

    struct Example;

    #[async_trait::async_trait]
    impl Task for Example {
        type Ok = ();

        const ID: &'static str = "example";

        async fn call<Q: Queue, C: Cache, F: Service>(&self, scheduler: &Scheduler<Q, C, F>) {
            tracing::info!("How are you?!")
        }
    }

    fn configure() -> Arc<Scheduler<impl Queue, impl Cache, impl Service>> {
        let scheduler = Scheduler::new()
            .with_cache(
                Redis::new(
                    Some(Runtime::Tokio1),
                    Some(deadpool_redis::Config {
                        url: Some("redis://localhost/2".to_owned()),
                        connection: None,
                        pool: None,
                    }),
                )
                .expect("should not fail"),
            )
            .with_queue(
                RabbitMq::new(
                    Some(Runtime::Tokio1),
                    Some(deadpool_lapin::Config {
                        url: Some("amqp://localhost/%2f".to_owned()),
                        pool: None,
                        connection_properties: ConnectionProperties::default(),
                    }),
                )
                .expect("should not fail"),
            )
            .attach_task(Example);

        Arc::new(scheduler)
    }

    #[tokio::test]
    async fn run_worker() {
        tracing_subscriber::fmt::init();

        let scheduler = configure();
        println!("{:?}", scheduler.run().await);

        assert!(false)
    }

    #[tokio::test]
    async fn request() {
        tracing_subscriber::fmt::init();

        let scheduler = configure();
        let result = scheduler.apply(TaskRequest::new::<Example>()).await;
        println!("{:?}", result);

        assert!(true);
    }
}
