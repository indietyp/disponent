use std::error::Error;
use std::fmt::{Display, Formatter};
use std::sync::Arc;

use deadpool_lapin::{Config, Pool, Runtime};
use error_stack::{IntoReport, Report, Result, ResultExt};
use futures::channel::oneshot;
use futures::{stream, SinkExt, StreamExt};
use lapin::message::Delivery;
use lapin::options::{
    BasicAckOptions, BasicConsumeOptions, BasicNackOptions, BasicPublishOptions,
    QueueDeclareOptions,
};
use lapin::types::{AMQPValue, FieldTable, LongString, LongUInt, ShortString};
use lapin::{BasicProperties, Channel};
use time::{Duration, OffsetDateTime};
use tracing::error;

use crate::{mpsc, Cache, Queue, QueueName, TaskRequest, TaskResult, When};

#[derive(Debug)]
pub struct RabbitMqError;

impl Display for RabbitMqError {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.write_str("RabbitMQ related error")
    }
}

impl Error for RabbitMqError {}

pub struct RabbitMq {
    pool: Pool,
    max_dead_message_ttl: Duration,
}

impl RabbitMq {
    pub fn new(runtime: Option<Runtime>, config: Option<Config>) -> Result<Self, RabbitMqError> {
        let mut pool = config
            .unwrap_or_default()
            .create_pool(runtime)
            .map_err(|err| Report::new(err).change_context(RabbitMqError))?;

        Ok(Self {
            pool,
            max_dead_message_ttl: Duration::days(7),
        })
    }

    async fn get_channel(&self) -> Result<Channel, RabbitMqError> {
        let connection = self
            .pool
            .get()
            .await
            .into_report()
            .change_context(RabbitMqError)?;

        connection
            .create_channel()
            .await
            .into_report()
            .change_context(RabbitMqError)
    }

    fn declare_arguments(name: &QueueName) -> FieldTable {
        let mut table = FieldTable::default();
        table.insert("x-dead-letter-exchange".into(), LongString::from("").into());
        table.insert(
            "x-dead-letter-routing-key".into(),
            LongString::from(name.dead_letter()).into(),
        );
        table.insert("x-max-priority".into(), LongUInt::from(u8::MAX).into());
        table
    }

    /// We use 3 different queue names:
    /// name: normal one (without delay)
    /// DQ: delay queue
    /// XQ: dead-letter queue
    async fn declare(&self, name: &QueueName) -> Result<(), RabbitMqError> {
        let channel = self.get_channel().await?;

        channel
            .queue_declare(
                &name.normal(),
                QueueDeclareOptions {
                    durable: true,
                    ..Default::default()
                },
                Self::declare_arguments(name),
            )
            .await
            .into_report()
            .change_context(RabbitMqError)?;

        channel
            .queue_declare(
                &name.delay(),
                QueueDeclareOptions {
                    durable: true,
                    ..Default::default()
                },
                Self::declare_arguments(name),
            )
            .await
            .into_report()
            .change_context(RabbitMqError)?;

        channel
            .queue_declare(
                &name.dead_letter(),
                QueueDeclareOptions {
                    durable: true,
                    ..Default::default()
                },
                {
                    let mut table = FieldTable::default();
                    table.insert(
                        "x-message-ttl".into(),
                        AMQPValue::from(self.max_dead_message_ttl.whole_milliseconds() as u64),
                    );
                    table
                },
            )
            .await
            .into_report()
            .change_context(RabbitMqError)?;

        Ok(())
    }

    async fn process_delivery<C: Cache>(
        &self,
        cache: &C,
        delivery: lapin::Result<Delivery>,
        mut sender: mpsc::Sender<(TaskRequest, oneshot::Sender<TaskResult>)>,
    ) -> Result<(), RabbitMqError> {
        let delivery = delivery.into_report().change_context(RabbitMqError)?;
        let mut request: TaskRequest = rmp_serde::from_slice(&delivery.data)
            .into_report()
            .change_context(RabbitMqError)?;

        let (tx, rx) = oneshot::channel::<TaskResult>();

        sender
            .send((request.clone(), tx))
            .await
            .into_report()
            .change_context(RabbitMqError)?;

        // TODO: timeout
        let result = rx.await.into_report().change_context(RabbitMqError);

        match result {
            Err(err) => {
                error!(?err);

                if let Some(retries) = request.retries.checked_sub(1) {
                    request.retries = retries;

                    match self.schedule(request).await {
                        Ok(_) => {
                            delivery
                                .ack(BasicAckOptions { multiple: false })
                                .await
                                .into_report()
                                .change_context(RabbitMqError)?;
                        }
                        Err(err) => {
                            error!(?err);

                            delivery
                                .nack(BasicNackOptions {
                                    multiple: false,
                                    requeue: false,
                                })
                                .await
                                .into_report()
                                .change_context(RabbitMqError)?;
                        }
                    }
                } else {
                    delivery
                        .nack(BasicNackOptions {
                            multiple: false,
                            requeue: false,
                        })
                        .await
                        .into_report()
                        .change_context(RabbitMqError)?;
                }
            }
            Ok(ok) => {
                cache
                    .insert(&ok.id.to_string(), ok, None)
                    .await
                    .change_context(RabbitMqError)?;

                delivery
                    .ack(BasicAckOptions { multiple: false })
                    .await
                    .into_report()
                    .change_context(RabbitMqError)?;
            }
        };
        // TODO: every <3

        Ok(())
    }
}

#[async_trait::async_trait]
impl Queue for RabbitMq {
    type Err = RabbitMqError;

    async fn create(&self) -> Result<(), RabbitMqError> {
        self.declare(&QueueName::default()).await
    }

    async fn schedule(&self, task: TaskRequest) -> Result<(), RabbitMqError> {
        let queue = task.queue.clone().unwrap_or_default();
        let mut props = BasicProperties::default();
        props = props.with_priority(task.priority);

        let delay = match task.when {
            When::Now => None,
            When::Moment(at) => {
                let now = at - OffsetDateTime::now_utc();

                // we skip if it is not in the future
                (!now.is_negative()).then(|| now)
            }
            When::Delay(delay) => Some(delay),
            When::Every(delay) => Some(delay),
        };
        let has_delay = delay.is_some();

        props = props.with_headers({
            let mut table = FieldTable::default();

            if let Some(delay) = delay {
                table.insert(
                    ShortString::from("x-message-ttl"),
                    AMQPValue::LongLongInt(delay.whole_milliseconds() as i64),
                );
            }

            table
        });

        let channel = self.get_channel().await?;

        let confirm = channel
            .basic_publish(
                "",
                &if has_delay {
                    queue.normal()
                } else {
                    queue.delay()
                },
                BasicPublishOptions::default(),
                &rmp_serde::to_vec(&task).unwrap(),
                props,
            )
            .await
            .into_report()
            .change_context(RabbitMqError)?;

        let confirmation = confirm.await.into_report().change_context(RabbitMqError)?;
        if confirmation.is_ack() {
            // TODO: confirmation, get the requested value back!
        }

        Ok(())
    }

    async fn consume<C: Cache>(
        &self,
        cache: &C,
        requests: mpsc::Sender<(TaskRequest, oneshot::Sender<TaskResult>)>,
    ) -> Result<(), Self::Err> {
        let name = QueueName::default();
        let channel = self.get_channel().await?;

        let immediate = channel
            .basic_consume(
                &name.normal(),
                "my_consumer",
                BasicConsumeOptions::default(),
                FieldTable::default(),
            )
            .await
            .into_report()
            .change_context(RabbitMqError)?;

        let dead_letter = channel
            .basic_consume(
                &name.dead_letter(),
                "my_consumer",
                BasicConsumeOptions::default(),
                FieldTable::default(),
            )
            .await
            .into_report()
            .change_context(RabbitMqError)?;

        stream::select_all([immediate, dead_letter])
            .for_each_concurrent(16, {
                let requests = Arc::new(requests);

                move |deliver| {
                    let requests = Arc::clone(&requests);

                    async move {
                        if let Err(err) = self
                            .process_delivery(cache, deliver, requests.as_ref().clone())
                            .await
                        {
                            error!(?err);
                        }
                    }
                }
            })
            .await;

        Ok(())
    }
}
