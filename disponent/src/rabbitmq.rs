use std::any::TypeId;
use std::error::Error;
use std::fmt::{Display, Formatter};

use deadpool::managed::PoolConfig;
use deadpool_lapin::{Config, Manager, Pool, Runtime};
use error_stack::{Report, Result};
use lapin::options::BasicPublishOptions;
use lapin::types::{AMQPValue, FieldTable, LongString, ShortString};
use lapin::{BasicProperties, ConnectionProperties};
use time::OffsetDateTime;

use crate::{Queue, TaskRequest};

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
}

impl RabbitMq {
    pub fn new(runtime: Option<Runtime>, config: Option<Config>) -> Result<Self, RabbitMqError> {
        let mut pool = config
            .unwrap_or_default()
            .create_pool(runtime)
            .map_err(|err| Report::new(err).change_context(RabbitMqError))?;

        Ok(Self { pool })
    }
}

pub struct Message<T: TaskRequest> {
    id: TypeId,
    task: T,
}

#[async_trait::async_trait]
impl Queue for RabbitMq {
    type Err = RabbitMqError;
    type StreamFut = ();

    async fn schedule<T: TaskRequest>(&self, task: T) -> Result<(), RabbitMqError> {
        let mut connection = self
            .pool
            .get()
            .await
            .map_err(|err| Report::new(err).change_context(RabbitMqError))?;

        let mut props = BasicProperties::default();
        if let Some(prio) = task.priority() {
            props = props.with_priority(prio);
        }

        props = props.with_headers({
            let mut table = FieldTable::default();

            table.insert(
                ShortString::from("x-dead-letter-exchange"),
                AMQPValue::LongString(LongString::from("")),
            );
            table.insert(
                ShortString::from("x-dead-letter-routing-key"),
                AMQPValue::LongString(LongString::from("DQ")),
            );
            table.insert(
                ShortString::from("x-message-ttl"),
                AMQPValue::LongLongInt({
                    let now = OffsetDateTime::now_utc();
                    let mut future = task.at().unwrap_or_else(|| OffsetDateTime::now_utc());
                    future += task.delay().unwrap_or_default();

                    (now - future).whole_milliseconds() as i64
                }),
            );

            table
        });

        let channel = connection.create_channel().await?;

        let payload = rmp_serde::to_vec(&Message {
            id: TypeId::of::<T>(),
            task,
        })
        .map_err(|err| Report::new(err).change_context(RabbitMqError))?;

        channel
            .basic_publish("", "TQ", BasicPublishOptions::default(), &payload, props)
            .await
            .map_err(|err| Report::new(err).change_context(RabbitMqError))?
            .await
            .map_err(|err| Report::new(err).change_context(RabbitMqError))?;

        Ok(())
    }

    fn consume(self) -> Self::StreamFut {
        todo!()
    }
}
