use std::error::Error;
use std::fmt::{Display, Formatter};

use deadpool_redis::{Config, Connection, Pool, Runtime};
use error_stack::{IntoReport, Result, ResultExt};
use redis::AsyncCommands;
use serde::de::DeserializeOwned;
use serde::{Deserialize, Serialize};
use time::Duration;

use crate::Cache;

#[derive(Debug)]
pub struct RedisError;

impl Display for RedisError {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.write_str("error during redis operation")
    }
}

impl Error for RedisError {}

pub struct Redis {
    pool: Pool,
}

impl Redis {
    pub fn new(runtime: Option<Runtime>, config: Option<Config>) -> Result<Self, RedisError> {
        let pool = config
            .unwrap_or_default()
            .create_pool(runtime)
            .into_report()
            .change_context(RedisError)?;

        Ok(Self { pool })
    }

    async fn get_connection(&self) -> Result<Connection, RedisError> {
        self.pool
            .get()
            .await
            .into_report()
            .change_context(RedisError)
    }
}

#[async_trait::async_trait]
impl Cache for Redis {
    type Err = RedisError;

    async fn insert<T: Serialize + Send>(
        &self,
        key: &str,
        value: T,
        ttl: Option<Duration>,
    ) -> Result<(), Self::Err> {
        let value = rmp_serde::to_vec(&value)
            .into_report()
            .change_context(RedisError)?;

        let mut connection = self.get_connection().await?;

        if let Some(ttl) = ttl.filter(|ttl| ttl.is_positive()) {
            connection
                .set_ex(key, value, ttl.whole_seconds() as usize)
                .await
                .into_report()
                .change_context(RedisError)?;
        } else {
            connection
                .set(key, value)
                .await
                .into_report()
                .change_context(RedisError)?;
        }

        Ok(())
    }

    async fn get<T: DeserializeOwned>(&self, key: &str) -> Result<Option<T>, Self::Err> {
        let mut connection = self.get_connection().await?;

        let value: Option<Vec<u8>> = connection
            .get(key)
            .await
            .into_report()
            .change_context(RedisError)?;

        if let Some(value) = value {
            rmp_serde::from_slice(&value)
                .map(Some)
                .into_report()
                .change_context(RedisError)
        } else {
            Ok(None)
        }
    }
}
