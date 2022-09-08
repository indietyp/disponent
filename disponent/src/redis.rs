use std::process::Termination;

use deadpool_redis::{Config, Connection, Pool, Runtime};
use error_stack::{IntoReport, Result, ResultExt};
use redis::AsyncCommands;
use serde::{Deserialize, Serialize};
use time::Duration;

use crate::Cache;

struct RedisError;

struct Redis {
    pool: Pool,
}

impl Redis {
    pub fn new(runtime: Option<Runtime>, config: Option<Config>) -> Result<Self, RedisError> {
        let pool = config
            .unwrap_or_default()
            .create_pool(runtime)
            .report()
            .change_context(RedisError)?;

        Ok(Self { pool })
    }

    async fn get_connection(&self) -> Result<Connection, RedisError> {
        self.pool.get().await.report().change_context(RedisError)
    }
}

#[async_trait::async_trait]
impl Cache for Redis {
    type Err = RedisError;

    async fn insert<T: Serialize>(
        &self,
        key: &str,
        value: T,
        ttl: Option<Duration>,
    ) -> Result<(), Self::Err> {
        let mut connection = self.get_connection().await?;

        let value = rmp_serde::to_vec(&value)
            .report()
            .change_context(RedisError)?;

        if let Some(ttl) = ttl.filter(|ttl| ttl.is_positive()) {
            connection
                .set_ex(key, value, ttl.whole_seconds() as usize)
                .await
                .report()
                .change_context(RedisError);
        } else {
            connection
                .set(key, value)
                .await
                .report()
                .change_context(RedisError)?;
        }

        Ok(())
    }

    async fn get<T: Deserialize>(&self, key: &str) -> Result<Option<T>, Self::Err> {
        let mut connection = self.get_connection().await?;

        let value: Option<Vec<u8>> = connection
            .get(key)
            .await
            .report()
            .change_context(RedisError)?;

        if let Some(value) = value {
            rmp_serde::from_slice(&value)
                .map(Some)
                .report()
                .change_context(RedisError)
        } else {
            Ok(None)
        }
    }
}
