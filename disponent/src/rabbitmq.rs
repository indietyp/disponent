use std::error::Error;
use std::fmt::{Display, Formatter};

use deadpool::managed::{Object, Pool};
use deadpool_lapin::Manager;
use error_stack::{Report, Result};
use lapin::ConnectionProperties;

type Connection = Object<Manager>;

#[derive(Debug)]
pub struct RabbitMqError;

impl Display for RabbitMqError {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.write_str("RabbitMQ related error")
    }
}

impl Error for RabbitMqError {}

pub struct RabbitMq {
    pool: Pool<Manager>,
}

impl RabbitMq {
    pub fn new(
        addr: &str,
        properties: ConnectionProperties,
        pool: usize,
    ) -> Result<Self, RabbitMqError> {
        let manager = Manager::new(addr, properties);
        let pool = Pool::builder(manager)
            .max_size(pool)
            .build()
            .map_err(|err| Report::new(err).change_context(RabbitMqError))?;
    }
}
