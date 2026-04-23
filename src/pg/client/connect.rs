//! Connection helpers for `PgClient`.

use super::{CapabilityCache, MIN_SUPPORTED_SERVER_VERSION_NUM, PgClient};
use crate::pg::conninfo::describe_connection_target;
use anyhow::{Context, Result};
use postgres::{Config, NoTls};
use std::time::Duration;

impl PgClient {
    pub fn new(dsn: &str, connect_timeout_ms: u64) -> Result<Self> {
        let config = config_from_dsn(dsn, connect_timeout_ms)?;
        Self::connect(&config, dsn)
    }

    pub fn for_database(dsn: &str, connect_timeout_ms: u64, database: &str) -> Result<Self> {
        let mut config = config_from_dsn(dsn, connect_timeout_ms)?;
        config.dbname(database);
        Self::connect(&config, dsn)
    }

    pub fn execute_admin_action(&mut self, query: &str) -> Result<()> {
        self.client.simple_query(query)?;
        Ok(())
    }

    pub(crate) const fn server_version_num(&self) -> i32 {
        self.server_version_num
    }

    fn connect(config: &Config, dsn: &str) -> Result<Self> {
        let mut client = config.connect(NoTls).with_context(|| {
            format!(
                "Failed to connect to Postgres using {}",
                describe_connection_target(dsn)
            )
        })?;
        let version_row = client.query_one(
            "SELECT current_setting('server_version_num')::int, current_setting('server_version')",
            &[],
        )?;
        let server_version_num = version_row.try_get::<_, i32>(0)?;
        let server_version = version_row.try_get::<_, String>(1)?;
        if server_version_num < MIN_SUPPORTED_SERVER_VERSION_NUM {
            return Err(anyhow::anyhow!(
                "pgmon requires PostgreSQL 14 or newer; connected server is PostgreSQL {server_version}."
            ));
        }
        Ok(Self {
            client,
            capability_cache: CapabilityCache::default(),
            server_version_num,
        })
    }
}

pub(super) fn config_from_dsn(dsn: &str, connect_timeout_ms: u64) -> Result<Config> {
    let mut config: Config = dsn.parse().with_context(|| {
        format!(
            "Failed to parse Postgres connection settings for {}",
            describe_connection_target(dsn)
        )
    })?;
    config.connect_timeout(Duration::from_millis(connect_timeout_ms));
    Ok(config)
}
