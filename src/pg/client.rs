mod activity;
mod connect;
mod explain;
mod metadata;
mod replication;
mod statements;
mod types;

use postgres::Client;

pub(crate) use self::explain::analyze_explain_query;
pub(crate) use self::types::{
    ActivityDetail, ActivityProcessSnapshot, ActivitySession, ActivitySnapshot,
    ActivitySummarySnapshot, AutoExplainInfo, CapabilityStatus, ExplainMode, ReplicationSender,
    ReplicationSlot, ReplicationSnapshot,
};

pub(crate) const MIN_SUPPORTED_SERVER_VERSION_NUM: i32 = 140_000;

pub struct PgClient {
    client: Client,
    capability_cache: CapabilityCache,
    server_version_num: i32,
}

#[derive(Debug, Default)]
struct CapabilityCache {
    io: Option<CapabilityStatus>,
    statements: Option<CapabilityStatus>,
    replication: CapabilityStatus,
    statements_query: Option<String>,
}

impl std::fmt::Debug for PgClient {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("PgClient").finish()
    }
}
