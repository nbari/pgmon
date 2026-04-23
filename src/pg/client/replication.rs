//! Replication fetchers and helpers for `PgClient`.

use super::{
    CapabilityStatus, DbResult, PgClient, PgClientConnection, ReplicationSender, ReplicationSlot,
    ReplicationSnapshot,
};
use crate::pg::queries::{
    REPLICATION_RECEIVER_QUERY, REPLICATION_SENDERS_QUERY, REPLICATION_SLOTS_QUERY,
};
use sqlx::Row;

impl PgClient {
    pub(crate) async fn fetch_replication_snapshot(
        &self,
        connection: &mut PgClientConnection,
    ) -> DbResult<ReplicationSnapshot> {
        let receiver_summary = if let Some(row) = sqlx::query(REPLICATION_RECEIVER_QUERY)
            .fetch_optional(connection.as_mut())
            .await
            .map_err(super::connect::classify_query_error)?
        {
            let status = row
                .try_get::<String, _>(0)
                .map_err(super::connect::classify_query_error)?;
            let sender_host = row
                .try_get::<String, _>(1)
                .map_err(super::connect::classify_query_error)?;
            let sender_port = row
                .try_get::<String, _>(2)
                .map_err(super::connect::classify_query_error)?;
            let slot_name = row
                .try_get::<String, _>(3)
                .map_err(super::connect::classify_query_error)?;
            let latest_end_lsn = row
                .try_get::<String, _>(4)
                .map_err(super::connect::classify_query_error)?;
            Some(format_receiver_summary(
                &status,
                &sender_host,
                &sender_port,
                &slot_name,
                &latest_end_lsn,
            ))
        } else {
            None
        };

        let senders = sqlx::query(REPLICATION_SENDERS_QUERY)
            .fetch_all(connection.as_mut())
            .await
            .map_err(super::connect::classify_query_error)?
            .into_iter()
            .map(|row| {
                Ok(ReplicationSender {
                    pid: row.try_get::<String, _>(0)?,
                    user: row.try_get::<String, _>(1)?,
                    application: row.try_get::<String, _>(2)?,
                    client: row.try_get::<String, _>(3)?,
                    state: row.try_get::<String, _>(4)?,
                    sync_state: row.try_get::<String, _>(5)?,
                    slot_name: row.try_get::<String, _>(6)?,
                    sent_lag_bytes: row.try_get::<i64, _>(7)?,
                    write_lag_bytes: row.try_get::<i64, _>(8)?,
                    flush_lag_bytes: row.try_get::<i64, _>(9)?,
                    replay_lag_bytes: row.try_get::<i64, _>(10)?,
                })
            })
            .collect::<Result<Vec<_>, sqlx::Error>>()
            .map_err(super::connect::classify_query_error)?;

        let slots = sqlx::query(REPLICATION_SLOTS_QUERY)
            .fetch_all(connection.as_mut())
            .await
            .map_err(super::connect::classify_query_error)?
            .into_iter()
            .map(|row| {
                Ok(ReplicationSlot {
                    slot_name: row.try_get::<String, _>(0)?,
                    slot_type: row.try_get::<String, _>(1)?,
                    active: row.try_get::<String, _>(2)?,
                    active_pid: row.try_get::<String, _>(3)?,
                    restart_lsn: row.try_get::<String, _>(4)?,
                    confirmed_flush_lsn: row.try_get::<String, _>(5)?,
                    wal_status: row.try_get::<String, _>(6)?,
                })
            })
            .collect::<Result<Vec<_>, sqlx::Error>>()
            .map_err(super::connect::classify_query_error)?;

        let capability =
            detect_replication_capability(connection, receiver_summary.as_ref(), &senders, &slots)
                .await?;
        self.state().capability_cache.replication = capability.clone();

        Ok(ReplicationSnapshot {
            capability,
            receiver_summary,
            senders,
            slots,
        })
    }
}

fn format_receiver_summary(
    status: &str,
    sender_host: &str,
    sender_port: &str,
    slot_name: &str,
    latest_end_lsn: &str,
) -> String {
    let mut parts = vec![format!("Receiver: {status}")];

    if !sender_host.is_empty() {
        if sender_port.is_empty() {
            parts.push(format!("source={sender_host}"));
        } else {
            parts.push(format!("source={sender_host}:{sender_port}"));
        }
    }
    if !slot_name.is_empty() {
        parts.push(format!("slot={slot_name}"));
    }
    if !latest_end_lsn.is_empty() {
        parts.push(format!("latest_end_lsn={latest_end_lsn}"));
    }

    parts.join(" | ")
}

async fn detect_replication_capability(
    connection: &mut PgClientConnection,
    receiver_summary: Option<&String>,
    senders: &[ReplicationSender],
    slots: &[ReplicationSlot],
) -> DbResult<CapabilityStatus> {
    if receiver_summary.is_some() || !senders.is_empty() || !slots.is_empty() {
        return Ok(CapabilityStatus::Available);
    }

    let settings = sqlx::query(
        "SELECT current_setting('max_wal_senders')::bigint, current_setting('max_replication_slots')::bigint, pg_is_in_recovery()",
    )
    .fetch_one(connection.as_mut())
    .await
    .map_err(super::connect::classify_query_error)?;
    let max_wal_senders = settings
        .try_get::<i64, _>(0)
        .map_err(super::connect::classify_query_error)?;
    let max_replication_slots = settings
        .try_get::<i64, _>(1)
        .map_err(super::connect::classify_query_error)?;
    let in_recovery = settings
        .try_get::<bool, _>(2)
        .map_err(super::connect::classify_query_error)?;

    if in_recovery || max_wal_senders > 0 || max_replication_slots > 0 {
        Ok(CapabilityStatus::Available)
    } else {
        Ok(CapabilityStatus::unavailable(
            "Replication is disabled on this server (max_wal_senders=0, max_replication_slots=0).",
        ))
    }
}

#[cfg(test)]
mod tests {
    use super::format_receiver_summary;

    #[test]
    fn test_format_receiver_summary_omits_empty_fields() {
        let summary = format_receiver_summary("streaming", "replica", "5432", "", "");

        assert_eq!(summary, "Receiver: streaming | source=replica:5432");
    }
}
