//! Replication fetchers and helpers for `PgClient`.

use super::{CapabilityStatus, PgClient, ReplicationSender, ReplicationSlot, ReplicationSnapshot};
use crate::pg::queries::{
    REPLICATION_RECEIVER_QUERY, REPLICATION_SENDERS_QUERY, REPLICATION_SLOTS_QUERY,
};
use anyhow::Result;
use postgres::Client;

impl PgClient {
    pub fn fetch_replication_snapshot(&mut self) -> Result<ReplicationSnapshot> {
        let receiver_summary =
            if let Some(row) = self.client.query_opt(REPLICATION_RECEIVER_QUERY, &[])? {
                let status = row.try_get::<_, String>(0)?;
                let sender_host = row.try_get::<_, String>(1)?;
                let sender_port = row.try_get::<_, String>(2)?;
                let slot_name = row.try_get::<_, String>(3)?;
                let latest_end_lsn = row.try_get::<_, String>(4)?;
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

        let senders = self
            .client
            .query(REPLICATION_SENDERS_QUERY, &[])?
            .into_iter()
            .map(|row| {
                Ok(ReplicationSender {
                    pid: row.try_get::<_, String>(0)?,
                    user: row.try_get::<_, String>(1)?,
                    application: row.try_get::<_, String>(2)?,
                    client: row.try_get::<_, String>(3)?,
                    state: row.try_get::<_, String>(4)?,
                    sync_state: row.try_get::<_, String>(5)?,
                    slot_name: row.try_get::<_, String>(6)?,
                    sent_lag_bytes: row.try_get::<_, i64>(7)?,
                    write_lag_bytes: row.try_get::<_, i64>(8)?,
                    flush_lag_bytes: row.try_get::<_, i64>(9)?,
                    replay_lag_bytes: row.try_get::<_, i64>(10)?,
                })
            })
            .collect::<Result<Vec<_>>>()?;

        let slots = self
            .client
            .query(REPLICATION_SLOTS_QUERY, &[])?
            .into_iter()
            .map(|row| {
                Ok(ReplicationSlot {
                    slot_name: row.try_get::<_, String>(0)?,
                    slot_type: row.try_get::<_, String>(1)?,
                    active: row.try_get::<_, String>(2)?,
                    active_pid: row.try_get::<_, String>(3)?,
                    restart_lsn: row.try_get::<_, String>(4)?,
                    confirmed_flush_lsn: row.try_get::<_, String>(5)?,
                    wal_status: row.try_get::<_, String>(6)?,
                })
            })
            .collect::<Result<Vec<_>>>()?;

        let capability = detect_replication_capability(
            &mut self.client,
            receiver_summary.as_ref(),
            &senders,
            &slots,
        )?;
        self.capability_cache.replication = capability.clone();

        Ok(ReplicationSnapshot {
            capability,
            receiver_summary,
            senders,
            slots,
        })
    }

    pub(crate) fn replication_capability(&self) -> CapabilityStatus {
        self.capability_cache.replication.clone()
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

fn detect_replication_capability(
    client: &mut Client,
    receiver_summary: Option<&String>,
    senders: &[ReplicationSender],
    slots: &[ReplicationSlot],
) -> Result<CapabilityStatus> {
    if receiver_summary.is_some() || !senders.is_empty() || !slots.is_empty() {
        return Ok(CapabilityStatus::Available);
    }

    let settings = client.query_one(
        "SELECT current_setting('max_wal_senders')::bigint, current_setting('max_replication_slots')::bigint, pg_is_in_recovery()",
        &[],
    )?;
    let max_wal_senders = settings.try_get::<_, i64>(0)?;
    let max_replication_slots = settings.try_get::<_, i64>(1)?;
    let in_recovery = settings.try_get::<_, bool>(2)?;

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
