//! SQLite persistence adapters for MCP server snapshots and Streamable HTTP session/SSE state.
//!
//! This module intentionally accepts an already-open [`rusqlite::Connection`] instead of opening
//! arbitrary paths. Hosts can therefore reuse their hardened no-follow/open-permission policy.

use super::{
    McpHttpSession, McpHttpSessionStore, McpServerState, McpServerStateStore, McpSseEvent,
    ProtocolError, ProtocolErrorCode, ProtocolResult, MCP_SERVER_STATE_VERSION,
};
use rusqlite::{params, Connection, ErrorCode, OptionalExtension, TransactionBehavior};
use std::sync::Mutex;

const SQLITE_MCP_SCHEMA: &str = r#"
PRAGMA foreign_keys=ON;
CREATE TABLE IF NOT EXISTS aikit_mcp_server_state (
  namespace TEXT PRIMARY KEY,
  revision INTEGER NOT NULL,
  schema_version INTEGER NOT NULL,
  state_json TEXT NOT NULL
);
CREATE TABLE IF NOT EXISTS aikit_mcp_http_sessions (
  session_id TEXT PRIMARY KEY,
  connection_id TEXT NOT NULL UNIQUE,
  subject TEXT NOT NULL,
  tenant_id TEXT,
  created_at_unix_ms INTEGER NOT NULL,
  expires_at_unix_ms INTEGER NOT NULL,
  next_event_sequence INTEGER NOT NULL
);
CREATE TABLE IF NOT EXISTS aikit_mcp_sse_events (
  session_id TEXT NOT NULL,
  sequence INTEGER NOT NULL,
  event_id TEXT NOT NULL,
  data_json TEXT NOT NULL,
  PRIMARY KEY(session_id, sequence),
  UNIQUE(session_id, event_id),
  FOREIGN KEY(session_id) REFERENCES aikit_mcp_http_sessions(session_id) ON DELETE CASCADE
);
"#;

const MAX_SQLITE_MCP_STATE_BYTES: usize = 32 * 1024 * 1024;

/// Transactional SQLite store for MCP state. Clone by opening another connection to the same
/// database and calling [`Self::from_connection`] again.
pub struct SqliteMcpStore {
    connection: Mutex<Connection>,
}

impl SqliteMcpStore {
    pub fn from_connection(connection: Connection) -> ProtocolResult<Self> {
        connection
            .busy_timeout(std::time::Duration::from_secs(5))
            .map_err(sqlite_error)?;
        connection
            .execute_batch(SQLITE_MCP_SCHEMA)
            .map_err(sqlite_error)?;
        ensure_session_column(
            &connection,
            "created_at_unix_ms",
            "INTEGER NOT NULL DEFAULT 0",
        )?;
        ensure_session_column(
            &connection,
            "expires_at_unix_ms",
            "INTEGER NOT NULL DEFAULT 0",
        )?;
        Ok(Self {
            connection: Mutex::new(connection),
        })
    }

    fn connection(&self) -> ProtocolResult<std::sync::MutexGuard<'_, Connection>> {
        self.connection.lock().map_err(|_| {
            ProtocolError::new(ProtocolErrorCode::Conflict, "MCP SQLite lock poisoned")
        })
    }
}

impl McpServerStateStore for SqliteMcpStore {
    fn load(&self, namespace: &str) -> ProtocolResult<Option<McpServerState>> {
        let connection = self.connection()?;
        let row = connection
            .query_row(
                "SELECT revision, schema_version, state_json FROM aikit_mcp_server_state WHERE namespace = ?1",
                params![namespace],
                |row| {
                    Ok((
                        row.get::<_, i64>(0)?,
                        row.get::<_, i64>(1)?,
                        row.get::<_, String>(2)?,
                    ))
                },
            )
            .optional()
            .map_err(sqlite_error)?;
        let Some((revision, schema_version, encoded)) = row else {
            return Ok(None);
        };
        if encoded.len() > MAX_SQLITE_MCP_STATE_BYTES {
            return Err(ProtocolError::invalid(
                "persisted MCP SQLite state exceeds the byte limit",
            ));
        }
        let revision = u64::try_from(revision)
            .map_err(|_| ProtocolError::invalid("negative MCP SQLite revision"))?;
        let schema_version = u32::try_from(schema_version)
            .map_err(|_| ProtocolError::invalid("invalid MCP SQLite schema version"))?;
        if schema_version != MCP_SERVER_STATE_VERSION {
            return Err(ProtocolError::invalid(
                "unsupported persisted MCP SQLite schema version",
            ));
        }
        let state: McpServerState = serde_json::from_str(&encoded).map_err(|error| {
            ProtocolError::invalid(format!("invalid persisted MCP SQLite state: {error}"))
        })?;
        if state.storage_revision() != revision || state.schema_version() != schema_version {
            return Err(ProtocolError::invalid(
                "MCP SQLite row metadata does not match serialized state",
            ));
        }
        Ok(Some(state))
    }

    fn compare_and_swap(
        &self,
        namespace: &str,
        expected_revision: Option<u64>,
        state: &McpServerState,
    ) -> ProtocolResult<()> {
        let encoded = serde_json::to_string(state).map_err(|error| {
            ProtocolError::invalid(format!("MCP state serialization failed: {error}"))
        })?;
        if encoded.len() > MAX_SQLITE_MCP_STATE_BYTES {
            return Err(ProtocolError::invalid(
                "MCP state exceeds the SQLite byte limit",
            ));
        }
        let revision = i64::try_from(state.storage_revision())
            .map_err(|_| ProtocolError::invalid("MCP revision exceeds SQLite i64"))?;
        let schema_version = i64::from(state.schema_version());
        let mut connection = self.connection()?;
        let transaction = connection
            .transaction_with_behavior(TransactionBehavior::Immediate)
            .map_err(sqlite_error)?;
        let changed = match expected_revision {
            None => match transaction.execute(
                "INSERT INTO aikit_mcp_server_state(namespace, revision, schema_version, state_json) VALUES (?1, ?2, ?3, ?4)",
                params![namespace, revision, schema_version, encoded],
            ) {
                Ok(changed) => changed,
                Err(error) if error.sqlite_error_code() == Some(ErrorCode::ConstraintViolation) => 0,
                Err(error) => return Err(sqlite_error(error)),
            },
            Some(expected) => {
                let expected = i64::try_from(expected)
                    .map_err(|_| ProtocolError::invalid("expected MCP revision exceeds SQLite i64"))?;
                transaction
                    .execute(
                        "UPDATE aikit_mcp_server_state SET revision = ?1, schema_version = ?2, state_json = ?3 WHERE namespace = ?4 AND revision = ?5",
                        params![revision, schema_version, encoded, namespace, expected],
                    )
                    .map_err(sqlite_error)?
            }
        };
        if changed != 1 {
            return Err(ProtocolError::conflict(
                "MCP SQLite compare-and-swap revision conflict",
            ));
        }
        transaction.commit().map_err(sqlite_error)
    }
}

impl McpHttpSessionStore for SqliteMcpStore {
    fn create_session(&self, session: &McpHttpSession, max_sessions: usize) -> ProtocolResult<()> {
        let created_at = i64::try_from(session.created_at_unix_ms)
            .map_err(|_| ProtocolError::invalid("MCP HTTP session timestamp exceeds SQLite i64"))?;
        let expires_at = i64::try_from(session.expires_at_unix_ms)
            .map_err(|_| ProtocolError::invalid("MCP HTTP session timestamp exceeds SQLite i64"))?;
        let max_sessions = i64::try_from(max_sessions.max(1))
            .map_err(|_| ProtocolError::invalid("MCP HTTP session bound exceeds SQLite i64"))?;
        let mut connection = self.connection()?;
        let transaction = connection
            .transaction_with_behavior(TransactionBehavior::Immediate)
            .map_err(sqlite_error)?;
        transaction
            .execute(
                "DELETE FROM aikit_mcp_http_sessions WHERE expires_at_unix_ms = 0 OR expires_at_unix_ms <= ?1",
                params![created_at],
            )
            .map_err(sqlite_error)?;
        let count: i64 = transaction
            .query_row("SELECT COUNT(*) FROM aikit_mcp_http_sessions", [], |row| {
                row.get(0)
            })
            .map_err(sqlite_error)?;
        if count >= max_sessions {
            return Err(ProtocolError::conflict(
                "MCP HTTP session capacity exhausted",
            ));
        }
        transaction
            .execute(
                "INSERT INTO aikit_mcp_http_sessions(session_id, connection_id, subject, tenant_id, created_at_unix_ms, expires_at_unix_ms, next_event_sequence) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7)",
                params![session.session_id, session.connection_id, session.subject, session.tenant_id, created_at, expires_at, 1_i64],
            )
            .map_err(sqlite_error)?;
        transaction.commit().map_err(sqlite_error)
    }

    fn load_session(&self, session_id: &str) -> ProtocolResult<Option<McpHttpSession>> {
        self.connection()?
            .query_row(
                "SELECT connection_id, subject, tenant_id, created_at_unix_ms, expires_at_unix_ms FROM aikit_mcp_http_sessions WHERE session_id = ?1",
                params![session_id],
                |row| {
                    Ok(McpHttpSession {
                        session_id: session_id.to_owned(),
                        connection_id: row.get(0)?,
                        subject: row.get(1)?,
                        tenant_id: row.get(2)?,
                        created_at_unix_ms: u64::try_from(row.get::<_, i64>(3)?)
                            .map_err(|_| rusqlite::Error::IntegralValueOutOfRange(3, 0))?,
                        expires_at_unix_ms: u64::try_from(row.get::<_, i64>(4)?)
                            .map_err(|_| rusqlite::Error::IntegralValueOutOfRange(4, 0))?,
                    })
                },
            )
            .optional()
            .map_err(sqlite_error)
    }

    fn delete_session(&self, session_id: &str) -> ProtocolResult<bool> {
        self.connection()?
            .execute(
                "DELETE FROM aikit_mcp_http_sessions WHERE session_id = ?1",
                params![session_id],
            )
            .map(|changed| changed == 1)
            .map_err(sqlite_error)
    }

    fn purge_expired(&self, now_unix_ms: u64) -> ProtocolResult<usize> {
        let now = i64::try_from(now_unix_ms)
            .map_err(|_| ProtocolError::invalid("MCP HTTP session timestamp exceeds SQLite i64"))?;
        self.connection()?
            .execute(
                "DELETE FROM aikit_mcp_http_sessions WHERE expires_at_unix_ms = 0 OR expires_at_unix_ms <= ?1",
                params![now],
            )
            .map_err(sqlite_error)
    }

    fn append_event(
        &self,
        session_id: &str,
        data: &serde_json::Value,
        max_events: usize,
        max_event_bytes: usize,
    ) -> ProtocolResult<McpSseEvent> {
        let data_json = serde_json::to_string(data)
            .map_err(|error| ProtocolError::invalid(format!("invalid SSE event: {error}")))?;
        if data_json.len() > max_event_bytes.max(1) {
            return Err(ProtocolError::invalid(
                "MCP SSE event exceeds the configured byte limit",
            ));
        }
        let mut connection = self.connection()?;
        let transaction = connection
            .transaction_with_behavior(TransactionBehavior::Immediate)
            .map_err(sqlite_error)?;
        let next: i64 = transaction
            .query_row(
                "SELECT next_event_sequence FROM aikit_mcp_http_sessions WHERE session_id = ?1",
                params![session_id],
                |row| row.get(0),
            )
            .optional()
            .map_err(sqlite_error)?
            .ok_or_else(|| ProtocolError::not_found("MCP HTTP session is not registered"))?;
        let sequence =
            u64::try_from(next).map_err(|_| ProtocolError::invalid("invalid MCP SSE sequence"))?;
        let event_id =
            crate::durability::stable_id("mcp_sse", &[session_id, sequence.to_string().as_str()]);
        transaction
            .execute(
                "INSERT INTO aikit_mcp_sse_events(session_id, sequence, event_id, data_json) VALUES (?1, ?2, ?3, ?4)",
                params![session_id, next, event_id, data_json],
            )
            .map_err(sqlite_error)?;
        transaction
            .execute(
                "UPDATE aikit_mcp_http_sessions SET next_event_sequence = ?1 WHERE session_id = ?2",
                params![next.saturating_add(1), session_id],
            )
            .map_err(sqlite_error)?;
        let keep = i64::try_from(max_events.max(1))
            .map_err(|_| ProtocolError::invalid("MCP SSE event bound exceeds SQLite i64"))?;
        transaction
            .execute(
                "DELETE FROM aikit_mcp_sse_events WHERE session_id = ?1 AND sequence <= (SELECT COALESCE(MAX(sequence), 0) - ?2 FROM aikit_mcp_sse_events WHERE session_id = ?1)",
                params![session_id, keep],
            )
            .map_err(sqlite_error)?;
        transaction.commit().map_err(sqlite_error)?;
        Ok(McpSseEvent {
            event_id,
            sequence,
            data: data.clone(),
        })
    }

    fn replay_events(
        &self,
        session_id: &str,
        last_event_id: Option<&str>,
        limit: usize,
        max_bytes: usize,
    ) -> ProtocolResult<Vec<McpSseEvent>> {
        let connection = self.connection()?;
        let session_exists = connection
            .query_row(
                "SELECT 1 FROM aikit_mcp_http_sessions WHERE session_id = ?1",
                params![session_id],
                |_| Ok(()),
            )
            .optional()
            .map_err(sqlite_error)?
            .is_some();
        if !session_exists {
            return Err(ProtocolError::not_found(
                "MCP HTTP session is not registered",
            ));
        }
        let after = match last_event_id {
            None => 0_i64,
            Some(event_id) => connection
                .query_row(
                    "SELECT sequence FROM aikit_mcp_sse_events WHERE session_id = ?1 AND event_id = ?2",
                    params![session_id, event_id],
                    |row| row.get::<_, i64>(0),
                )
                .optional()
                .map_err(sqlite_error)?
                .ok_or_else(|| ProtocolError::invalid("Last-Event-ID is not retained for this MCP session"))?,
        };
        let limit = i64::try_from(limit.max(1))
            .map_err(|_| ProtocolError::invalid("MCP SSE replay limit exceeds SQLite i64"))?;
        let mut statement = connection
            .prepare(
                "SELECT sequence, event_id, data_json FROM aikit_mcp_sse_events WHERE session_id = ?1 AND sequence > ?2 ORDER BY sequence ASC LIMIT ?3",
            )
            .map_err(sqlite_error)?;
        let mut rows = statement
            .query(params![session_id, after, limit])
            .map_err(sqlite_error)?;
        let mut events = Vec::new();
        let mut retained_bytes = 0_usize;
        while let Some(row) = rows.next().map_err(sqlite_error)? {
            let sequence = row.get::<_, i64>(0).map_err(sqlite_error)?;
            let event_id = row.get::<_, String>(1).map_err(sqlite_error)?;
            let data = row.get::<_, String>(2).map_err(sqlite_error)?;
            if retained_bytes.saturating_add(data.len()) > max_bytes.max(1) {
                if events.is_empty() {
                    return Err(ProtocolError::new(
                        ProtocolErrorCode::Conflict,
                        "persisted MCP SSE event exceeds the replay byte limit",
                    ));
                }
                break;
            }
            retained_bytes = retained_bytes.saturating_add(data.len());
            events.push(McpSseEvent {
                event_id,
                sequence: u64::try_from(sequence)
                    .map_err(|_| ProtocolError::invalid("negative MCP SSE sequence"))?,
                data: serde_json::from_str(&data).map_err(|error| {
                    ProtocolError::invalid(format!("invalid persisted MCP SSE event: {error}"))
                })?,
            });
        }
        Ok(events)
    }
}

fn sqlite_error(error: rusqlite::Error) -> ProtocolError {
    ProtocolError::new(
        ProtocolErrorCode::Conflict,
        format!("MCP SQLite error: {error}"),
    )
}

fn ensure_session_column(
    connection: &Connection,
    name: &str,
    definition: &str,
) -> ProtocolResult<()> {
    let mut statement = connection
        .prepare("PRAGMA table_info(aikit_mcp_http_sessions)")
        .map_err(sqlite_error)?;
    let columns = statement
        .query_map([], |row| row.get::<_, String>(1))
        .map_err(sqlite_error)?;
    for column in columns {
        if column.map_err(sqlite_error)? == name {
            return Ok(());
        }
    }
    drop(statement);
    if !matches!(name, "created_at_unix_ms" | "expires_at_unix_ms") {
        return Err(ProtocolError::invalid(
            "invalid MCP SQLite migration column",
        ));
    }
    connection
        .execute(
            &format!("ALTER TABLE aikit_mcp_http_sessions ADD COLUMN {name} {definition}"),
            [],
        )
        .map(|_| ())
        .map_err(sqlite_error)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::protocols::{McpServerInfo, McpServerRegistry};
    use std::sync::Arc;

    #[test]
    fn sqlite_store_persists_server_cas_sessions_and_bounded_sse() {
        let store = Arc::new(
            SqliteMcpStore::from_connection(Connection::open_in_memory().unwrap()).unwrap(),
        );
        let server = crate::protocols::McpJsonRpcServer::new(
            "sqlite-test",
            McpServerInfo::new("test", "1").unwrap(),
            store.clone(),
            McpServerRegistry::new(),
        )
        .unwrap();
        assert_eq!(server.snapshot().unwrap().storage_revision(), 1);
        let session = McpHttpSession {
            session_id: "session-1".into(),
            connection_id: "connection-1".into(),
            subject: "user-1".into(),
            tenant_id: Some("tenant-1".into()),
            created_at_unix_ms: 1,
            expires_at_unix_ms: 10_000,
        };
        store.create_session(&session, 10).unwrap();
        let first = store
            .append_event("session-1", &serde_json::json!({"n":1}), 2, 1024)
            .unwrap();
        assert_eq!(
            store
                .append_event(
                    "session-1",
                    &serde_json::json!({"oversized":"payload"}),
                    2,
                    8
                )
                .unwrap_err()
                .code,
            ProtocolErrorCode::InvalidRequest
        );
        store
            .append_event("session-1", &serde_json::json!({"n":2}), 2, 1024)
            .unwrap();
        store
            .append_event("session-1", &serde_json::json!({"n":3}), 2, 1024)
            .unwrap();
        assert!(store
            .replay_events("session-1", Some(&first.event_id), 10, 4096)
            .is_err());
        let retained = store.replay_events("session-1", None, 10, 4096).unwrap();
        assert_eq!(retained.len(), 2);
        assert_eq!(
            store
                .replay_events("session-1", None, 10, 1)
                .unwrap_err()
                .code,
            ProtocolErrorCode::Conflict
        );
        assert!(store.delete_session("session-1").unwrap());
        assert!(store.load_session("session-1").unwrap().is_none());
        assert_eq!(
            store
                .replay_events("session-1", None, 10, 4096)
                .unwrap_err()
                .code,
            ProtocolErrorCode::NotFound
        );
        let expiring = McpHttpSession {
            session_id: "expiring".into(),
            connection_id: "expiring".into(),
            subject: "owner".into(),
            tenant_id: None,
            created_at_unix_ms: 20,
            expires_at_unix_ms: 30,
        };
        store.create_session(&expiring, 1).unwrap();
        let capacity = McpHttpSession {
            session_id: "capacity".into(),
            connection_id: "capacity".into(),
            subject: "owner".into(),
            tenant_id: None,
            created_at_unix_ms: 21,
            expires_at_unix_ms: 40,
        };
        assert_eq!(
            store.create_session(&capacity, 1).unwrap_err().code,
            ProtocolErrorCode::Conflict
        );
        assert_eq!(store.purge_expired(30).unwrap(), 1);
        assert!(store.load_session("expiring").unwrap().is_none());
    }

    #[test]
    fn sqlite_store_reopens_server_sessions_and_sse_from_the_same_database() {
        let directory = tempfile::tempdir().unwrap();
        let path = directory.path().join("mcp.sqlite3");
        let first =
            Arc::new(SqliteMcpStore::from_connection(Connection::open(&path).unwrap()).unwrap());
        let server = crate::protocols::McpJsonRpcServer::new(
            "reopen-test",
            McpServerInfo::new("test", "1").unwrap(),
            first.clone(),
            McpServerRegistry::new(),
        )
        .unwrap();
        let session = McpHttpSession {
            session_id: "persisted-session".into(),
            connection_id: "persisted-connection".into(),
            subject: "persisted-owner".into(),
            tenant_id: None,
            created_at_unix_ms: 1,
            expires_at_unix_ms: 10_000,
        };
        first.create_session(&session, 10).unwrap();
        let event = first
            .append_event(
                &session.session_id,
                &serde_json::json!({"persisted":true}),
                10,
                1024,
            )
            .unwrap();

        let reopened =
            Arc::new(SqliteMcpStore::from_connection(Connection::open(&path).unwrap()).unwrap());
        let restarted = crate::protocols::McpJsonRpcServer::new(
            "reopen-test",
            McpServerInfo::new("test", "1").unwrap(),
            reopened.clone(),
            McpServerRegistry::new(),
        )
        .unwrap();
        assert_eq!(
            restarted.snapshot().unwrap().storage_revision(),
            server.snapshot().unwrap().storage_revision()
        );
        assert_eq!(
            reopened.load_session(&session.session_id).unwrap(),
            Some(session.clone())
        );
        assert_eq!(
            reopened
                .replay_events(&session.session_id, None, 10, 4096)
                .unwrap(),
            vec![event]
        );
    }

    #[test]
    fn sqlite_store_migrates_legacy_session_rows_to_fail_closed_expiry() {
        let connection = Connection::open_in_memory().unwrap();
        connection
            .execute_batch(
                "CREATE TABLE aikit_mcp_http_sessions (
                    session_id TEXT PRIMARY KEY,
                    connection_id TEXT NOT NULL UNIQUE,
                    subject TEXT NOT NULL,
                    tenant_id TEXT,
                    next_event_sequence INTEGER NOT NULL
                );
                INSERT INTO aikit_mcp_http_sessions
                    (session_id, connection_id, subject, tenant_id, next_event_sequence)
                VALUES ('legacy', 'legacy-connection', 'legacy-owner', NULL, 1);",
            )
            .unwrap();
        let store = SqliteMcpStore::from_connection(connection).unwrap();
        assert_eq!(store.purge_expired(1).unwrap(), 1);
        assert!(store.load_session("legacy").unwrap().is_none());
        let replacement = McpHttpSession {
            session_id: "replacement".into(),
            connection_id: "replacement-connection".into(),
            subject: "owner".into(),
            tenant_id: None,
            created_at_unix_ms: 1,
            expires_at_unix_ms: 10,
        };
        store.create_session(&replacement, 1).unwrap();
        assert_eq!(
            store.load_session("replacement").unwrap(),
            Some(replacement)
        );
    }
}
