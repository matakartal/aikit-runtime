//! Transactional, cross-process local persistence for memory and resumable sessions.

use crate::memory::{MemoryEntry, MemoryQuery, MemoryStore};
use crate::session::{Session, SessionStore, SessionStoreError, SessionStoreResult};
use rusqlite::{params, Connection, ErrorCode, OptionalExtension};
use std::path::Path;
use std::sync::Mutex;
use std::time::{SystemTime, UNIX_EPOCH};

const SCHEMA: &str = r#"
PRAGMA journal_mode=WAL;
PRAGMA foreign_keys=ON;
CREATE TABLE IF NOT EXISTS aikit_memory (
  namespace TEXT NOT NULL,
  key TEXT NOT NULL,
  entry_json TEXT NOT NULL,
  importance INTEGER NOT NULL,
  updated_ms TEXT NOT NULL,
  PRIMARY KEY(namespace, key)
);
CREATE TABLE IF NOT EXISTS aikit_sessions (
  id TEXT PRIMARY KEY,
  revision INTEGER NOT NULL,
  session_json TEXT NOT NULL
);
"#;

fn open(path: impl AsRef<Path>) -> std::result::Result<Connection, String> {
    if let Some(parent) = path.as_ref().parent() {
        std::fs::create_dir_all(parent).map_err(|e| e.to_string())?;
    }
    let connection = Connection::open(path).map_err(|e| e.to_string())?;
    connection
        .busy_timeout(std::time::Duration::from_secs(5))
        .map_err(|e| e.to_string())?;
    connection
        .execute_batch(SCHEMA)
        .map_err(|e| e.to_string())?;
    Ok(connection)
}

pub struct SqliteMemoryStore {
    connection: Mutex<Connection>,
}

impl SqliteMemoryStore {
    pub fn open(path: impl AsRef<Path>) -> std::result::Result<Self, String> {
        Ok(Self {
            connection: Mutex::new(open(path)?),
        })
    }
}

impl MemoryStore for SqliteMemoryStore {
    fn put(&self, mut entry: MemoryEntry) -> std::result::Result<(), String> {
        if entry.namespace.trim().is_empty() || entry.key.trim().is_empty() {
            return Err("memory namespace and key must be non-empty".into());
        }
        if entry.namespace.len() > 256 || entry.key.len() > 512 {
            return Err("memory namespace/key exceeds length limit".into());
        }
        let connection = self
            .connection
            .lock()
            .map_err(|_| "SQLite mutex poisoned")?;
        let created: Option<String> = connection
            .query_row(
                "SELECT entry_json FROM aikit_memory WHERE namespace=?1 AND key=?2",
                params![entry.namespace, entry.key],
                |row| row.get(0),
            )
            .optional()
            .map_err(|e| e.to_string())?;
        if let Some(created) = created {
            let existing: MemoryEntry =
                serde_json::from_str(&created).map_err(|e| e.to_string())?;
            entry.created_unix_ms = existing.created_unix_ms;
        }
        entry.updated_unix_ms = now_ms() as u128;
        let json = serde_json::to_string(&entry).map_err(|e| e.to_string())?;
        connection.execute(
            "INSERT INTO aikit_memory(namespace,key,entry_json,importance,updated_ms) VALUES(?1,?2,?3,?4,?5)
             ON CONFLICT(namespace,key) DO UPDATE SET entry_json=excluded.entry_json, importance=excluded.importance, updated_ms=excluded.updated_ms",
            params![entry.namespace, entry.key, json, entry.importance, entry.updated_unix_ms.to_string()],
        ).map_err(|e| e.to_string())?;
        Ok(())
    }

    fn get(&self, namespace: &str, key: &str) -> std::result::Result<Option<MemoryEntry>, String> {
        let connection = self
            .connection
            .lock()
            .map_err(|_| "SQLite mutex poisoned")?;
        let json: Option<String> = connection
            .query_row(
                "SELECT entry_json FROM aikit_memory WHERE namespace=?1 AND key=?2",
                params![namespace, key],
                |row| row.get(0),
            )
            .optional()
            .map_err(|e| e.to_string())?;
        json.map(|json| serde_json::from_str(&json).map_err(|e| e.to_string()))
            .transpose()
    }

    fn search(&self, query: &MemoryQuery) -> std::result::Result<Vec<MemoryEntry>, String> {
        let connection = self
            .connection
            .lock()
            .map_err(|_| "SQLite mutex poisoned")?;
        let mut statement = connection.prepare(
            "SELECT entry_json FROM aikit_memory WHERE namespace=?1 ORDER BY importance DESC, length(updated_ms) DESC, updated_ms DESC, key ASC",
        ).map_err(|e| e.to_string())?;
        let entries = statement
            .query_map(params![query.namespace], |row| row.get::<_, String>(0))
            .map_err(|e| e.to_string())?;
        let words: Vec<String> = query
            .text
            .split(|c: char| !c.is_alphanumeric())
            .filter(|word| !word.is_empty())
            .map(str::to_ascii_lowercase)
            .collect();
        let mut found = Vec::new();
        for json in entries {
            let entry: MemoryEntry = serde_json::from_str(&json.map_err(|e| e.to_string())?)
                .map_err(|e| e.to_string())?;
            if !query.tags.is_subset(&entry.tags) {
                continue;
            }
            let haystack = format!("{} {}", entry.key, entry.value).to_ascii_lowercase();
            if words.is_empty() || words.iter().any(|word| haystack.contains(word)) {
                found.push(entry);
            }
            if found.len() >= query.limit.min(100) {
                break;
            }
        }
        Ok(found)
    }

    fn delete(&self, namespace: &str, key: &str) -> std::result::Result<bool, String> {
        let connection = self
            .connection
            .lock()
            .map_err(|_| "SQLite mutex poisoned")?;
        Ok(connection
            .execute(
                "DELETE FROM aikit_memory WHERE namespace=?1 AND key=?2",
                params![namespace, key],
            )
            .map_err(|e| e.to_string())?
            > 0)
    }
}

pub struct SqliteSessionStore {
    connection: Mutex<Connection>,
}

impl SqliteSessionStore {
    pub fn open(path: impl AsRef<Path>) -> SessionStoreResult<Self> {
        Ok(Self {
            connection: Mutex::new(open(path).map_err(io_error)?),
        })
    }
}

impl SessionStore for SqliteSessionStore {
    fn create_session(&self, mut session: Session) -> SessionStoreResult<Session> {
        validate_id(&session.id)?;
        let now = now_ms();
        session.revision = 1;
        session.created_at_unix_ms = now;
        session.updated_at_unix_ms = now;
        let json = serde_json::to_string(&session)?;
        let connection = self
            .connection
            .lock()
            .map_err(|_| SessionStoreError::LockPoisoned)?;
        match connection.execute(
            "INSERT INTO aikit_sessions(id,revision,session_json) VALUES(?1,?2,?3)",
            params![session.id, session.revision, json],
        ) {
            Ok(_) => Ok(session),
            Err(error) if error.sqlite_error_code() == Some(ErrorCode::ConstraintViolation) => {
                let actual = current_revision(&connection, &session.id)?.unwrap_or(0);
                Err(SessionStoreError::Conflict {
                    id: session.id,
                    expected_revision: 0,
                    actual_revision: actual,
                })
            }
            Err(error) => Err(io_error(error.to_string())),
        }
    }

    fn load_session(&self, id: &str) -> SessionStoreResult<Session> {
        validate_id(id)?;
        let connection = self
            .connection
            .lock()
            .map_err(|_| SessionStoreError::LockPoisoned)?;
        let json: Option<String> = connection
            .query_row(
                "SELECT session_json FROM aikit_sessions WHERE id=?1",
                params![id],
                |row| row.get(0),
            )
            .optional()
            .map_err(|e| io_error(e.to_string()))?;
        json.map(|value| serde_json::from_str(&value).map_err(SessionStoreError::from))
            .transpose()?
            .ok_or_else(|| SessionStoreError::NotFound { id: id.into() })
    }

    fn compare_and_swap(
        &self,
        expected_revision: u64,
        mut replacement: Session,
    ) -> SessionStoreResult<Session> {
        validate_id(&replacement.id)?;
        let connection = self
            .connection
            .lock()
            .map_err(|_| SessionStoreError::LockPoisoned)?;
        let current_json: Option<String> = connection
            .query_row(
                "SELECT session_json FROM aikit_sessions WHERE id=?1",
                params![replacement.id],
                |row| row.get(0),
            )
            .optional()
            .map_err(|e| io_error(e.to_string()))?;
        let current: Session = match current_json {
            Some(json) => serde_json::from_str(&json)?,
            None => return Err(SessionStoreError::NotFound { id: replacement.id }),
        };
        if current.revision != expected_revision {
            return Err(SessionStoreError::Conflict {
                id: replacement.id,
                expected_revision,
                actual_revision: current.revision,
            });
        }
        replacement.revision = current.revision.saturating_add(1);
        replacement.created_at_unix_ms = current.created_at_unix_ms;
        replacement.updated_at_unix_ms = now_ms().max(current.updated_at_unix_ms);
        let json = serde_json::to_string(&replacement)?;
        let changed = connection
            .execute(
                "UPDATE aikit_sessions SET revision=?1,session_json=?2 WHERE id=?3 AND revision=?4",
                params![
                    replacement.revision,
                    json,
                    replacement.id,
                    expected_revision
                ],
            )
            .map_err(|e| io_error(e.to_string()))?;
        if changed == 0 {
            let actual = current_revision(&connection, &replacement.id)?.unwrap_or(0);
            return Err(SessionStoreError::Conflict {
                id: replacement.id,
                expected_revision,
                actual_revision: actual,
            });
        }
        Ok(replacement)
    }
}

fn current_revision(connection: &Connection, id: &str) -> SessionStoreResult<Option<u64>> {
    connection
        .query_row(
            "SELECT revision FROM aikit_sessions WHERE id=?1",
            params![id],
            |row| row.get(0),
        )
        .optional()
        .map_err(|e| io_error(e.to_string()))
}

fn validate_id(id: &str) -> SessionStoreResult<()> {
    if id.trim().is_empty() || id.chars().any(char::is_control) {
        Err(SessionStoreError::InvalidId {
            reason: "id must be non-empty and contain no control characters".into(),
        })
    } else {
        Ok(())
    }
}

fn io_error(message: impl ToString) -> SessionStoreError {
    SessionStoreError::Io {
        message: message.to_string(),
    }
}

fn now_ms() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis() as u64
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::types::Message;
    use serde_json::json;

    #[test]
    fn sqlite_memory_survives_reopen() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("state.db");
        SqliteMemoryStore::open(&path)
            .unwrap()
            .put(MemoryEntry::new("agent", "choice", json!("Rust")))
            .unwrap();
        let reopened = SqliteMemoryStore::open(&path).unwrap();
        assert_eq!(
            reopened.get("agent", "choice").unwrap().unwrap().value,
            json!("Rust")
        );
    }

    #[test]
    fn sqlite_session_enforces_cross_instance_cas() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("state.db");
        let first = SqliteSessionStore::open(&path).unwrap();
        let second = SqliteSessionStore::open(&path).unwrap();
        let created = first
            .create_session(Session::new("s1", vec![Message::user("one")]))
            .unwrap();
        let mut updated = created.clone();
        updated.messages.push(Message {
            role: crate::types::Role::Assistant,
            content: vec![crate::types::ContentBlock::Text { text: "two".into() }],
        });
        first.compare_and_swap(created.revision, updated).unwrap();
        assert!(matches!(
            second.compare_and_swap(created.revision, created),
            Err(SessionStoreError::Conflict { .. })
        ));
    }
}
