//! Transactional, cross-process local persistence for memory and resumable sessions.

use crate::durability::{RunState, DURABILITY_SCHEMA_VERSION};
use crate::durable_store::{
    validate_append_only, DurableStore, DurableStoreError, DurableStoreResult,
};
use crate::memory::{MemoryEntry, MemoryQuery, MemoryStore};
use crate::session::{
    validate_execution_lease_claim, validate_stored_execution_lease, Session,
    SessionExecutionLease, SessionExecutionLeaseRecord, SessionStore, SessionStoreError,
    SessionStoreResult,
};
use rusqlite::{params, Connection, ErrorCode, OpenFlags, OptionalExtension, TransactionBehavior};
use std::fs::{self, File, OpenOptions};
use std::path::{Path, PathBuf};
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
CREATE TABLE IF NOT EXISTS aikit_durable_runs (
  run_id TEXT PRIMARY KEY,
  revision INTEGER NOT NULL,
  schema_version INTEGER NOT NULL,
  state_json TEXT NOT NULL
);
CREATE TABLE IF NOT EXISTS aikit_session_execution_leases (
  id TEXT PRIMARY KEY,
  owner TEXT NOT NULL,
  token TEXT NOT NULL,
  expires_at_unix_ms INTEGER NOT NULL
);
"#;

fn open(path: impl AsRef<Path>) -> std::result::Result<Connection, String> {
    let path = path.as_ref();
    if let Some(parent) = path
        .parent()
        .filter(|parent| !parent.as_os_str().is_empty())
    {
        std::fs::create_dir_all(parent).map_err(|e| e.to_string())?;
    }
    // Pre-create/open with O_NOFOLLOW on Unix, then make SQLite open only that existing path. A
    // second descriptor check catches ordinary target replacement between the secure open and
    // SQLite's open. Schema writes happen only after this verification.
    let database_file = open_database_file(path, true)?;
    // Resolve benign parent aliases (notably macOS `/var` -> `/private/var`) without resolving the
    // final component. A final-path swap to a symlink therefore still reaches SQLite as a symlink
    // and is rejected by SQLITE_OPEN_NOFOLLOW.
    let sqlite_path = sqlite_nofollow_path(path)?;
    let connection = Connection::open_with_flags(&sqlite_path, database_open_flags())
        .map_err(|e| e.to_string())?;
    let verified_file = open_database_file(path, false)?;
    if !same_open_file(&database_file, &verified_file)? {
        return Err(format!(
            "SQLite database {} changed while it was being opened",
            path.display()
        ));
    }
    connection
        .busy_timeout(std::time::Duration::from_secs(5))
        .map_err(|e| e.to_string())?;
    connection
        .execute_batch(SCHEMA)
        .map_err(|e| e.to_string())?;
    ensure_execution_lease_token_column(&connection)?;
    ensure_durable_schema_version_column(&connection)?;
    tighten_owner_only_permissions(&database_file)?;
    Ok(connection)
}

fn ensure_execution_lease_token_column(connection: &Connection) -> std::result::Result<(), String> {
    let mut statement = connection
        .prepare("PRAGMA table_info(aikit_session_execution_leases)")
        .map_err(|error| error.to_string())?;
    let columns = statement
        .query_map([], |row| row.get::<_, String>(1))
        .map_err(|error| error.to_string())?
        .collect::<std::result::Result<Vec<_>, _>>()
        .map_err(|error| error.to_string())?;
    drop(statement);
    if !columns.iter().any(|column| column == "token") {
        // Existing in-flight rows remain NULL and therefore fail closed as indeterminate. Only
        // newly acquired/recovered leases receive a valid store-generated token.
        connection
            .execute(
                "ALTER TABLE aikit_session_execution_leases ADD COLUMN token TEXT",
                [],
            )
            .map_err(|error| error.to_string())?;
    }
    Ok(())
}

fn ensure_durable_schema_version_column(
    connection: &Connection,
) -> std::result::Result<(), String> {
    let mut statement = connection
        .prepare("PRAGMA table_info(aikit_durable_runs)")
        .map_err(|error| error.to_string())?;
    let columns = statement
        .query_map([], |row| row.get::<_, String>(1))
        .map_err(|error| error.to_string())?
        .collect::<std::result::Result<Vec<_>, _>>()
        .map_err(|error| error.to_string())?;
    drop(statement);
    if !columns.iter().any(|column| column == "schema_version") {
        // Version 1 predates the explicit SQLite schema column. Existing rows were already
        // serialized with that version, so the migration records it while later loads still
        // validate the row against both the supported version and serialized event log.
        connection
            .execute(
                "ALTER TABLE aikit_durable_runs ADD COLUMN schema_version INTEGER NOT NULL DEFAULT 1",
                [],
            )
            .map_err(|error| error.to_string())?;
    }
    Ok(())
}

fn database_open_flags() -> OpenFlags {
    OpenFlags::SQLITE_OPEN_READ_WRITE | OpenFlags::SQLITE_OPEN_NOFOLLOW
}

fn sqlite_nofollow_path(path: &Path) -> std::result::Result<PathBuf, String> {
    let file_name = path
        .file_name()
        .ok_or_else(|| "SQLite database path must have a final file name".to_string())?;
    let parent = path
        .parent()
        .filter(|parent| !parent.as_os_str().is_empty())
        .unwrap_or_else(|| Path::new("."));
    let canonical_parent = fs::canonicalize(parent).map_err(|error| error.to_string())?;
    Ok(canonical_parent.join(file_name))
}

fn open_database_file(path: &Path, create: bool) -> std::result::Result<File, String> {
    match fs::symlink_metadata(path) {
        Ok(metadata) if metadata.file_type().is_symlink() => {
            return Err(format!(
                "refusing to open SQLite database {} through a symlink",
                path.display()
            ));
        }
        Ok(_) => {}
        Err(error) if create && error.kind() == std::io::ErrorKind::NotFound => {}
        Err(error) => return Err(error.to_string()),
    }

    let mut options = OpenOptions::new();
    options.read(true).write(true).create(create);
    #[cfg(unix)]
    {
        use std::os::unix::fs::OpenOptionsExt;
        options.mode(0o600).custom_flags(libc::O_NOFOLLOW);
    }
    let file = options.open(path).map_err(|error| error.to_string())?;
    if !file
        .metadata()
        .map_err(|error| error.to_string())?
        .is_file()
    {
        return Err(format!(
            "SQLite database {} is not a regular file",
            path.display()
        ));
    }
    tighten_owner_only_permissions(&file)?;
    Ok(file)
}

fn tighten_owner_only_permissions(file: &File) -> std::result::Result<(), String> {
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        file.set_permissions(fs::Permissions::from_mode(0o600))
            .map_err(|error| error.to_string())?;
    }
    #[cfg(not(unix))]
    {
        let _ = file;
    }
    Ok(())
}

#[cfg(unix)]
fn same_open_file(left: &File, right: &File) -> std::result::Result<bool, String> {
    use std::os::unix::fs::MetadataExt;
    let left = left.metadata().map_err(|error| error.to_string())?;
    let right = right.metadata().map_err(|error| error.to_string())?;
    Ok(left.dev() == right.dev() && left.ino() == right.ino())
}

#[cfg(windows)]
fn same_open_file(left: &File, right: &File) -> std::result::Result<bool, String> {
    Ok(windows_file_identity(left)? == windows_file_identity(right)?)
}

#[cfg(windows)]
fn windows_file_identity(file: &File) -> std::result::Result<(u32, u64), String> {
    use std::mem::MaybeUninit;
    use std::os::windows::io::AsRawHandle;
    use windows_sys::Win32::Foundation::HANDLE;
    use windows_sys::Win32::Storage::FileSystem::{
        GetFileInformationByHandle, BY_HANDLE_FILE_INFORMATION,
    };

    let mut information = MaybeUninit::<BY_HANDLE_FILE_INFORMATION>::uninit();
    // SAFETY: `file` owns a valid handle for the duration of this call and Windows initializes the
    // complete output structure when the function reports success.
    let succeeded = unsafe {
        GetFileInformationByHandle(file.as_raw_handle() as HANDLE, information.as_mut_ptr())
    };
    if succeeded == 0 {
        return Err(format!(
            "cannot prove Windows SQLite file identity: {}",
            std::io::Error::last_os_error()
        ));
    }
    // SAFETY: the successful call above initialized `information`.
    let information = unsafe { information.assume_init() };
    let file_index =
        (u64::from(information.nFileIndexHigh) << 32) | u64::from(information.nFileIndexLow);
    Ok((information.dwVolumeSerialNumber, file_index))
}

#[cfg(not(any(unix, windows)))]
fn same_open_file(_left: &File, _right: &File) -> std::result::Result<bool, String> {
    Err("cannot prove SQLite file identity on this platform".into())
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
            entry.revision = existing.revision.saturating_add(1);
        } else {
            entry.revision = 1;
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
            if query.plane.is_some_and(|plane| plane != entry.plane) {
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

    fn compare_and_swap(
        &self,
        mut entry: MemoryEntry,
        expected_revision: u64,
    ) -> std::result::Result<u64, String> {
        if entry.namespace.trim().is_empty() || entry.key.trim().is_empty() {
            return Err("memory namespace and key must be non-empty".into());
        }
        let mut connection = self
            .connection
            .lock()
            .map_err(|_| "SQLite mutex poisoned")?;
        let transaction = connection
            .transaction()
            .map_err(|error| error.to_string())?;
        let existing: Option<String> = transaction
            .query_row(
                "SELECT entry_json FROM aikit_memory WHERE namespace=?1 AND key=?2",
                params![entry.namespace, entry.key],
                |row| row.get(0),
            )
            .optional()
            .map_err(|error| error.to_string())?;
        let existing = existing
            .map(|json| {
                serde_json::from_str::<MemoryEntry>(&json).map_err(|error| error.to_string())
            })
            .transpose()?;
        let actual = existing.as_ref().map_or(0, |existing| existing.revision);
        if actual != expected_revision {
            return Err(format!(
                "memory revision conflict: expected {expected_revision}, found {actual}"
            ));
        }
        if let Some(existing) = &existing {
            entry.created_unix_ms = existing.created_unix_ms;
        }
        entry.revision = actual
            .checked_add(1)
            .ok_or_else(|| "memory revision overflow".to_string())?;
        entry.updated_unix_ms = now_ms() as u128;
        let revision = entry.revision;
        let json = serde_json::to_string(&entry).map_err(|error| error.to_string())?;
        transaction
            .execute(
                "INSERT INTO aikit_memory(namespace,key,entry_json,importance,updated_ms) VALUES(?1,?2,?3,?4,?5)
                 ON CONFLICT(namespace,key) DO UPDATE SET entry_json=excluded.entry_json, importance=excluded.importance, updated_ms=excluded.updated_ms",
                params![entry.namespace, entry.key, json, entry.importance, entry.updated_unix_ms.to_string()],
            )
            .map_err(|error| error.to_string())?;
        transaction.commit().map_err(|error| error.to_string())?;
        Ok(revision)
    }
}

/// Cross-process SQLite checkpoint store for the event-log-authoritative [`RunState`].
pub struct SqliteDurableStore {
    connection: Mutex<Connection>,
}

impl SqliteDurableStore {
    pub fn open(path: impl AsRef<Path>) -> DurableStoreResult<Self> {
        Ok(Self {
            connection: Mutex::new(open(path).map_err(DurableStoreError::Io)?),
        })
    }
}

fn durable_revision(state: &RunState) -> u64 {
    state.events().last().map_or(0, |event| event.sequence)
}

fn durable_revision_sql(revision: u64) -> DurableStoreResult<i64> {
    i64::try_from(revision)
        .map_err(|_| DurableStoreError::Invalid("durable revision exceeds SQLite i64".into()))
}

fn durable_schema_sql(schema_version: u32) -> DurableStoreResult<i64> {
    Ok(i64::from(schema_version))
}

fn decode_durable_row(
    requested_run_id: &str,
    row_run_id: String,
    revision: i64,
    schema_version: i64,
    json: String,
) -> DurableStoreResult<RunState> {
    if row_run_id != requested_run_id {
        return Err(DurableStoreError::Invalid(
            "SQLite row key does not match requested run ID".into(),
        ));
    }
    let revision = u64::try_from(revision)
        .map_err(|_| DurableStoreError::Invalid("negative durable revision in SQLite".into()))?;
    let schema_version = u32::try_from(schema_version).map_err(|_| {
        DurableStoreError::Invalid("invalid SQLite durability schema version".into())
    })?;
    if schema_version != DURABILITY_SCHEMA_VERSION {
        return Err(DurableStoreError::Invalid(format!(
            "unsupported SQLite durability schema {schema_version}; expected {DURABILITY_SCHEMA_VERSION}"
        )));
    }
    let state: RunState = serde_json::from_str(&json)
        .map_err(|error| DurableStoreError::Invalid(error.to_string()))?;
    if state.run_id() != requested_run_id {
        return Err(DurableStoreError::Invalid(
            "SQLite row key does not match serialized run ID".into(),
        ));
    }
    if state.schema_version() != schema_version {
        return Err(DurableStoreError::Invalid(
            "SQLite schema version does not match serialized run state".into(),
        ));
    }
    if durable_revision(&state) != revision {
        return Err(DurableStoreError::Invalid(
            "SQLite revision does not match serialized event log".into(),
        ));
    }
    Ok(state)
}

impl DurableStore for SqliteDurableStore {
    fn create(&self, state: &RunState) -> DurableStoreResult<()> {
        let revision = durable_revision_sql(durable_revision(state))?;
        let schema_version = durable_schema_sql(state.schema_version())?;
        let json = serde_json::to_string(state)
            .map_err(|error| DurableStoreError::Invalid(error.to_string()))?;
        let connection = self
            .connection
            .lock()
            .map_err(|_| DurableStoreError::Io("SQLite mutex poisoned".into()))?;
        match connection.execute(
            "INSERT INTO aikit_durable_runs(run_id,revision,schema_version,state_json) VALUES(?1,?2,?3,?4)",
            params![state.run_id(), revision, schema_version, json],
        ) {
            Ok(_) => Ok(()),
            Err(error) if error.sqlite_error_code() == Some(ErrorCode::ConstraintViolation) => {
                Err(DurableStoreError::AlreadyExists {
                    run_id: state.run_id().into(),
                })
            }
            Err(error) => Err(DurableStoreError::Io(error.to_string())),
        }
    }

    fn load(&self, run_id: &str) -> DurableStoreResult<RunState> {
        let connection = self
            .connection
            .lock()
            .map_err(|_| DurableStoreError::Io("SQLite mutex poisoned".into()))?;
        let row: Option<(String, i64, i64, String)> = connection
            .query_row(
                "SELECT run_id,revision,schema_version,state_json FROM aikit_durable_runs WHERE run_id=?1",
                params![run_id],
                |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?, row.get(3)?)),
            )
            .optional()
            .map_err(|error| DurableStoreError::Io(error.to_string()))?;
        let (row_run_id, revision, schema_version, json) =
            row.ok_or_else(|| DurableStoreError::NotFound {
                run_id: run_id.into(),
            })?;
        decode_durable_row(run_id, row_run_id, revision, schema_version, json)
    }

    fn compare_and_swap(
        &self,
        expected_sequence: u64,
        replacement: &RunState,
    ) -> DurableStoreResult<()> {
        let expected = durable_revision_sql(expected_sequence)?;
        let replacement_revision = durable_revision_sql(durable_revision(replacement))?;
        let replacement_schema = durable_schema_sql(replacement.schema_version())?;
        let replacement_json = serde_json::to_string(replacement)
            .map_err(|error| DurableStoreError::Invalid(error.to_string()))?;
        let mut connection = self
            .connection
            .lock()
            .map_err(|_| DurableStoreError::Io("SQLite mutex poisoned".into()))?;
        let transaction = connection
            .transaction_with_behavior(TransactionBehavior::Immediate)
            .map_err(|error| DurableStoreError::Io(error.to_string()))?;
        let current: Option<(String, i64, i64, String)> = transaction
            .query_row(
                "SELECT run_id,revision,schema_version,state_json FROM aikit_durable_runs WHERE run_id=?1",
                params![replacement.run_id()],
                |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?, row.get(3)?)),
            )
            .optional()
            .map_err(|error| DurableStoreError::Io(error.to_string()))?;
        let Some((row_run_id, actual_sql, current_schema, current_json)) = current else {
            return Err(DurableStoreError::NotFound {
                run_id: replacement.run_id().into(),
            });
        };
        let actual = u64::try_from(actual_sql).map_err(|_| {
            DurableStoreError::Invalid("negative durable revision in SQLite".into())
        })?;
        if actual != expected_sequence {
            return Err(DurableStoreError::Conflict {
                run_id: replacement.run_id().into(),
                expected: expected_sequence,
                actual,
            });
        }
        let current = decode_durable_row(
            replacement.run_id(),
            row_run_id,
            actual_sql,
            current_schema,
            current_json,
        )?;
        validate_append_only(&current, replacement)?;

        let updated = transaction
            .execute(
                "UPDATE aikit_durable_runs SET revision=?1,schema_version=?2,state_json=?3 WHERE run_id=?4 AND revision=?5",
                params![replacement_revision, replacement_schema, replacement_json, replacement.run_id(), expected],
            )
            .map_err(|error| DurableStoreError::Io(error.to_string()))?;
        if updated == 1 {
            transaction
                .commit()
                .map_err(|error| DurableStoreError::Io(error.to_string()))?;
            return Ok(());
        }
        Err(DurableStoreError::Conflict {
            run_id: replacement.run_id().into(),
            expected: expected_sequence,
            actual,
        })
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
        let revision = revision_to_sql(session.revision)?;
        let connection = self
            .connection
            .lock()
            .map_err(|_| SessionStoreError::LockPoisoned)?;
        match connection.execute(
            "INSERT INTO aikit_sessions(id,revision,session_json) VALUES(?1,?2,?3)",
            params![session.id, revision, json],
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
        replacement.revision = current
            .revision
            .checked_add(1)
            .ok_or_else(|| io_error("session revision overflow"))?;
        replacement.created_at_unix_ms = current.created_at_unix_ms;
        replacement.updated_at_unix_ms = now_ms().max(current.updated_at_unix_ms);
        let json = serde_json::to_string(&replacement)?;
        let replacement_revision = revision_to_sql(replacement.revision)?;
        let expected_revision_sql = revision_to_sql(expected_revision)?;
        let changed = connection
            .execute(
                "UPDATE aikit_sessions SET revision=?1,session_json=?2 WHERE id=?3 AND revision=?4",
                params![
                    replacement_revision,
                    json,
                    replacement.id,
                    expected_revision_sql
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

    fn acquire_execution_lease(
        &self,
        base: Session,
        owner: &str,
    ) -> SessionStoreResult<SessionExecutionLease> {
        validate_id(&base.id)?;
        validate_lease_owner(owner)?;
        let mut connection = self
            .connection
            .lock()
            .map_err(|_| SessionStoreError::LockPoisoned)?;
        let transaction = connection
            .transaction_with_behavior(TransactionBehavior::Immediate)
            .map_err(|error| io_error(error.to_string()))?;
        let existing_lease: Option<i64> = transaction
            .query_row(
                "SELECT 1 FROM aikit_session_execution_leases WHERE id=?1",
                params![base.id],
                |row| row.get(0),
            )
            .optional()
            .map_err(|error| io_error(error.to_string()))?;
        if existing_lease.is_some() {
            return Err(sqlite_lease_conflict(&base));
        }
        let actual_revision = current_revision(&transaction, &base.id)?;
        if base.revision == 0 {
            if let Some(actual_revision) = actual_revision {
                return Err(SessionStoreError::Conflict {
                    id: base.id,
                    expected_revision: 0,
                    actual_revision,
                });
            }
        } else {
            let actual_revision = actual_revision.ok_or_else(|| SessionStoreError::NotFound {
                id: base.id.clone(),
            })?;
            if actual_revision != base.revision {
                return Err(SessionStoreError::Conflict {
                    id: base.id,
                    expected_revision: base.revision,
                    actual_revision,
                });
            }
        }
        let lease = SessionExecutionLeaseRecord::new(owner)?;
        transaction
            .execute(
                "INSERT INTO aikit_session_execution_leases(id,owner,token,expires_at_unix_ms) VALUES(?1,?2,?3,?4)",
                params![
                    base.id,
                    lease.owner,
                    lease.token,
                    revision_to_sql(lease.expires_at_unix_ms)?
                ],
            )
            .map_err(|error| io_error(error.to_string()))?;
        transaction
            .commit()
            .map_err(|error| io_error(error.to_string()))?;
        Ok(SessionExecutionLease::from_record(base, &lease))
    }

    fn recover_expired_execution_lease(
        &self,
        base: Session,
        recovery_owner: &str,
    ) -> SessionStoreResult<SessionExecutionLease> {
        validate_id(&base.id)?;
        validate_lease_owner(recovery_owner)?;
        let mut connection = self
            .connection
            .lock()
            .map_err(|_| SessionStoreError::LockPoisoned)?;
        let transaction = connection
            .transaction_with_behavior(TransactionBehavior::Immediate)
            .map_err(|error| io_error(error.to_string()))?;
        let existing_lease = sqlite_execution_lease_record(&transaction, &base.id)?
            .ok_or_else(|| sqlite_lease_conflict(&base))?;
        if !existing_lease.is_expired()? {
            return Err(sqlite_lease_conflict(&base));
        }

        let actual_revision = current_revision(&transaction, &base.id)?;
        if base.revision == 0 {
            if let Some(actual_revision) = actual_revision {
                return Err(SessionStoreError::Conflict {
                    id: base.id,
                    expected_revision: 0,
                    actual_revision,
                });
            }
        } else {
            let actual_revision = actual_revision.ok_or_else(|| SessionStoreError::NotFound {
                id: base.id.clone(),
            })?;
            if actual_revision != base.revision {
                return Err(SessionStoreError::Conflict {
                    id: base.id,
                    expected_revision: base.revision,
                    actual_revision,
                });
            }
        }

        let recovered_lease = SessionExecutionLeaseRecord::new(recovery_owner)?;
        transaction
            .execute(
                "UPDATE aikit_session_execution_leases SET owner=?1, token=?2, expires_at_unix_ms=?3 WHERE id=?4",
                params![
                    recovered_lease.owner,
                    recovered_lease.token,
                    revision_to_sql(recovered_lease.expires_at_unix_ms)?,
                    base.id
                ],
            )
            .map_err(|error| io_error(error.to_string()))?;
        transaction
            .commit()
            .map_err(|error| io_error(error.to_string()))?;
        Ok(SessionExecutionLease::from_record(base, &recovered_lease))
    }

    fn clear_expired_execution_lease(&self, base: Session) -> SessionStoreResult<Session> {
        validate_id(&base.id)?;
        let mut connection = self
            .connection
            .lock()
            .map_err(|_| SessionStoreError::LockPoisoned)?;
        let transaction = connection
            .transaction_with_behavior(TransactionBehavior::Immediate)
            .map_err(|error| io_error(error.to_string()))?;
        let existing = sqlite_execution_lease_record(&transaction, &base.id)?
            .ok_or_else(|| sqlite_lease_conflict(&base))?;
        if !existing.is_expired()? {
            return Err(sqlite_lease_conflict(&base));
        }
        let actual_revision = current_revision(&transaction, &base.id)?;
        if base.revision == 0 {
            if let Some(actual_revision) = actual_revision {
                return Err(SessionStoreError::Conflict {
                    id: base.id,
                    expected_revision: 0,
                    actual_revision,
                });
            }
        } else {
            let actual_revision = actual_revision.ok_or_else(|| SessionStoreError::NotFound {
                id: base.id.clone(),
            })?;
            if actual_revision != base.revision {
                return Err(SessionStoreError::Conflict {
                    id: base.id,
                    expected_revision: base.revision,
                    actual_revision,
                });
            }
        }
        let removed = transaction
            .execute(
                "DELETE FROM aikit_session_execution_leases WHERE id=?1 AND token=?2",
                params![base.id, existing.token],
            )
            .map_err(|error| io_error(error.to_string()))?;
        if removed != 1 {
            return Err(sqlite_lease_conflict(&base));
        }
        transaction
            .commit()
            .map_err(|error| io_error(error.to_string()))?;
        Ok(base)
    }

    fn commit_execution_lease(
        &self,
        mut lease: SessionExecutionLease,
    ) -> SessionStoreResult<Session> {
        validate_id(&lease.session.id)?;
        let mut connection = self
            .connection
            .lock()
            .map_err(|_| SessionStoreError::LockPoisoned)?;
        let transaction = connection
            .transaction_with_behavior(TransactionBehavior::Immediate)
            .map_err(|error| io_error(error.to_string()))?;
        let actual = sqlite_execution_lease_record(&transaction, &lease.session.id)?;
        validate_execution_lease_claim(actual.as_ref(), &lease)?;

        let session_id = lease.session.id.clone();
        let lease_token = lease.token.clone();
        let result = (|| -> SessionStoreResult<Session> {
            if lease.session.revision == 0 {
                if let Some(actual_revision) = current_revision(&transaction, &lease.session.id)? {
                    return Err(SessionStoreError::Conflict {
                        id: lease.session.id.clone(),
                        expected_revision: 0,
                        actual_revision,
                    });
                }
                let now = now_ms();
                lease.session.revision = 1;
                lease.session.created_at_unix_ms = now;
                lease.session.updated_at_unix_ms = now;
                let json = serde_json::to_string(&lease.session)?;
                transaction
                    .execute(
                        "INSERT INTO aikit_sessions(id,revision,session_json) VALUES(?1,?2,?3)",
                        params![lease.session.id, 1_i64, json],
                    )
                    .map_err(|error| io_error(error.to_string()))?;
            } else {
                let current_json: Option<String> = transaction
                    .query_row(
                        "SELECT session_json FROM aikit_sessions WHERE id=?1",
                        params![lease.session.id],
                        |row| row.get(0),
                    )
                    .optional()
                    .map_err(|error| io_error(error.to_string()))?;
                let current: Session = current_json
                    .map(|json| serde_json::from_str(&json))
                    .transpose()?
                    .ok_or_else(|| SessionStoreError::NotFound {
                        id: lease.session.id.clone(),
                    })?;
                if current.revision != lease.session.revision {
                    return Err(SessionStoreError::Conflict {
                        id: lease.session.id.clone(),
                        expected_revision: lease.session.revision,
                        actual_revision: current.revision,
                    });
                }
                lease.session.revision = current
                    .revision
                    .checked_add(1)
                    .ok_or_else(|| io_error("session revision overflow"))?;
                lease.session.created_at_unix_ms = current.created_at_unix_ms;
                lease.session.updated_at_unix_ms = now_ms().max(current.updated_at_unix_ms);
                let json = serde_json::to_string(&lease.session)?;
                let changed = transaction
                    .execute(
                        "UPDATE aikit_sessions SET revision=?1,session_json=?2 WHERE id=?3 AND revision=?4",
                        params![
                            revision_to_sql(lease.session.revision)?,
                            json,
                            lease.session.id,
                            revision_to_sql(current.revision)?
                        ],
                    )
                    .map_err(|error| io_error(error.to_string()))?;
                if changed == 0 {
                    return Err(SessionStoreError::Conflict {
                        id: lease.session.id.clone(),
                        expected_revision: current.revision,
                        actual_revision: current_revision(&transaction, &current.id)?.unwrap_or(0),
                    });
                }
            }
            Ok(lease.session)
        })();
        if matches!(
            &result,
            Ok(_) | Err(SessionStoreError::Conflict { .. } | SessionStoreError::NotFound { .. })
        ) {
            transaction
                .execute(
                    "DELETE FROM aikit_session_execution_leases WHERE id=?1 AND token=?2",
                    params![session_id, lease_token],
                )
                .map_err(|error| io_error(error.to_string()))?;
            transaction
                .commit()
                .map_err(|error| io_error(error.to_string()))?;
        }
        result
    }

    fn release_execution_lease(&self, lease: SessionExecutionLease) -> SessionStoreResult<Session> {
        validate_id(&lease.session.id)?;
        let mut connection = self
            .connection
            .lock()
            .map_err(|_| SessionStoreError::LockPoisoned)?;
        let transaction = connection
            .transaction_with_behavior(TransactionBehavior::Immediate)
            .map_err(|error| io_error(error.to_string()))?;
        let actual = sqlite_execution_lease_record(&transaction, &lease.session.id)?;
        validate_execution_lease_claim(actual.as_ref(), &lease)?;
        let removed = transaction
            .execute(
                "DELETE FROM aikit_session_execution_leases WHERE id=?1 AND token=?2",
                params![lease.session.id, lease.token],
            )
            .map_err(|error| io_error(error.to_string()))?;
        if removed != 1 {
            return Err(sqlite_lease_conflict(&lease.session));
        }
        transaction
            .commit()
            .map_err(|error| io_error(error.to_string()))?;
        Ok(lease.session)
    }
}

fn sqlite_execution_lease_record(
    connection: &Connection,
    id: &str,
) -> SessionStoreResult<Option<SessionExecutionLeaseRecord>> {
    let stored: Option<(String, Option<String>, i64)> = connection
        .query_row(
            "SELECT owner,token,expires_at_unix_ms FROM aikit_session_execution_leases WHERE id=?1",
            params![id],
            |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?)),
        )
        .optional()
        .map_err(|error| io_error(error.to_string()))?;
    stored
        .map(|(owner, token, expires_at_unix_ms)| {
            let token = token.ok_or_else(|| SessionStoreError::Serialization {
                message: "stored SQLite execution lease has no fencing token".into(),
            })?;
            let expires_at_unix_ms = u64::try_from(expires_at_unix_ms).map_err(|_| {
                SessionStoreError::Serialization {
                    message: "stored SQLite execution lease has an invalid expiration".into(),
                }
            })?;
            let record = SessionExecutionLeaseRecord {
                owner,
                token,
                expires_at_unix_ms,
            };
            validate_stored_execution_lease(&record)?;
            Ok(record)
        })
        .transpose()
}

fn current_revision(connection: &Connection, id: &str) -> SessionStoreResult<Option<u64>> {
    let revision: Option<i64> = connection
        .query_row(
            "SELECT revision FROM aikit_sessions WHERE id=?1",
            params![id],
            |row| row.get(0),
        )
        .optional()
        .map_err(|e| io_error(e.to_string()))?;
    revision
        .map(|value| {
            u64::try_from(value).map_err(|_| io_error("database contains a negative revision"))
        })
        .transpose()
}

fn revision_to_sql(revision: u64) -> SessionStoreResult<i64> {
    i64::try_from(revision).map_err(|_| io_error("session revision exceeds SQLite INTEGER range"))
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

fn validate_lease_owner(owner: &str) -> SessionStoreResult<()> {
    if owner.trim().is_empty() || owner.chars().any(char::is_control) {
        Err(SessionStoreError::InvalidId {
            reason: "execution lease owner must be non-empty and contain no control characters"
                .into(),
        })
    } else {
        Ok(())
    }
}

fn sqlite_lease_conflict(session: &Session) -> SessionStoreError {
    SessionStoreError::Conflict {
        id: session.id.clone(),
        expected_revision: session.revision,
        actual_revision: session.revision.saturating_add(1),
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
    fn sqlite_memory_cas_and_plane_filter_are_atomic() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("memory-cas.db");
        let store = SqliteMemoryStore::open(&path).unwrap();
        let entry = MemoryEntry::new("agent", "fact", json!("verified"))
            .with_plane(crate::memory::MemoryPlane::Semantic);
        assert_eq!(store.compare_and_swap(entry.clone(), 0), Ok(1));
        assert!(store.compare_and_swap(entry, 0).is_err());
        let results = store
            .search(
                &MemoryQuery::new("agent", "", 10).in_plane(crate::memory::MemoryPlane::Semantic),
            )
            .unwrap();
        assert_eq!(results.len(), 1);
        assert_eq!(results[0].revision, 1);
    }

    #[test]
    fn sqlite_durable_store_enforces_cross_instance_event_cas() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("durable.db");
        let first = SqliteDurableStore::open(&path).unwrap();
        let second = SqliteDurableStore::open(&path).unwrap();
        let initial = crate::durability::RunState::new(
            "session-1",
            "run-1",
            crate::durability::DurabilityMode::Sync,
        )
        .unwrap();
        first.create(&initial).unwrap();
        let expected = initial.events().last().unwrap().sequence;

        let mut winner = initial.clone();
        winner
            .replace_state("winner", json!({"worker": "first"}))
            .unwrap();
        first.compare_and_swap(expected, &winner).unwrap();

        let mut stale = initial;
        stale
            .replace_state("stale", json!({"worker": "second"}))
            .unwrap();
        assert!(matches!(
            second.compare_and_swap(expected, &stale),
            Err(DurableStoreError::Conflict { .. })
        ));
        assert_eq!(second.load("run-1").unwrap(), winner);
    }

    #[test]
    fn sqlite_durable_store_rejects_same_revision_divergent_history() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("durable-divergent.db");
        let store = SqliteDurableStore::open(&path).unwrap();
        let initial = crate::durability::RunState::new(
            "session-1",
            "run-1",
            crate::durability::DurabilityMode::Sync,
        )
        .unwrap();
        store.create(&initial).unwrap();

        let mut committed = initial.clone();
        committed
            .replace_state("committed", json!({"worker": "first"}))
            .unwrap();
        store
            .compare_and_swap(durable_revision(&initial), &committed)
            .unwrap();

        let mut divergent = initial;
        divergent
            .replace_state("divergent", json!({"worker": "second"}))
            .unwrap();
        assert!(matches!(
            store.compare_and_swap(durable_revision(&committed), &divergent),
            Err(DurableStoreError::Invalid(_))
        ));
        assert_eq!(store.load("run-1").unwrap(), committed);
    }

    #[test]
    fn sqlite_durable_load_rejects_serialized_run_id_corruption() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("durable-run-id-corruption.db");
        let store = SqliteDurableStore::open(&path).unwrap();
        let initial = crate::durability::RunState::new(
            "session-1",
            "run-1",
            crate::durability::DurabilityMode::Sync,
        )
        .unwrap();
        store.create(&initial).unwrap();
        let other = crate::durability::RunState::new(
            "session-1",
            "run-2",
            crate::durability::DurabilityMode::Sync,
        )
        .unwrap();
        let other_json = serde_json::to_string(&other).unwrap();
        store
            .connection
            .lock()
            .unwrap()
            .execute(
                "UPDATE aikit_durable_runs SET state_json=?1 WHERE run_id=?2",
                params![other_json, initial.run_id()],
            )
            .unwrap();

        assert_eq!(
            store.load(initial.run_id()).unwrap_err(),
            DurableStoreError::Invalid("SQLite row key does not match serialized run ID".into())
        );
    }

    #[test]
    fn sqlite_durable_load_rejects_persisted_revision_corruption() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("durable-revision-corruption.db");
        let store = SqliteDurableStore::open(&path).unwrap();
        let initial = crate::durability::RunState::new(
            "session-1",
            "run-1",
            crate::durability::DurabilityMode::Sync,
        )
        .unwrap();
        store.create(&initial).unwrap();
        store
            .connection
            .lock()
            .unwrap()
            .execute(
                "UPDATE aikit_durable_runs SET revision=revision+1 WHERE run_id=?1",
                params![initial.run_id()],
            )
            .unwrap();

        assert_eq!(
            store.load(initial.run_id()).unwrap_err(),
            DurableStoreError::Invalid(
                "SQLite revision does not match serialized event log".into()
            )
        );
    }

    #[test]
    fn sqlite_durable_load_rejects_persisted_schema_corruption() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("durable-schema-corruption.db");
        let store = SqliteDurableStore::open(&path).unwrap();
        let initial = crate::durability::RunState::new(
            "session-1",
            "run-1",
            crate::durability::DurabilityMode::Sync,
        )
        .unwrap();
        store.create(&initial).unwrap();
        store
            .connection
            .lock()
            .unwrap()
            .execute(
                "UPDATE aikit_durable_runs SET schema_version=?1 WHERE run_id=?2",
                params![i64::from(DURABILITY_SCHEMA_VERSION) + 1, initial.run_id()],
            )
            .unwrap();

        assert_eq!(
            store.load(initial.run_id()).unwrap_err(),
            DurableStoreError::Invalid(format!(
                "unsupported SQLite durability schema {}; expected {}",
                DURABILITY_SCHEMA_VERSION + 1,
                DURABILITY_SCHEMA_VERSION
            ))
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

    #[test]
    fn sqlite_revision_conversion_rejects_values_outside_integer_range() {
        assert_eq!(revision_to_sql(i64::MAX as u64).unwrap(), i64::MAX);
        assert!(revision_to_sql(i64::MAX as u64 + 1).is_err());
    }

    #[test]
    fn sqlite_rejects_negative_persisted_revisions() {
        let connection = Connection::open_in_memory().unwrap();
        connection.execute_batch(SCHEMA).unwrap();
        connection
            .execute(
                "INSERT INTO aikit_sessions(id,revision,session_json) VALUES(?1,?2,?3)",
                params!["bad", -1_i64, "{}"],
            )
            .unwrap();
        assert!(current_revision(&connection, "bad").is_err());
    }

    #[cfg(unix)]
    #[test]
    fn sqlite_rejects_a_final_path_symlink() {
        use std::os::unix::fs::symlink;

        let dir = tempfile::tempdir().unwrap();
        let target = dir.path().join("target.db");
        let alias = dir.path().join("alias.db");
        fs::write(&target, b"must remain untouched").unwrap();
        symlink(&target, &alias).unwrap();

        let memory_error = SqliteMemoryStore::open(&alias).err().unwrap();
        assert!(memory_error.contains("symlink"), "{memory_error}");
        let session_error = SqliteSessionStore::open(&alias).err().unwrap();
        assert!(session_error.to_string().contains("symlink"));
        assert_eq!(fs::read(&target).unwrap(), b"must remain untouched");
        assert!(database_open_flags().contains(OpenFlags::SQLITE_OPEN_NOFOLLOW));
        let sqlite_path = sqlite_nofollow_path(&alias).unwrap();
        assert_eq!(sqlite_path.file_name(), alias.file_name());
        assert!(fs::symlink_metadata(sqlite_path)
            .unwrap()
            .file_type()
            .is_symlink());
    }

    #[cfg(unix)]
    #[test]
    fn sqlite_tightens_existing_database_permissions_to_owner_only() {
        use std::os::unix::fs::PermissionsExt;

        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("permissions.db");
        fs::write(&path, []).unwrap();
        fs::set_permissions(&path, fs::Permissions::from_mode(0o666)).unwrap();

        let _store = SqliteSessionStore::open(&path).unwrap();

        assert_eq!(
            fs::metadata(&path).unwrap().permissions().mode() & 0o777,
            0o600
        );
    }

    #[test]
    fn sqlite_expired_execution_lease_can_be_recovered_without_revision_drift() {
        let directory = tempfile::tempdir().unwrap();
        let path = directory.path().join("lease-recovery.db");
        let first = SqliteSessionStore::open(&path).unwrap();
        let base = Session::new("recoverable", Vec::new());
        let mut stale = first
            .acquire_execution_lease(base.clone(), "crashed-owner")
            .unwrap();
        Connection::open(&path)
            .unwrap()
            .execute(
                "UPDATE aikit_session_execution_leases SET expires_at_unix_ms=0 WHERE id=?1",
                params![base.id],
            )
            .unwrap();

        let second = SqliteSessionStore::open(&path).unwrap();
        assert!(matches!(
            second.acquire_execution_lease(base.clone(), "automatic-retry"),
            Err(SessionStoreError::Conflict { .. })
        ));
        let mut recovered = second
            .recover_expired_execution_lease(base, "crashed-owner")
            .unwrap();
        assert_ne!(stale.token, recovered.token);
        assert_eq!(recovered.session().revision, 0);
        stale
            .session_mut()
            .messages
            .push(Message::user("unsafe stale retry"));
        recovered
            .session_mut()
            .messages
            .push(Message::user("safe retry"));

        assert!(matches!(
            first.commit_execution_lease(stale),
            Err(SessionStoreError::Conflict { .. })
        ));
        let saved = second.commit_execution_lease(recovered).unwrap();
        assert_eq!(saved.revision, 1);
        assert_eq!(second.load_session("recoverable").unwrap(), saved);
    }

    #[test]
    fn sqlite_malformed_expiration_blocks_normal_and_explicit_recovery() {
        let directory = tempfile::tempdir().unwrap();
        let path = directory.path().join("malformed-lease.db");
        let store = SqliteSessionStore::open(&path).unwrap();
        let base = Session::new("malformed-lease", Vec::new());
        store
            .acquire_execution_lease(base.clone(), "crashed-owner")
            .unwrap();
        Connection::open(&path)
            .unwrap()
            .execute(
                "UPDATE aikit_session_execution_leases SET expires_at_unix_ms=-1 WHERE id=?1",
                params![base.id],
            )
            .unwrap();

        assert!(matches!(
            store.acquire_execution_lease(base.clone(), "automatic-retry"),
            Err(SessionStoreError::Conflict { .. })
        ));
        assert!(matches!(
            store.recover_expired_execution_lease(base, "manual-recovery"),
            Err(SessionStoreError::Serialization { .. })
        ));
    }

    #[test]
    fn sqlite_legacy_lease_without_fencing_token_fails_closed() {
        let directory = tempfile::tempdir().unwrap();
        let path = directory.path().join("legacy-lease.db");
        let connection = Connection::open(&path).unwrap();
        connection
            .execute_batch(
                "CREATE TABLE aikit_session_execution_leases (
                    id TEXT PRIMARY KEY,
                    owner TEXT NOT NULL,
                    expires_at_unix_ms INTEGER NOT NULL
                );
                INSERT INTO aikit_session_execution_leases(id,owner,expires_at_unix_ms)
                VALUES('legacy','old-worker',0);",
            )
            .unwrap();
        drop(connection);

        let store = SqliteSessionStore::open(&path).unwrap();
        let base = Session::new("legacy", Vec::new());
        assert!(matches!(
            store.acquire_execution_lease(base.clone(), "automatic-retry"),
            Err(SessionStoreError::Conflict { .. })
        ));
        assert!(matches!(
            store.recover_expired_execution_lease(base.clone(), "manual-recovery"),
            Err(SessionStoreError::Serialization { .. })
        ));
        assert!(matches!(
            store.clear_expired_execution_lease(base),
            Err(SessionStoreError::Serialization { .. })
        ));
    }

    #[test]
    fn sqlite_atomic_clear_removes_only_an_expired_lease_without_revision_drift() {
        let directory = tempfile::tempdir().unwrap();
        let path = directory.path().join("atomic-clear.db");
        let store = SqliteSessionStore::open(&path).unwrap();
        let base = Session::new("atomic-clear", Vec::new());
        store
            .acquire_execution_lease(base.clone(), "crashed-owner")
            .unwrap();
        Connection::open(&path)
            .unwrap()
            .execute(
                "UPDATE aikit_session_execution_leases SET expires_at_unix_ms=0 WHERE id=?1",
                params![base.id],
            )
            .unwrap();

        let cleared = store.clear_expired_execution_lease(base.clone()).unwrap();
        assert_eq!(cleared.revision, 0);
        assert!(
            current_revision(&store.connection.lock().unwrap(), &base.id)
                .unwrap()
                .is_none()
        );
        let reacquired = store.acquire_execution_lease(base, "new-worker").unwrap();
        assert_eq!(reacquired.session().revision, 0);
    }

    #[cfg(windows)]
    #[test]
    fn windows_sqlite_file_identity_distinguishes_open_files() {
        let directory = tempfile::tempdir().unwrap();
        let first_path = directory.path().join("first.db");
        let second_path = directory.path().join("second.db");
        fs::write(&first_path, b"one").unwrap();
        fs::write(&second_path, b"two").unwrap();
        let first = File::open(&first_path).unwrap();
        let first_again = File::open(&first_path).unwrap();
        let second = File::open(&second_path).unwrap();

        assert!(same_open_file(&first, &first_again).unwrap());
        assert!(!same_open_file(&first, &second).unwrap());
    }

    #[test]
    fn sqlite_terminal_revision_conflict_releases_matching_execution_lease() {
        let directory = tempfile::tempdir().unwrap();
        let path = directory.path().join("lease-conflict.db");
        let store = SqliteSessionStore::open(&path).unwrap();
        let created = store
            .create_session(Session::new("conflicted", vec![Message::user("one")]))
            .unwrap();
        let mut leased = store
            .acquire_execution_lease(created.clone(), "first-owner")
            .unwrap();
        leased
            .session_mut()
            .messages
            .push(Message::user("first result"));

        let mut external = created.clone();
        external.messages.push(Message::user("external winner"));
        let external = store.compare_and_swap(created.revision, external).unwrap();
        assert!(matches!(
            store.commit_execution_lease(leased),
            Err(SessionStoreError::Conflict { .. })
        ));

        let recovered = store
            .acquire_execution_lease(external, "second-owner")
            .expect("known terminal conflict must release the matching owner lease");
        assert_eq!(recovered.session().revision, 2);
    }
}
