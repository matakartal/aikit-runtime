//! Canonical run recording and resumable sessions.
//!
//! A stream alone is not a session: reconstructing history from final text drops reasoning items,
//! tool calls, and tool results. [`RunRecorder`] keeps the exact canonical messages the runtime
//! appends. [`SessionStore`] persists those messages behind optimistic compare-and-swap semantics
//! so concurrent resumptions cannot silently overwrite each other.

use crate::types::{Message, ProviderMetadata, Usage};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::collections::{BTreeMap, HashMap};
use std::ffi::OsString;
use std::fmt;
use std::fs::{self, File, OpenOptions};
use std::io::{BufReader, BufWriter, Write};
use std::path::{Component, Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex, OnceLock, Weak};
use std::time::{SystemTime, UNIX_EPOCH};

const SESSION_FILE_VERSION: u32 = 1;

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum RunTerminalStatus {
    Running,
    Completed,
    Failed,
    BudgetExceeded,
    MaxTurns,
    Cancelled,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct RunOutcome {
    pub messages: Vec<Message>,
    pub usage: Usage,
    /// Ordered response-level metadata, grouped by provider. Empty for older/custom providers
    /// that do not emit metadata deltas. This is raw, potentially sensitive provider output
    /// (including generated tokens or grounding queries) and is persisted by session stores.
    #[serde(default, skip_serializing_if = "ProviderMetadata::is_empty")]
    pub provider_metadata: ProviderMetadata,
    pub terminal_status: RunTerminalStatus,
    pub stop_reason: Option<String>,
    pub model_attempts: Vec<String>,
    /// Convenience projection only. Canonical assistant content remains in `messages`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub final_text: Option<String>,
}

impl Default for RunOutcome {
    fn default() -> Self {
        RunOutcome {
            messages: Vec::new(),
            usage: Usage::default(),
            provider_metadata: ProviderMetadata::new(),
            terminal_status: RunTerminalStatus::Running,
            stop_reason: None,
            model_attempts: Vec::new(),
            final_text: None,
        }
    }
}

#[derive(Debug, Clone, Default)]
pub struct RunRecorder {
    inner: Arc<Mutex<RunOutcome>>,
}

impl RunRecorder {
    pub fn begin(&self, messages: Vec<Message>) {
        let mut outcome = self.inner.lock().expect("run recorder mutex poisoned");
        *outcome = RunOutcome {
            messages,
            ..RunOutcome::default()
        };
    }

    pub fn record_model_attempt(&self, model: impl Into<String>) {
        self.inner
            .lock()
            .expect("run recorder mutex poisoned")
            .model_attempts
            .push(model.into());
    }

    pub fn append_message(&self, message: Message) {
        self.inner
            .lock()
            .expect("run recorder mutex poisoned")
            .messages
            .push(message);
    }

    /// Preserve one potentially sensitive provider-native metadata object in stream order.
    /// Runtime calls this for every `StreamDelta::ProviderMetadata`; no merge/normalization can
    /// erase native fields.
    pub fn record_provider_metadata(&self, provider: impl Into<String>, metadata: Value) {
        self.inner
            .lock()
            .expect("run recorder mutex poisoned")
            .provider_metadata
            .entry(provider.into())
            .or_default()
            .push(metadata);
    }

    /// Stores a convenient terminal text projection without flattening canonical message history.
    pub fn set_final_text(&self, final_text: impl Into<String>) {
        self.inner
            .lock()
            .expect("run recorder mutex poisoned")
            .final_text = Some(final_text.into());
    }

    pub fn complete(
        &self,
        usage: Usage,
        status: RunTerminalStatus,
        stop_reason: impl Into<String>,
    ) {
        let mut outcome = self.inner.lock().expect("run recorder mutex poisoned");
        outcome.usage = usage;
        outcome.terminal_status = status;
        outcome.stop_reason = Some(stop_reason.into());
    }

    pub fn outcome(&self) -> RunOutcome {
        self.inner
            .lock()
            .expect("run recorder mutex poisoned")
            .clone()
    }
}

/// A resumable canonical conversation.
///
/// `revision == 0` represents a not-yet-created record. Stores assign revision 1 on creation and
/// increment it on every successful compare-and-swap. Timestamps are assigned by the store.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct Session {
    pub id: String,
    pub revision: u64,
    pub messages: Vec<Message>,
    #[serde(default)]
    pub metadata: BTreeMap<String, Value>,
    pub created_at_unix_ms: u64,
    pub updated_at_unix_ms: u64,
    /// Optional run details retained for compatibility with the original outcome store API.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub outcome: Option<RunOutcome>,
}

impl Session {
    pub fn new(id: impl Into<String>, messages: Vec<Message>) -> Self {
        Self {
            id: id.into(),
            revision: 0,
            messages,
            metadata: BTreeMap::new(),
            created_at_unix_ms: 0,
            updated_at_unix_ms: 0,
            outcome: None,
        }
    }

    pub fn with_metadata(mut self, key: impl Into<String>, value: Value) -> Self {
        self.metadata.insert(key.into(), value);
        self
    }

    fn from_outcome(id: impl Into<String>, outcome: RunOutcome) -> Self {
        Self {
            messages: outcome.messages.clone(),
            outcome: Some(outcome),
            ..Self::new(id, Vec::new())
        }
    }

    fn canonical_outcome(&self) -> RunOutcome {
        let mut outcome = self.outcome.clone().unwrap_or_default();
        outcome.messages.clone_from(&self.messages);
        outcome
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum SessionStoreError {
    NotFound {
        id: String,
    },
    Conflict {
        id: String,
        expected_revision: u64,
        actual_revision: u64,
    },
    InvalidId {
        reason: String,
    },
    Io {
        message: String,
    },
    Serialization {
        message: String,
    },
    LockPoisoned,
}

impl fmt::Display for SessionStoreError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            SessionStoreError::NotFound { id } => write!(f, "session `{id}` was not found"),
            SessionStoreError::Conflict {
                id,
                expected_revision,
                actual_revision,
            } => write!(
                f,
                "session `{id}` revision conflict: expected {expected_revision}, actual {actual_revision}"
            ),
            SessionStoreError::InvalidId { reason } => write!(f, "invalid session id: {reason}"),
            SessionStoreError::Io { message } => write!(f, "session store I/O error: {message}"),
            SessionStoreError::Serialization { message } => {
                write!(f, "session store serialization error: {message}")
            }
            SessionStoreError::LockPoisoned => write!(f, "session store lock poisoned"),
        }
    }
}

impl std::error::Error for SessionStoreError {}

impl From<SessionStoreError> for crate::error::AikitError {
    fn from(error: SessionStoreError) -> Self {
        match error {
            SessionStoreError::Conflict {
                id,
                expected_revision,
                actual_revision,
            } => crate::error::AikitError::Conflict(format!(
                "session `{id}` revision conflict: expected {expected_revision}, actual {actual_revision}"
            )),
            other => crate::error::AikitError::Session(other.to_string()),
        }
    }
}

impl From<std::io::Error> for SessionStoreError {
    fn from(error: std::io::Error) -> Self {
        Self::Io {
            message: error.to_string(),
        }
    }
}

impl From<serde_json::Error> for SessionStoreError {
    fn from(error: serde_json::Error) -> Self {
        Self::Serialization {
            message: error.to_string(),
        }
    }
}

pub type SessionStoreResult<T> = std::result::Result<T, SessionStoreError>;

/// Resumable session persistence with optimistic concurrency.
///
/// The original `load`/`save` outcome methods remain as compatibility adapters. New callers should
/// use `create_session`, `load_session`, and `compare_and_swap` so revision conflicts are explicit.
pub trait SessionStore: Send + Sync {
    fn create_session(&self, session: Session) -> SessionStoreResult<Session>;

    fn load_session(&self, session_id: &str) -> SessionStoreResult<Session>;

    fn compare_and_swap(
        &self,
        expected_revision: u64,
        replacement: Session,
    ) -> SessionStoreResult<Session>;

    fn load(&self, session_id: &str) -> std::result::Result<Option<RunOutcome>, String> {
        match self.load_session(session_id) {
            Ok(session) => Ok(Some(session.canonical_outcome())),
            Err(SessionStoreError::NotFound { .. }) => Ok(None),
            Err(error) => Err(error.to_string()),
        }
    }

    fn save(&self, session_id: &str, outcome: &RunOutcome) -> std::result::Result<(), String> {
        loop {
            match self.load_session(session_id) {
                Ok(mut current) => {
                    current.messages.clone_from(&outcome.messages);
                    current.outcome = Some(outcome.clone());
                    match self.compare_and_swap(current.revision, current) {
                        Ok(_) => return Ok(()),
                        Err(SessionStoreError::Conflict { .. }) => continue,
                        Err(error) => return Err(error.to_string()),
                    }
                }
                Err(SessionStoreError::NotFound { .. }) => {
                    match self.create_session(Session::from_outcome(session_id, outcome.clone())) {
                        Ok(_) => return Ok(()),
                        Err(SessionStoreError::Conflict { .. }) => continue,
                        Err(error) => return Err(error.to_string()),
                    }
                }
                Err(error) => return Err(error.to_string()),
            }
        }
    }
}

#[derive(Default)]
pub struct InMemorySessionStore {
    sessions: Mutex<HashMap<String, Session>>,
}

impl SessionStore for InMemorySessionStore {
    fn create_session(&self, mut session: Session) -> SessionStoreResult<Session> {
        validate_session_id(&session.id)?;
        let mut sessions = self
            .sessions
            .lock()
            .map_err(|_| SessionStoreError::LockPoisoned)?;
        if let Some(current) = sessions.get(&session.id) {
            return Err(SessionStoreError::Conflict {
                id: session.id,
                expected_revision: 0,
                actual_revision: current.revision,
            });
        }

        initialize_created_session(&mut session);
        sessions.insert(session.id.clone(), session.clone());
        Ok(session)
    }

    fn load_session(&self, session_id: &str) -> SessionStoreResult<Session> {
        validate_session_id(session_id)?;
        self.sessions
            .lock()
            .map_err(|_| SessionStoreError::LockPoisoned)?
            .get(session_id)
            .cloned()
            .ok_or_else(|| SessionStoreError::NotFound {
                id: session_id.to_string(),
            })
    }

    fn compare_and_swap(
        &self,
        expected_revision: u64,
        mut replacement: Session,
    ) -> SessionStoreResult<Session> {
        validate_session_id(&replacement.id)?;
        let mut sessions = self
            .sessions
            .lock()
            .map_err(|_| SessionStoreError::LockPoisoned)?;
        let current =
            sessions
                .get(&replacement.id)
                .cloned()
                .ok_or_else(|| SessionStoreError::NotFound {
                    id: replacement.id.clone(),
                })?;
        validate_revision(&replacement.id, expected_revision, current.revision)?;

        prepare_replacement(&current, &mut replacement);
        sessions.insert(replacement.id.clone(), replacement.clone());
        Ok(replacement)
    }
}

/// A single-file JSON session database.
///
/// Mutations serialize under a process-local lock shared by all store instances whose paths resolve
/// to the same canonical parent and file name, then write a mode-0600 temporary file in the
/// destination directory and atomically rename it. The final target is never opened through a
/// symlink and must be a regular file. This prevents torn files and lost updates inside one process.
/// Multi-process callers should use a database-backed implementation with a real cross-process CAS
/// transaction.
pub struct JsonFileSessionStore {
    path: PathBuf,
    lock: Arc<Mutex<()>>,
}

impl JsonFileSessionStore {
    pub fn new(path: impl Into<PathBuf>) -> Self {
        let path = normalize_path(path.into());
        Self {
            lock: shared_file_lock(&path),
            path,
        }
    }

    pub fn path(&self) -> &Path {
        &self.path
    }

    fn read_database(&self) -> SessionStoreResult<SessionFile> {
        let Some(file) = open_existing_session_file(&self.path)? else {
            return Ok(SessionFile::default());
        };
        let reader = BufReader::new(file);
        let database: SessionFile = serde_json::from_reader(reader)?;
        if database.version != SESSION_FILE_VERSION {
            return Err(SessionStoreError::Serialization {
                message: format!(
                    "unsupported session file version {}; expected {SESSION_FILE_VERSION}",
                    database.version
                ),
            });
        }
        Ok(database)
    }

    fn write_database(&self, database: &SessionFile) -> SessionStoreResult<()> {
        let parent = self.path.parent().unwrap_or_else(|| Path::new("."));
        fs::create_dir_all(parent)?;

        // Capture whether a regular destination exists before preparing the replacement. A target
        // that appears or disappears while the temporary file is written indicates an unsupported
        // external writer; fail closed rather than replacing an unexpected path entry.
        let target_before = open_existing_session_file(&self.path)?;

        let mut temporary_path;
        let file = loop {
            let nonce = TEMP_FILE_NONCE.fetch_add(1, Ordering::Relaxed);
            let file_name = self
                .path
                .file_name()
                .and_then(|name| name.to_str())
                .unwrap_or("sessions.json");
            temporary_path =
                parent.join(format!(".{file_name}.{}.{}.tmp", std::process::id(), nonce));

            let mut options = OpenOptions::new();
            options.write(true).create_new(true);
            #[cfg(unix)]
            {
                use std::os::unix::fs::OpenOptionsExt;
                options.mode(0o600);
            }
            match options.open(&temporary_path) {
                Ok(file) => {
                    if let Err(error) = ensure_regular_file(&file, "session temporary file")
                        .and_then(|()| tighten_owner_only_permissions(&file))
                    {
                        drop(file);
                        let _ = fs::remove_file(&temporary_path);
                        return Err(error);
                    }
                    break file;
                }
                Err(error) if error.kind() == std::io::ErrorKind::AlreadyExists => continue,
                Err(error) => return Err(error.into()),
            }
        };

        let result = (|| -> SessionStoreResult<()> {
            let mut writer = BufWriter::new(file);
            serde_json::to_writer_pretty(&mut writer, database)?;
            writer.write_all(b"\n")?;
            writer.flush()?;
            writer.get_ref().sync_all()?;
            drop(writer);

            let target_after = open_existing_session_file(&self.path)?;
            let replace_existing = match (&target_before, &target_after) {
                (None, None) => false,
                (Some(before), Some(after)) if same_open_file(before, after)? => true,
                _ => {
                    return Err(SessionStoreError::Io {
                        message: format!(
                            "session file {} changed while an update was being prepared",
                            self.path.display()
                        ),
                    });
                }
            };
            drop(target_after);
            drop(target_before);
            if replace_existing {
                // Atomic rename replaces the exact regular file checked above and never follows a
                // final symlink.
                fs::rename(&temporary_path, &self.path)?;
            } else {
                // Unlike rename, hard-link creation never replaces a path entry that appeared
                // after the last check. Both names are in the same directory/filesystem.
                fs::hard_link(&temporary_path, &self.path)?;
                fs::remove_file(&temporary_path)?;
            }
            let installed =
                open_existing_session_file(&self.path)?.ok_or_else(|| SessionStoreError::Io {
                    message: format!(
                        "session file {} disappeared after atomic replacement",
                        self.path.display()
                    ),
                })?;
            ensure_regular_file(&installed, "session file")?;
            sync_parent_directory(parent)?;
            Ok(())
        })();

        if result.is_err() {
            let _ = fs::remove_file(&temporary_path);
        }
        result
    }
}

impl SessionStore for JsonFileSessionStore {
    fn create_session(&self, mut session: Session) -> SessionStoreResult<Session> {
        validate_session_id(&session.id)?;
        let _guard = self
            .lock
            .lock()
            .map_err(|_| SessionStoreError::LockPoisoned)?;
        let mut database = self.read_database()?;
        if let Some(current) = database.sessions.get(&session.id) {
            return Err(SessionStoreError::Conflict {
                id: session.id,
                expected_revision: 0,
                actual_revision: current.revision,
            });
        }

        initialize_created_session(&mut session);
        database
            .sessions
            .insert(session.id.clone(), session.clone());
        self.write_database(&database)?;
        Ok(session)
    }

    fn load_session(&self, session_id: &str) -> SessionStoreResult<Session> {
        validate_session_id(session_id)?;
        let _guard = self
            .lock
            .lock()
            .map_err(|_| SessionStoreError::LockPoisoned)?;
        self.read_database()?
            .sessions
            .get(session_id)
            .cloned()
            .ok_or_else(|| SessionStoreError::NotFound {
                id: session_id.to_string(),
            })
    }

    fn compare_and_swap(
        &self,
        expected_revision: u64,
        mut replacement: Session,
    ) -> SessionStoreResult<Session> {
        validate_session_id(&replacement.id)?;
        let _guard = self
            .lock
            .lock()
            .map_err(|_| SessionStoreError::LockPoisoned)?;
        let mut database = self.read_database()?;
        let current = database
            .sessions
            .get(&replacement.id)
            .cloned()
            .ok_or_else(|| SessionStoreError::NotFound {
                id: replacement.id.clone(),
            })?;
        validate_revision(&replacement.id, expected_revision, current.revision)?;

        prepare_replacement(&current, &mut replacement);
        database
            .sessions
            .insert(replacement.id.clone(), replacement.clone());
        self.write_database(&database)?;
        Ok(replacement)
    }
}

#[derive(Debug, Serialize, Deserialize)]
struct SessionFile {
    version: u32,
    sessions: BTreeMap<String, Session>,
}

impl Default for SessionFile {
    fn default() -> Self {
        Self {
            version: SESSION_FILE_VERSION,
            sessions: BTreeMap::new(),
        }
    }
}

fn validate_session_id(session_id: &str) -> SessionStoreResult<()> {
    if session_id.trim().is_empty() {
        return Err(SessionStoreError::InvalidId {
            reason: "id cannot be empty".to_string(),
        });
    }
    if session_id.chars().any(char::is_control) {
        return Err(SessionStoreError::InvalidId {
            reason: "id cannot contain control characters".to_string(),
        });
    }
    Ok(())
}

fn validate_revision(
    session_id: &str,
    expected_revision: u64,
    actual_revision: u64,
) -> SessionStoreResult<()> {
    if expected_revision == actual_revision {
        Ok(())
    } else {
        Err(SessionStoreError::Conflict {
            id: session_id.to_string(),
            expected_revision,
            actual_revision,
        })
    }
}

fn initialize_created_session(session: &mut Session) {
    let now = now_unix_ms();
    session.revision = 1;
    session.created_at_unix_ms = now;
    session.updated_at_unix_ms = now;
    synchronize_outcome_messages(session);
}

fn prepare_replacement(current: &Session, replacement: &mut Session) {
    replacement.revision = current.revision.saturating_add(1);
    replacement.created_at_unix_ms = current.created_at_unix_ms;
    replacement.updated_at_unix_ms =
        now_unix_ms().max(current.updated_at_unix_ms.saturating_add(1));
    synchronize_outcome_messages(replacement);
}

fn synchronize_outcome_messages(session: &mut Session) {
    if let Some(outcome) = &mut session.outcome {
        outcome.messages.clone_from(&session.messages);
    }
}

fn now_unix_ms() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis()
        .try_into()
        .unwrap_or(u64::MAX)
}

fn normalize_path(path: PathBuf) -> PathBuf {
    let absolute = if path.is_absolute() {
        path
    } else {
        std::env::current_dir()
            .map(|current| current.join(&path))
            .unwrap_or(path)
    };

    let Some(file_name) = absolute.file_name().map(OsString::from) else {
        return lexical_normalize(&absolute);
    };
    let parent = absolute.parent().unwrap_or_else(|| Path::new("."));
    canonicalize_parent(parent).join(file_name)
}

/// Resolve every existing parent component (including symlinks) while preserving a not-yet-created
/// destination. If part of the parent does not exist yet, canonicalize the nearest existing
/// ancestor and append the normalized missing suffix.
fn canonicalize_parent(parent: &Path) -> PathBuf {
    if let Ok(canonical) = fs::canonicalize(parent) {
        return canonical;
    }

    let normalized = lexical_normalize(parent);
    let mut cursor = normalized.as_path();
    let mut missing = Vec::new();
    loop {
        if let Ok(mut canonical) = fs::canonicalize(cursor) {
            for component in missing.iter().rev() {
                canonical.push(component);
            }
            return canonical;
        }
        let Some(name) = cursor.file_name() else {
            return normalized;
        };
        missing.push(name.to_os_string());
        let Some(next) = cursor.parent() else {
            return normalized;
        };
        cursor = next;
    }
}

fn lexical_normalize(path: &Path) -> PathBuf {
    let mut normalized = PathBuf::new();
    for component in path.components() {
        match component {
            Component::Prefix(prefix) => normalized.push(prefix.as_os_str()),
            Component::RootDir => normalized.push(component.as_os_str()),
            Component::CurDir => {}
            Component::ParentDir => {
                normalized.pop();
            }
            Component::Normal(part) => normalized.push(part),
        }
    }
    normalized
}

fn open_existing_session_file(path: &Path) -> SessionStoreResult<Option<File>> {
    // On Unix, O_NOFOLLOW closes the check/open race. The metadata preflight makes an already
    // present symlink fail closed on other targets too, where std does not expose an equivalent.
    match fs::symlink_metadata(path) {
        Ok(metadata) if metadata.file_type().is_symlink() => {
            return Err(SessionStoreError::Io {
                message: format!(
                    "refusing to open session file {} through a symlink",
                    path.display()
                ),
            });
        }
        Ok(_) => {}
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
        Err(error) => return Err(error.into()),
    }

    let mut options = OpenOptions::new();
    options.read(true);
    #[cfg(unix)]
    {
        use std::os::unix::fs::OpenOptionsExt;
        options.custom_flags(libc::O_NOFOLLOW);
    }
    let file = match options.open(path) {
        Ok(file) => file,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(None),
        Err(error) => return Err(error.into()),
    };
    ensure_regular_file(&file, "session file")?;
    tighten_owner_only_permissions(&file)?;
    Ok(Some(file))
}

fn ensure_regular_file(file: &File, description: &str) -> SessionStoreResult<()> {
    if file.metadata()?.is_file() {
        Ok(())
    } else {
        Err(SessionStoreError::Io {
            message: format!("{description} is not a regular file"),
        })
    }
}

fn tighten_owner_only_permissions(file: &File) -> SessionStoreResult<()> {
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        file.set_permissions(fs::Permissions::from_mode(0o600))?;
    }
    #[cfg(not(unix))]
    {
        let _ = file;
    }
    Ok(())
}

#[cfg(unix)]
fn same_open_file(left: &File, right: &File) -> SessionStoreResult<bool> {
    use std::os::unix::fs::MetadataExt;
    let left = left.metadata()?;
    let right = right.metadata()?;
    Ok(left.dev() == right.dev() && left.ino() == right.ino())
}

#[cfg(not(unix))]
fn same_open_file(_left: &File, _right: &File) -> SessionStoreResult<bool> {
    // The store promises process-local, not cross-process, serialization. All in-process aliases
    // share the canonical-path mutex; other platforms still reject an observed symlink and avoid
    // replacing a newly appeared target on first creation.
    Ok(true)
}

type SharedFileLocks = HashMap<PathBuf, Weak<Mutex<()>>>;

fn shared_file_lock(path: &Path) -> Arc<Mutex<()>> {
    static LOCKS: OnceLock<Mutex<SharedFileLocks>> = OnceLock::new();
    let mut locks = LOCKS
        .get_or_init(|| Mutex::new(HashMap::new()))
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner());
    locks.retain(|_, lock| lock.strong_count() > 0);
    if let Some(lock) = locks.get(path).and_then(Weak::upgrade) {
        return lock;
    }
    let lock = Arc::new(Mutex::new(()));
    locks.insert(path.to_path_buf(), Arc::downgrade(&lock));
    lock
}

fn sync_parent_directory(parent: &Path) -> SessionStoreResult<()> {
    #[cfg(unix)]
    {
        File::open(parent)?.sync_all()?;
    }
    #[cfg(not(unix))]
    {
        let _ = parent;
    }
    Ok(())
}

static TEMP_FILE_NONCE: AtomicU64 = AtomicU64::new(1);

#[cfg(test)]
mod tests {
    use super::*;
    use crate::types::{ContentBlock, Role};
    use tempfile::tempdir;

    fn canonical_messages() -> Vec<Message> {
        vec![
            Message::user("hi"),
            Message {
                role: Role::Assistant,
                content: vec![
                    ContentBlock::Reasoning {
                        text: "think".into(),
                        signature: Some("sig".into()),
                        provider: Some("anthropic".into()),
                        opaque: Some(serde_json::json!({"encrypted_content": "opaque"})),
                    },
                    ContentBlock::ToolUse {
                        id: "c1".into(),
                        name: "Read".into(),
                        input: serde_json::json!({ "path": "a" }),
                    },
                ],
            },
            Message {
                role: Role::User,
                content: vec![ContentBlock::ToolResult {
                    tool_use_id: "c1".into(),
                    content: "{\"text\":\"hello\"}".into(),
                    is_error: false,
                }],
            },
        ]
    }

    #[test]
    fn recorder_preserves_reasoning_tool_history_and_final_text() {
        let recorder = RunRecorder::default();
        recorder.begin(canonical_messages());
        recorder.set_final_text("hello");
        recorder.record_provider_metadata(
            "anthropic",
            serde_json::json!({ "stop_reason": "end_turn" }),
        );
        recorder.record_provider_metadata(
            "anthropic",
            serde_json::json!({ "usage": { "cache_read_input_tokens": 9 } }),
        );
        recorder.complete(Usage::default(), RunTerminalStatus::Completed, "end_turn");
        let outcome = recorder.outcome();
        assert_eq!(outcome.final_text.as_deref(), Some("hello"));
        assert_eq!(outcome.provider_metadata["anthropic"].len(), 2);
        assert_eq!(
            outcome.provider_metadata["anthropic"][1]["usage"]["cache_read_input_tokens"],
            9
        );
        assert!(matches!(
            outcome.messages[1].content[0],
            ContentBlock::Reasoning { .. }
        ));
        assert!(matches!(
            outcome.messages[1].content[1],
            ContentBlock::ToolUse { .. }
        ));
        assert!(matches!(
            outcome.messages[2].content[0],
            ContentBlock::ToolResult { .. }
        ));
    }

    #[test]
    fn in_memory_store_creates_loads_and_advances_revision() {
        let store = InMemorySessionStore::default();
        let created = store
            .create_session(
                Session::new("s1", canonical_messages())
                    .with_metadata("tenant", serde_json::json!("acme")),
            )
            .unwrap();
        assert_eq!(created.revision, 1);
        assert!(created.created_at_unix_ms > 0);
        assert_eq!(store.load_session("s1").unwrap(), created);

        let mut replacement = created.clone();
        replacement.messages.push(Message::user("continue"));
        let updated = store
            .compare_and_swap(created.revision, replacement)
            .unwrap();
        assert_eq!(updated.revision, 2);
        assert_eq!(updated.created_at_unix_ms, created.created_at_unix_ms);
        assert!(updated.updated_at_unix_ms > created.updated_at_unix_ms);
    }

    #[test]
    fn in_memory_store_reports_typed_conflict_and_not_found() {
        let store = InMemorySessionStore::default();
        let created = store
            .create_session(Session::new("s1", vec![Message::user("one")]))
            .unwrap();
        let error = store
            .compare_and_swap(0, created.clone())
            .expect_err("stale revision must fail");
        assert_eq!(
            error,
            SessionStoreError::Conflict {
                id: "s1".into(),
                expected_revision: 0,
                actual_revision: 1,
            }
        );
        let typed: crate::error::AikitError = error.into();
        assert_eq!(typed.info().code, crate::error::ErrorCode::Conflict);
        let missing = store.load_session("missing").unwrap_err();
        assert_eq!(
            missing,
            SessionStoreError::NotFound {
                id: "missing".into()
            }
        );
        let typed: crate::error::AikitError = missing.into();
        assert_eq!(typed.info().code, crate::error::ErrorCode::Session);
    }

    #[test]
    fn compatibility_outcome_api_round_trips_all_fields() {
        let store = InMemorySessionStore::default();
        let outcome = RunOutcome {
            messages: canonical_messages(),
            usage: Usage {
                input_tokens: 10,
                output_tokens: 4,
                ..Usage::default()
            },
            provider_metadata: [(
                "openai".into(),
                vec![serde_json::json!({
                    "status": "completed",
                    "usage": [{ "input_tokens_details": { "cached_tokens": 3 } }]
                })],
            )]
            .into(),
            terminal_status: RunTerminalStatus::Completed,
            stop_reason: Some("end_turn".into()),
            model_attempts: vec!["model-a".into()],
            final_text: Some("hello".into()),
        };
        store.save("s1", &outcome).unwrap();
        assert_eq!(store.load("s1").unwrap(), Some(outcome));
        assert_eq!(store.load("missing").unwrap(), None);
    }

    #[test]
    fn json_store_and_legacy_serde_preserve_provider_metadata_compatibly() {
        let directory = tempdir().unwrap();
        let path = directory.path().join("metadata-sessions.json");
        let outcome = RunOutcome {
            messages: vec![Message::user("metadata")],
            provider_metadata: [(
                "google".into(),
                vec![serde_json::json!({
                    "finishReason": "STOP",
                    "groundingMetadata": { "webSearchQueries": ["aikit"] }
                })],
            )]
            .into(),
            terminal_status: RunTerminalStatus::Completed,
            stop_reason: Some("end_turn".into()),
            ..RunOutcome::default()
        };
        {
            let store = JsonFileSessionStore::new(&path);
            store.save("metadata", &outcome).unwrap();
        }
        let reopened = JsonFileSessionStore::new(&path)
            .load("metadata")
            .unwrap()
            .expect("persisted outcome");
        assert_eq!(reopened, outcome);

        let legacy: RunOutcome = serde_json::from_value(serde_json::json!({
            "messages": [],
            "usage": {
                "input_tokens": 0,
                "output_tokens": 0,
                "cache_creation_input_tokens": 0,
                "cache_read_input_tokens": 0,
                "reasoning_tokens": 0
            },
            "terminal_status": "completed",
            "stop_reason": "end_turn",
            "model_attempts": []
        }))
        .unwrap();
        assert!(legacy.provider_metadata.is_empty());
    }

    #[test]
    fn json_store_reopens_with_exact_reasoning_and_tool_fidelity() {
        let directory = tempdir().unwrap();
        let path = directory.path().join("sessions.json");
        let created = {
            let store = JsonFileSessionStore::new(&path);
            store
                .create_session(
                    Session::new("reopen", canonical_messages())
                        .with_metadata("purpose", serde_json::json!("resume")),
                )
                .unwrap()
        };

        let reopened = JsonFileSessionStore::new(&path)
            .load_session("reopen")
            .unwrap();
        assert_eq!(reopened, created);
        assert_eq!(reopened.messages, canonical_messages());

        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            let metadata = fs::metadata(path).unwrap();
            assert!(metadata.is_file());
            assert_eq!(metadata.permissions().mode() & 0o777, 0o600);
        }
    }

    #[test]
    fn json_store_path_aliases_share_lock_and_reject_stale_revision() {
        let directory = tempdir().unwrap();
        let real_parent = directory.path().join("real");
        let nested = real_parent.join("nested");
        fs::create_dir_all(&nested).unwrap();
        let path = real_parent.join("sessions.json");
        let alias = nested.join("..").join(".").join("sessions.json");
        let first = JsonFileSessionStore::new(&path);
        let second = JsonFileSessionStore::new(&alias);
        assert_eq!(first.path(), second.path());
        assert!(Arc::ptr_eq(&first.lock, &second.lock));

        #[cfg(unix)]
        {
            use std::os::unix::fs::symlink;
            let parent_alias = directory.path().join("parent-alias");
            symlink(&real_parent, &parent_alias).unwrap();
            let through_symlink = JsonFileSessionStore::new(parent_alias.join("sessions.json"));
            assert_eq!(first.path(), through_symlink.path());
            assert!(Arc::ptr_eq(&first.lock, &through_symlink.lock));
        }

        let snapshot = first
            .create_session(Session::new("cas", vec![Message::user("one")]))
            .unwrap();

        let mut winner = snapshot.clone();
        winner.messages.push(Message::user("winner"));
        let winner = first.compare_and_swap(snapshot.revision, winner).unwrap();

        let mut stale = snapshot.clone();
        stale.messages.push(Message::user("stale"));
        assert_eq!(
            second
                .compare_and_swap(snapshot.revision, stale)
                .unwrap_err(),
            SessionStoreError::Conflict {
                id: "cas".into(),
                expected_revision: 1,
                actual_revision: 2,
            }
        );
        assert_eq!(second.load_session("cas").unwrap(), winner);
    }

    #[cfg(unix)]
    #[test]
    fn json_store_rejects_final_symlink_without_replacing_it() {
        use std::os::unix::fs::symlink;

        let directory = tempdir().unwrap();
        let backing = directory.path().join("backing.json");
        let path = directory.path().join("sessions.json");
        fs::write(&backing, b"do not touch").unwrap();
        symlink(&backing, &path).unwrap();

        let store = JsonFileSessionStore::new(&path);
        let error = store
            .create_session(Session::new("symlink", Vec::new()))
            .expect_err("a final symlink must be rejected");
        assert!(matches!(error, SessionStoreError::Io { .. }));
        assert!(error.to_string().contains("through a symlink"));
        assert!(fs::symlink_metadata(&path)
            .unwrap()
            .file_type()
            .is_symlink());
        assert_eq!(fs::read(&backing).unwrap(), b"do not touch");
    }

    #[cfg(unix)]
    #[test]
    fn json_store_tightens_existing_file_permissions_on_open() {
        use std::os::unix::fs::PermissionsExt;

        let directory = tempdir().unwrap();
        let path = directory.path().join("sessions.json");
        fs::write(&path, serde_json::to_vec(&SessionFile::default()).unwrap()).unwrap();
        fs::set_permissions(&path, fs::Permissions::from_mode(0o666)).unwrap();

        let store = JsonFileSessionStore::new(&path);
        assert!(matches!(
            store.load_session("missing"),
            Err(SessionStoreError::NotFound { .. })
        ));
        let metadata = fs::metadata(&path).unwrap();
        assert!(metadata.is_file());
        assert_eq!(metadata.permissions().mode() & 0o777, 0o600);
    }

    #[test]
    fn json_store_create_conflict_and_missing_are_typed() {
        let directory = tempdir().unwrap();
        let store = JsonFileSessionStore::new(directory.path().join("sessions.json"));
        store
            .create_session(Session::new("s1", Vec::new()))
            .unwrap();
        assert_eq!(
            store
                .create_session(Session::new("s1", Vec::new()))
                .unwrap_err(),
            SessionStoreError::Conflict {
                id: "s1".into(),
                expected_revision: 0,
                actual_revision: 1,
            }
        );
        assert_eq!(
            store.load_session("absent").unwrap_err(),
            SessionStoreError::NotFound {
                id: "absent".into()
            }
        );
    }
}
