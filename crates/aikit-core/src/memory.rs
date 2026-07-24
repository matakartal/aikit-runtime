//! Explicit, governed persistent memory.
//!
//! Model output is never written automatically: only a caller/agent action invoking `remember`
//! creates an entry. That avoids turning prompt injection into silent long-term memory poisoning.
//! v1 search is deterministic keyword/tag ranking; vector stores can implement the same trait.

use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::cmp::Reverse;
use std::collections::{BTreeSet, HashMap};
use std::ffi::OsString;
use std::fs::{File, OpenOptions};
use std::io::{BufWriter, Write};
use std::path::{Component, Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex, OnceLock, Weak};
use std::time::{SystemTime, UNIX_EPOCH};

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum MemoryPlane {
    #[default]
    Working,
    Episodic,
    Semantic,
}

#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct MemoryProvenance {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub source_run_id: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub source_event_sequence: Option<u64>,
    #[serde(default)]
    pub model_generated: bool,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct MemoryEntry {
    pub namespace: String,
    pub key: String,
    pub value: Value,
    #[serde(default)]
    pub plane: MemoryPlane,
    /// Optimistic concurrency revision. Zero is construction-time/unpersisted only.
    #[serde(default)]
    pub revision: u64,
    #[serde(default)]
    pub provenance: MemoryProvenance,
    pub tags: BTreeSet<String>,
    pub importance: u8,
    pub created_unix_ms: u128,
    pub updated_unix_ms: u128,
}

impl MemoryEntry {
    pub fn new(namespace: impl Into<String>, key: impl Into<String>, value: Value) -> Self {
        let now = now_ms();
        MemoryEntry {
            namespace: namespace.into(),
            key: key.into(),
            value,
            plane: MemoryPlane::Working,
            revision: 0,
            provenance: MemoryProvenance::default(),
            tags: BTreeSet::new(),
            importance: 50,
            created_unix_ms: now,
            updated_unix_ms: now,
        }
    }

    pub fn with_tags(mut self, tags: impl IntoIterator<Item = String>) -> Self {
        self.tags = tags.into_iter().collect();
        self
    }

    pub fn with_importance(mut self, importance: u8) -> Self {
        self.importance = importance.min(100);
        self
    }

    pub fn with_plane(mut self, plane: MemoryPlane) -> Self {
        self.plane = plane;
        self
    }

    pub fn with_provenance(mut self, provenance: MemoryProvenance) -> Self {
        self.provenance = provenance;
        self
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MemoryQuery {
    pub namespace: String,
    pub text: String,
    pub tags: BTreeSet<String>,
    pub limit: usize,
    pub plane: Option<MemoryPlane>,
}

impl MemoryQuery {
    pub fn new(namespace: impl Into<String>, text: impl Into<String>, limit: usize) -> Self {
        MemoryQuery {
            namespace: namespace.into(),
            text: text.into(),
            tags: BTreeSet::new(),
            limit,
            plane: None,
        }
    }

    pub fn in_plane(mut self, plane: MemoryPlane) -> Self {
        self.plane = Some(plane);
        self
    }
}

pub trait MemoryStore: Send + Sync {
    fn put(&self, entry: MemoryEntry) -> std::result::Result<(), String>;
    fn get(&self, namespace: &str, key: &str) -> std::result::Result<Option<MemoryEntry>, String>;
    fn search(&self, query: &MemoryQuery) -> std::result::Result<Vec<MemoryEntry>, String>;
    fn delete(&self, namespace: &str, key: &str) -> std::result::Result<bool, String>;

    /// Atomic compare-and-swap. `expected_revision == 0` creates only when absent.
    fn compare_and_swap(
        &self,
        _entry: MemoryEntry,
        _expected_revision: u64,
    ) -> std::result::Result<u64, String> {
        Err("memory store does not implement compare-and-swap".into())
    }
}

#[derive(Default)]
pub struct InMemoryMemoryStore {
    entries: Mutex<HashMap<(String, String), MemoryEntry>>,
}

impl MemoryStore for InMemoryMemoryStore {
    fn put(&self, mut entry: MemoryEntry) -> std::result::Result<(), String> {
        validate_entry(&entry)?;
        let mut entries = self
            .entries
            .lock()
            .map_err(|_| "memory mutex poisoned".to_string())?;
        if let Some(existing) = entries.get(&(entry.namespace.clone(), entry.key.clone())) {
            entry.created_unix_ms = existing.created_unix_ms;
            entry.revision = next_memory_revision(existing.revision)?;
        } else {
            entry.revision = 1;
        }
        entry.updated_unix_ms = now_ms();
        entries.insert((entry.namespace.clone(), entry.key.clone()), entry);
        Ok(())
    }

    fn get(&self, namespace: &str, key: &str) -> std::result::Result<Option<MemoryEntry>, String> {
        Ok(self
            .entries
            .lock()
            .map_err(|_| "memory mutex poisoned".to_string())?
            .get(&(namespace.to_string(), key.to_string()))
            .cloned())
    }

    fn search(&self, query: &MemoryQuery) -> std::result::Result<Vec<MemoryEntry>, String> {
        let entries = self
            .entries
            .lock()
            .map_err(|_| "memory mutex poisoned".to_string())?;
        Ok(rank_entries(entries.values(), query))
    }

    fn delete(&self, namespace: &str, key: &str) -> std::result::Result<bool, String> {
        Ok(self
            .entries
            .lock()
            .map_err(|_| "memory mutex poisoned".to_string())?
            .remove(&(namespace.to_string(), key.to_string()))
            .is_some())
    }

    fn compare_and_swap(
        &self,
        mut entry: MemoryEntry,
        expected_revision: u64,
    ) -> std::result::Result<u64, String> {
        validate_entry(&entry)?;
        let mut entries = self
            .entries
            .lock()
            .map_err(|_| "memory mutex poisoned".to_string())?;
        let key = (entry.namespace.clone(), entry.key.clone());
        let actual = entries.get(&key).map_or(0, |existing| existing.revision);
        if actual != expected_revision {
            return Err(format!(
                "memory revision conflict: expected {expected_revision}, found {actual}"
            ));
        }
        if let Some(existing) = entries.get(&key) {
            entry.created_unix_ms = existing.created_unix_ms;
        }
        entry.revision = actual
            .checked_add(1)
            .ok_or_else(|| "memory revision overflow".to_string())?;
        entry.updated_unix_ms = now_ms();
        let revision = entry.revision;
        entries.insert(key, entry);
        Ok(revision)
    }
}

/// Process-safe-in-memory, crash-safe-on-write JSON store. Cross-process locking is intentionally
/// outside v1; each write uses a same-directory temp file + atomic rename and mode 0600 on Unix.
pub struct JsonFileMemoryStore {
    path: PathBuf,
    entries: Arc<Mutex<HashMap<(String, String), MemoryEntry>>>,
}

impl JsonFileMemoryStore {
    pub fn open(path: impl AsRef<Path>) -> std::result::Result<Self, String> {
        let requested = path.as_ref().to_path_buf();
        if let Some(parent) = requested.parent() {
            std::fs::create_dir_all(parent).map_err(|e| e.to_string())?;
        }
        let path = normalize_memory_path(requested);
        let loaded = if let Some(file) = open_existing_memory_file(&path)? {
            let values: Vec<MemoryEntry> =
                serde_json::from_reader(file).map_err(|e| e.to_string())?;
            let mut loaded = HashMap::with_capacity(values.len());
            for entry in values {
                let entry = normalize_persisted_entry(entry)?;
                let key = (entry.namespace.clone(), entry.key.clone());
                if loaded.insert(key, entry).is_some() {
                    return Err("memory file contains duplicate namespace/key entries".into());
                }
            }
            loaded
        } else {
            HashMap::new()
        };
        let entries = shared_memory_state(&path, loaded);
        Ok(JsonFileMemoryStore { path, entries })
    }

    fn persist(
        &self,
        entries: &HashMap<(String, String), MemoryEntry>,
    ) -> std::result::Result<(), String> {
        let mut values: Vec<_> = entries.values().cloned().collect();
        values.sort_by(|a, b| (&a.namespace, &a.key).cmp(&(&b.namespace, &b.key)));
        write_memory_file(&self.path, &values)
    }
}

impl MemoryStore for JsonFileMemoryStore {
    fn put(&self, mut entry: MemoryEntry) -> std::result::Result<(), String> {
        validate_entry(&entry)?;
        let mut entries = self
            .entries
            .lock()
            .map_err(|_| "memory mutex poisoned".to_string())?;
        let mut next = entries.clone();
        if let Some(existing) = next.get(&(entry.namespace.clone(), entry.key.clone())) {
            entry.created_unix_ms = existing.created_unix_ms;
            entry.revision = next_memory_revision(existing.revision)?;
        } else {
            entry.revision = 1;
        }
        entry.updated_unix_ms = now_ms();
        next.insert((entry.namespace.clone(), entry.key.clone()), entry);
        self.persist(&next)?;
        *entries = next;
        Ok(())
    }

    fn get(&self, namespace: &str, key: &str) -> std::result::Result<Option<MemoryEntry>, String> {
        Ok(self
            .entries
            .lock()
            .map_err(|_| "memory mutex poisoned".to_string())?
            .get(&(namespace.to_string(), key.to_string()))
            .cloned())
    }

    fn search(&self, query: &MemoryQuery) -> std::result::Result<Vec<MemoryEntry>, String> {
        let entries = self
            .entries
            .lock()
            .map_err(|_| "memory mutex poisoned".to_string())?;
        Ok(rank_entries(entries.values(), query))
    }

    fn delete(&self, namespace: &str, key: &str) -> std::result::Result<bool, String> {
        let mut entries = self
            .entries
            .lock()
            .map_err(|_| "memory mutex poisoned".to_string())?;
        let mut next = entries.clone();
        let removed = next
            .remove(&(namespace.to_string(), key.to_string()))
            .is_some();
        if removed {
            self.persist(&next)?;
            *entries = next;
        }
        Ok(removed)
    }

    fn compare_and_swap(
        &self,
        mut entry: MemoryEntry,
        expected_revision: u64,
    ) -> std::result::Result<u64, String> {
        validate_entry(&entry)?;
        let mut entries = self
            .entries
            .lock()
            .map_err(|_| "memory mutex poisoned".to_string())?;
        let mut next = entries.clone();
        let key = (entry.namespace.clone(), entry.key.clone());
        let actual = next.get(&key).map_or(0, |existing| existing.revision);
        if actual != expected_revision {
            return Err(format!(
                "memory revision conflict: expected {expected_revision}, found {actual}"
            ));
        }
        if let Some(existing) = next.get(&key) {
            entry.created_unix_ms = existing.created_unix_ms;
        }
        entry.revision = actual
            .checked_add(1)
            .ok_or_else(|| "memory revision overflow".to_string())?;
        entry.updated_unix_ms = now_ms();
        let revision = entry.revision;
        next.insert(key, entry);
        self.persist(&next)?;
        *entries = next;
        Ok(revision)
    }
}

type MemoryMap = HashMap<(String, String), MemoryEntry>;
type SharedMemoryStates = HashMap<PathBuf, Weak<Mutex<MemoryMap>>>;

fn shared_memory_state(path: &Path, loaded: MemoryMap) -> Arc<Mutex<MemoryMap>> {
    static STATES: OnceLock<Mutex<SharedMemoryStates>> = OnceLock::new();
    let mut states = STATES
        .get_or_init(|| Mutex::new(HashMap::new()))
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner());
    states.retain(|_, state| state.strong_count() > 0);
    if let Some(state) = states.get(path).and_then(Weak::upgrade) {
        return state;
    }
    let state = Arc::new(Mutex::new(loaded));
    states.insert(path.to_path_buf(), Arc::downgrade(&state));
    state
}

fn normalize_memory_path(path: PathBuf) -> PathBuf {
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
    std::fs::canonicalize(parent)
        .unwrap_or_else(|_| lexical_normalize(parent))
        .join(file_name)
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

fn open_existing_memory_file(path: &Path) -> std::result::Result<Option<File>, String> {
    match std::fs::symlink_metadata(path) {
        Ok(metadata) if metadata.file_type().is_symlink() => {
            return Err(format!(
                "refusing to open memory file {} through a symlink",
                path.display()
            ));
        }
        Ok(_) => {}
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
        Err(error) => return Err(error.to_string()),
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
        Err(error) => return Err(error.to_string()),
    };
    ensure_regular_memory_file(&file, "memory file")?;
    tighten_memory_permissions(&file)?;
    Ok(Some(file))
}

fn ensure_regular_memory_file(file: &File, description: &str) -> std::result::Result<(), String> {
    if file
        .metadata()
        .map_err(|error| error.to_string())?
        .is_file()
    {
        Ok(())
    } else {
        Err(format!("{description} is not a regular file"))
    }
}

fn tighten_memory_permissions(file: &File) -> std::result::Result<(), String> {
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        file.set_permissions(std::fs::Permissions::from_mode(0o600))
            .map_err(|error| error.to_string())?;
    }
    #[cfg(not(unix))]
    {
        let _ = file;
    }
    Ok(())
}

fn write_memory_file(path: &Path, values: &[MemoryEntry]) -> std::result::Result<(), String> {
    let parent = path.parent().unwrap_or_else(|| Path::new("."));
    std::fs::create_dir_all(parent).map_err(|error| error.to_string())?;
    let target_before = open_existing_memory_file(path)?;

    let (temporary_path, file) = loop {
        let nonce = MEMORY_TEMP_NONCE.fetch_add(1, Ordering::Relaxed);
        let file_name = path
            .file_name()
            .and_then(|name| name.to_str())
            .unwrap_or("memory.json");
        let temporary_path =
            parent.join(format!(".{file_name}.{}.{}.tmp", std::process::id(), nonce));
        let mut options = OpenOptions::new();
        options.write(true).create_new(true);
        #[cfg(unix)]
        {
            use std::os::unix::fs::OpenOptionsExt;
            options.mode(0o600);
        }
        match options.open(&temporary_path) {
            Ok(file) => break (temporary_path, file),
            Err(error) if error.kind() == std::io::ErrorKind::AlreadyExists => continue,
            Err(error) => return Err(error.to_string()),
        }
    };

    let result = (|| -> std::result::Result<(), String> {
        ensure_regular_memory_file(&file, "memory temporary file")?;
        tighten_memory_permissions(&file)?;
        let mut writer = BufWriter::new(file);
        serde_json::to_writer_pretty(&mut writer, values).map_err(|error| error.to_string())?;
        writer.write_all(b"\n").map_err(|error| error.to_string())?;
        writer.flush().map_err(|error| error.to_string())?;
        writer
            .get_ref()
            .sync_all()
            .map_err(|error| error.to_string())?;
        drop(writer);

        let target_after = open_existing_memory_file(path)?;
        let replace_existing = match (&target_before, &target_after) {
            (None, None) => false,
            (Some(before), Some(after)) if same_memory_file(before, after)? => true,
            _ => {
                return Err(format!(
                    "memory file {} changed during update",
                    path.display()
                ))
            }
        };
        drop(target_after);
        drop(target_before);
        if replace_existing {
            std::fs::rename(&temporary_path, path).map_err(|error| error.to_string())?;
        } else {
            std::fs::hard_link(&temporary_path, path).map_err(|error| error.to_string())?;
            std::fs::remove_file(&temporary_path).map_err(|error| error.to_string())?;
        }
        let installed = open_existing_memory_file(path)?
            .ok_or_else(|| "memory file disappeared after installation".to_string())?;
        ensure_regular_memory_file(&installed, "memory file")?;
        sync_memory_parent(parent)?;
        Ok(())
    })();

    if result.is_err() {
        let _ = std::fs::remove_file(&temporary_path);
    }
    result
}

#[cfg(unix)]
fn same_memory_file(left: &File, right: &File) -> std::result::Result<bool, String> {
    use std::os::unix::fs::MetadataExt;
    let left = left.metadata().map_err(|error| error.to_string())?;
    let right = right.metadata().map_err(|error| error.to_string())?;
    Ok(left.dev() == right.dev() && left.ino() == right.ino())
}

#[cfg(not(unix))]
fn same_memory_file(_left: &File, _right: &File) -> std::result::Result<bool, String> {
    Ok(true)
}

fn sync_memory_parent(parent: &Path) -> std::result::Result<(), String> {
    #[cfg(unix)]
    {
        File::open(parent)
            .and_then(|directory| directory.sync_all())
            .map_err(|error| error.to_string())?;
    }
    #[cfg(not(unix))]
    {
        let _ = parent;
    }
    Ok(())
}

static MEMORY_TEMP_NONCE: AtomicU64 = AtomicU64::new(1);

pub(crate) fn validate_entry(entry: &MemoryEntry) -> std::result::Result<(), String> {
    if entry.namespace.trim().is_empty() || entry.key.trim().is_empty() {
        return Err("memory namespace and key must be non-empty".into());
    }
    if entry.namespace.len() > 256 || entry.key.len() > 512 {
        return Err("memory namespace/key exceeds length limit".into());
    }
    if entry.importance > 100 {
        return Err("memory importance must be between 0 and 100".into());
    }
    Ok(())
}

/// Migrate fields that older stores persisted before their current invariants existed.
///
/// This is intentionally separate from [`validate_entry`]: new caller writes remain strict,
/// while already-persisted revision zero and out-of-range ranking weights get one deterministic
/// compatibility interpretation. Every other invalid persisted field remains fail closed.
pub(crate) fn normalize_persisted_entry(
    mut entry: MemoryEntry,
) -> std::result::Result<MemoryEntry, String> {
    if entry.revision == 0 {
        entry.revision = 1;
    }
    if entry.importance > 100 {
        entry.importance = 100;
    }
    validate_entry(&entry)?;
    Ok(entry)
}

fn next_memory_revision(revision: u64) -> std::result::Result<u64, String> {
    revision
        .checked_add(1)
        .ok_or_else(|| "memory revision overflow".to_string())
}

fn rank_entries<'a>(
    entries: impl Iterator<Item = &'a MemoryEntry>,
    query: &MemoryQuery,
) -> Vec<MemoryEntry> {
    let words: BTreeSet<String> = query
        .text
        .split(|c: char| !c.is_alphanumeric())
        .filter(|word| !word.is_empty())
        .map(str::to_ascii_lowercase)
        .collect();
    let mut scored: Vec<(usize, u8, u128, String, MemoryEntry)> = entries
        .filter(|entry| entry.namespace == query.namespace)
        .filter(|entry| query.plane.is_none_or(|plane| entry.plane == plane))
        .filter(|entry| query.tags.is_subset(&entry.tags))
        .map(|entry| {
            let haystack = format!("{} {}", entry.key, entry.value).to_ascii_lowercase();
            let word_score = words
                .iter()
                .filter(|word| haystack.contains(word.as_str()))
                .count();
            let tag_score = query.tags.intersection(&entry.tags).count();
            (
                word_score + tag_score,
                entry.importance,
                entry.updated_unix_ms,
                entry.key.clone(),
                entry.clone(),
            )
        })
        .filter(|(score, _, _, _, _)| words.is_empty() || *score > 0)
        .collect();
    scored.sort_by_key(|(score, importance, updated, key, _)| {
        (
            Reverse(*score),
            Reverse(*importance),
            Reverse(*updated),
            key.clone(),
        )
    });
    scored
        .into_iter()
        .take(query.limit.min(100))
        .map(|(_, _, _, _, entry)| entry)
        .collect()
}

fn now_ms() -> u128 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis()
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn namespaces_are_isolated_and_recall_is_bounded() {
        let store = InMemoryMemoryStore::default();
        store
            .put(MemoryEntry::new("a", "rust", json!("tokio async")))
            .unwrap();
        store
            .put(MemoryEntry::new("b", "rust", json!("different tenant")))
            .unwrap();
        let got = store.search(&MemoryQuery::new("a", "tokio", 1)).unwrap();
        assert_eq!(got.len(), 1);
        assert_eq!(got[0].namespace, "a");
    }

    #[test]
    fn memory_planes_filter_and_cas_rejects_lost_updates() {
        let store = InMemoryMemoryStore::default();
        let working = MemoryEntry::new("agent", "scratch", json!("short lived"));
        let semantic = MemoryEntry::new("agent", "rule", json!("always validate"))
            .with_plane(MemoryPlane::Semantic)
            .with_provenance(MemoryProvenance {
                source_run_id: Some("run-1".into()),
                source_event_sequence: Some(7),
                model_generated: false,
            });
        store.put(working).unwrap();
        assert_eq!(store.compare_and_swap(semantic.clone(), 0), Ok(1));

        let semantic_results = store
            .search(&MemoryQuery::new("agent", "", 10).in_plane(MemoryPlane::Semantic))
            .unwrap();
        assert_eq!(semantic_results.len(), 1);
        assert_eq!(semantic_results[0].key, "rule");
        assert_eq!(semantic_results[0].revision, 1);

        let mut updated = semantic;
        updated.value = json!("new value");
        assert!(store.compare_and_swap(updated.clone(), 0).is_err());
        assert_eq!(store.compare_and_swap(updated, 1), Ok(2));
    }

    #[test]
    fn put_rejects_revision_overflow_without_mutating_memory() {
        let store = InMemoryMemoryStore::default();
        let mut existing = MemoryEntry::new("agent", "stable", json!("old"));
        existing.revision = u64::MAX;
        store.entries.lock().unwrap().insert(
            (existing.namespace.clone(), existing.key.clone()),
            existing.clone(),
        );

        let error = store
            .put(MemoryEntry::new("agent", "stable", json!("new")))
            .unwrap_err();
        assert!(error.contains("revision overflow"));
        assert_eq!(store.get("agent", "stable").unwrap(), Some(existing));
    }

    #[test]
    fn invalid_importance_is_rejected() {
        let store = InMemoryMemoryStore::default();
        let mut entry = MemoryEntry::new("agent", "unsafe", json!(true));
        entry.importance = 101;
        assert!(store.put(entry).is_err());
    }

    #[test]
    fn file_store_survives_reopen_and_uses_private_permissions() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("memory.json");
        {
            let store = JsonFileMemoryStore::open(&path).unwrap();
            store
                .put(MemoryEntry::new("agent", "decision", json!("use Rust")))
                .unwrap();
        }
        let reopened = JsonFileMemoryStore::open(&path).unwrap();
        assert_eq!(
            reopened.get("agent", "decision").unwrap().unwrap().value,
            json!("use Rust")
        );
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            assert_eq!(
                std::fs::metadata(path).unwrap().permissions().mode() & 0o777,
                0o600
            );
        }
    }

    #[test]
    fn file_store_migrates_legacy_revision_before_cas() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("memory.json");
        let mut legacy = MemoryEntry::new("agent", "legacy", json!("old"));
        legacy.importance = u8::MAX;
        assert_eq!(legacy.revision, 0);
        std::fs::write(&path, serde_json::to_vec(&vec![legacy]).unwrap()).unwrap();

        let store = JsonFileMemoryStore::open(&path).unwrap();
        let migrated = store.get("agent", "legacy").unwrap().unwrap();
        assert_eq!(migrated.revision, 1);
        assert_eq!(migrated.importance, 100);
        let update = MemoryEntry::new("agent", "legacy", json!("new"));
        assert!(store.compare_and_swap(update.clone(), 0).is_err());
        assert_eq!(store.compare_and_swap(update, 1), Ok(2));
    }

    #[test]
    fn file_store_rejects_duplicate_keys() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("memory.json");
        let first = MemoryEntry::new("agent", "duplicate", json!(1));
        let second = MemoryEntry::new("agent", "duplicate", json!(2));
        std::fs::write(&path, serde_json::to_vec(&vec![first, second]).unwrap()).unwrap();

        let error = JsonFileMemoryStore::open(&path).err().unwrap();
        assert!(error.contains("duplicate namespace/key"));
    }

    #[cfg(unix)]
    #[test]
    fn file_store_aliases_share_one_process_state() {
        use std::os::unix::fs::symlink;

        let dir = tempfile::tempdir().unwrap();
        let real = dir.path().join("real");
        let alias = dir.path().join("alias");
        std::fs::create_dir(&real).unwrap();
        symlink(&real, &alias).unwrap();

        let first = JsonFileMemoryStore::open(real.join("memory.json")).unwrap();
        let second = JsonFileMemoryStore::open(alias.join("./memory.json")).unwrap();
        first
            .put(MemoryEntry::new("agent", "one", json!(1)))
            .unwrap();
        assert_eq!(second.get("agent", "one").unwrap().unwrap().value, json!(1));
        second
            .put(MemoryEntry::new("agent", "two", json!(2)))
            .unwrap();
        assert_eq!(first.get("agent", "two").unwrap().unwrap().value, json!(2));

        let reopened = JsonFileMemoryStore::open(real.join("memory.json")).unwrap();
        assert!(reopened.get("agent", "one").unwrap().is_some());
        assert!(reopened.get("agent", "two").unwrap().is_some());
    }

    #[cfg(unix)]
    #[test]
    fn file_store_rejects_final_symlink_and_preserves_target() {
        use std::os::unix::fs::symlink;

        let dir = tempfile::tempdir().unwrap();
        let target = dir.path().join("target.json");
        let link = dir.path().join("memory.json");
        std::fs::write(&target, "sensitive").unwrap();
        symlink(&target, &link).unwrap();

        let error = JsonFileMemoryStore::open(&link).err().unwrap();
        assert!(error.contains("symlink"));
        assert_eq!(std::fs::read_to_string(target).unwrap(), "sensitive");
    }

    #[cfg(unix)]
    #[test]
    fn file_store_tightens_existing_permissions() {
        use std::os::unix::fs::PermissionsExt;

        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("memory.json");
        std::fs::write(&path, "[]").unwrap();
        std::fs::set_permissions(&path, std::fs::Permissions::from_mode(0o666)).unwrap();

        let _store = JsonFileMemoryStore::open(&path).unwrap();
        assert_eq!(
            std::fs::metadata(path).unwrap().permissions().mode() & 0o777,
            0o600
        );
    }

    #[test]
    fn model_output_is_not_written_without_explicit_put() {
        let store = InMemoryMemoryStore::default();
        assert!(store
            .search(&MemoryQuery::new("agent", "anything", 10))
            .unwrap()
            .is_empty());
    }
}
