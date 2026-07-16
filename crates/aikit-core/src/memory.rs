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

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct MemoryEntry {
    pub namespace: String,
    pub key: String,
    pub value: Value,
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
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MemoryQuery {
    pub namespace: String,
    pub text: String,
    pub tags: BTreeSet<String>,
    pub limit: usize,
}

impl MemoryQuery {
    pub fn new(namespace: impl Into<String>, text: impl Into<String>, limit: usize) -> Self {
        MemoryQuery {
            namespace: namespace.into(),
            text: text.into(),
            tags: BTreeSet::new(),
            limit,
        }
    }
}

pub trait MemoryStore: Send + Sync {
    fn put(&self, entry: MemoryEntry) -> std::result::Result<(), String>;
    fn get(&self, namespace: &str, key: &str) -> std::result::Result<Option<MemoryEntry>, String>;
    fn search(&self, query: &MemoryQuery) -> std::result::Result<Vec<MemoryEntry>, String>;
    fn delete(&self, namespace: &str, key: &str) -> std::result::Result<bool, String>;
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
            values
                .into_iter()
                .map(|entry| ((entry.namespace.clone(), entry.key.clone()), entry))
                .collect()
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

fn validate_entry(entry: &MemoryEntry) -> std::result::Result<(), String> {
    if entry.namespace.trim().is_empty() || entry.key.trim().is_empty() {
        return Err("memory namespace and key must be non-empty".into());
    }
    if entry.namespace.len() > 256 || entry.key.len() > 512 {
        return Err("memory namespace/key exceeds length limit".into());
    }
    Ok(())
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
