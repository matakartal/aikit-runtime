//! The filesystem sandbox — a descriptor-relative path jail for the built-in file tools.
//!
//! # Threat model (be honest about what this is and isn't)
//!
//! This is a **path jail** for aikit's built-in file tools (Read/Write/Edit/Grep/Glob). A jailed
//! root is opened once as a directory capability. Every later lookup is relative to that open
//! descriptor, and every component is opened with symlink following disabled. This prevents `..`,
//! symlink escapes, and check-then-open rename/symlink races from redirecting file I/O outside the
//! captured root. Jailed mode is supported on Linux and macOS; other platforms fail closed.
//!
//! It is **NOT** process isolation. The `Bash` tool can spawn a process that ignores the jail
//! entirely. Bash is instead guarded by two other composable layers: the *permission engine*
//! (deny rules decide what may run at all) and a [`BashPolicy`](super::process::BashPolicy) of
//! *process hardening* (scrubbed environment so the agent's secrets don't leak into shells, a
//! wall-clock timeout, bounded output, and Unix rlimits). OS containment remains a distinct layer.

#[cfg(unix)]
use cap_std::fs::{Dir, OpenOptions, OpenOptionsExt};
#[cfg(unix)]
use cap_std::{ambient_authority, fs};
use std::ffi::{OsStr, OsString};
use std::fs::File;
#[cfg(unix)]
use std::os::unix::fs::OpenOptionsExt as StdOpenOptionsExt;
use std::path::{Component, Path, PathBuf};
use std::sync::Arc;

#[derive(Debug)]
struct CapabilityRoot {
    /// Stable user-facing path captured when the jail is configured. Security decisions never
    /// reopen this ambient path; all jailed I/O uses `dir`.
    path: PathBuf,
    /// Absolute lexical spellings accepted for UX (for example macOS `/var` for a root whose
    /// canonical display path is `/private/var`). They only select this already-open capability.
    aliases: Vec<PathBuf>,
    #[cfg(unix)]
    dir: Dir,
}

/// An allow-list of open root-directory capabilities that file access is confined to.
#[derive(Debug, Clone)]
pub struct Sandbox {
    roots: Vec<Arc<CapabilityRoot>>,
    unrestricted: bool,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum SandboxError {
    /// The path resolves outside every allowed root or names a symlink in jailed mode.
    Escape(String),
    /// The path (or its parent) could not be opened safely.
    Io(String),
}

impl std::fmt::Display for SandboxError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            SandboxError::Escape(p) => write!(f, "path escapes the sandbox: {p}"),
            SandboxError::Io(e) => write!(f, "sandbox path error: {e}"),
        }
    }
}

impl std::error::Error for SandboxError {}

impl From<SandboxError> for crate::error::AikitError {
    fn from(error: SandboxError) -> Self {
        crate::error::AikitError::Sandbox(error.to_string())
    }
}

#[derive(Debug)]
struct JailedPath {
    root: Arc<CapabilityRoot>,
    relative: PathBuf,
    display: PathBuf,
}

#[derive(Debug, Clone, Copy)]
enum FileMode {
    Read,
    Edit,
    Write,
}

/// A lazily openable regular-file entry discovered by a sandboxed directory walk.
pub(crate) struct SandboxedWalkFile<'a> {
    display: PathBuf,
    source: WalkSource<'a>,
}

enum WalkSource<'a> {
    Ambient(&'a Path),
    #[cfg(unix)]
    Capability {
        parent: &'a Dir,
        name: OsString,
    },
}

impl SandboxedWalkFile<'_> {
    pub(crate) fn path(&self) -> &Path {
        &self.display
    }

    /// Open the exact directory entry without following a symlink introduced after enumeration.
    pub(crate) fn open(&self) -> Result<File, SandboxError> {
        match &self.source {
            WalkSource::Ambient(path) => open_ambient_file(path, FileMode::Read),
            #[cfg(unix)]
            WalkSource::Capability { parent, name } => {
                open_file_at(parent, name, FileMode::Read, &self.display)
            }
        }
    }
}

impl Sandbox {
    /// Jail all file access to `root` (canonicalized; must exist).
    pub fn jail(root: impl AsRef<Path>) -> Result<Self, SandboxError> {
        Ok(Self {
            roots: vec![open_capability_root(root.as_ref())?],
            unrestricted: false,
        })
    }

    /// Jail to several roots. Relative paths use the first root; absolute paths may select any
    /// configured root.
    pub fn with_roots(roots: impl IntoIterator<Item = PathBuf>) -> Result<Self, SandboxError> {
        let mut opened = Vec::new();
        for root in roots {
            opened.push(open_capability_root(&root)?);
        }
        Ok(Self {
            roots: opened,
            unrestricted: false,
        })
    }

    /// No restriction — the caller explicitly opts out of the jail (full filesystem access).
    pub fn unrestricted() -> Self {
        Self {
            roots: Vec::new(),
            unrestricted: true,
        }
    }

    pub fn is_unrestricted(&self) -> bool {
        self.unrestricted
    }

    /// The first root's stable display path. Bash uses this only as an ambient working directory;
    /// the descriptor-relative guarantees in this module apply to built-in file tools, not Bash.
    pub fn primary_root(&self) -> Option<&Path> {
        self.roots.first().map(|root| root.path.as_path())
    }

    pub(crate) fn open_read(&self, path: impl AsRef<Path>) -> Result<File, SandboxError> {
        if self.unrestricted {
            return open_ambient_file(path.as_ref(), FileMode::Read);
        }
        self.open_jailed_file(path.as_ref(), FileMode::Read)
    }

    pub(crate) fn open_edit(&self, path: impl AsRef<Path>) -> Result<File, SandboxError> {
        if self.unrestricted {
            return open_ambient_file(path.as_ref(), FileMode::Edit);
        }
        self.open_jailed_file(path.as_ref(), FileMode::Edit)
    }

    pub(crate) fn open_write(&self, path: impl AsRef<Path>) -> Result<File, SandboxError> {
        if self.unrestricted {
            if let Some(parent) = path.as_ref().parent() {
                let _ = std::fs::create_dir_all(parent);
            }
            return open_ambient_file(path.as_ref(), FileMode::Write);
        }
        self.open_jailed_file(path.as_ref(), FileMode::Write)
    }

    /// Walk regular files below `base`. A missing `base` means the primary root. Jailed walks
    /// enumerate and reopen entries relative to held directory descriptors; symlinks are skipped.
    pub(crate) fn walk_files<F>(
        &self,
        base: Option<&Path>,
        mut visit: F,
    ) -> Result<(), SandboxError>
    where
        F: for<'a> FnMut(SandboxedWalkFile<'a>),
    {
        if self.unrestricted {
            let base = base.ok_or_else(|| SandboxError::Io("no sandbox root".into()))?;
            walk_ambient(base, &mut visit);
            return Ok(());
        }

        #[cfg(not(unix))]
        {
            let _ = (base, &mut visit);
            return Err(unsupported_platform());
        }

        #[cfg(unix)]
        {
            let (dir, display) = match base {
                Some(path) => {
                    let mapped = self.map_jailed(path)?;
                    let Some(dir) = open_directory_path(&mapped)? else {
                        // Preserve the previous walk behavior: a regular-file base yields no
                        // recursive entries rather than being treated as the file itself.
                        return Ok(());
                    };
                    (dir, mapped.display)
                }
                None => {
                    let root = self
                        .roots
                        .first()
                        .ok_or_else(|| SandboxError::Io("no sandbox root".into()))?;
                    (
                        root.dir.try_clone().map_err(|error| {
                            SandboxError::Io(format!("clone sandbox root: {error}"))
                        })?,
                        root.path.clone(),
                    )
                }
            };
            walk_capability(&dir, &display, &mut visit);
            Ok(())
        }
    }

    fn open_jailed_file(&self, path: &Path, mode: FileMode) -> Result<File, SandboxError> {
        #[cfg(not(unix))]
        {
            let _ = (path, mode);
            Err(unsupported_platform())
        }

        #[cfg(unix)]
        {
            let mapped = self.map_jailed(path)?;
            if mapped.relative == Path::new(".") {
                return Err(SandboxError::Escape(format!(
                    "{} is not a regular file",
                    mapped.display.display()
                )));
            }
            let create_parents = matches!(mode, FileMode::Write);
            let (parent, name) = open_parent_dir(&mapped, create_parents)?;
            open_file_at(&parent, &name, mode, &mapped.display)
        }
    }

    fn map_jailed(&self, path: &Path) -> Result<JailedPath, SandboxError> {
        if path.is_absolute() {
            let normalized = normalize_absolute(path);
            for root in &self.roots {
                for alias in &root.aliases {
                    if let Ok(relative) = normalized.strip_prefix(alias) {
                        let relative = dot_if_empty(relative);
                        return Ok(JailedPath {
                            root: Arc::clone(root),
                            display: root.path.join(&relative),
                            relative,
                        });
                    }
                }
            }
            Err(SandboxError::Escape(normalized.display().to_string()))
        } else {
            let root = self
                .roots
                .first()
                .ok_or_else(|| SandboxError::Io("no sandbox root".into()))?;
            let relative = normalize_relative(path)?;
            Ok(JailedPath {
                root: Arc::clone(root),
                display: root.path.join(&relative),
                relative,
            })
        }
    }
}

#[cfg(unix)]
fn open_capability_root(path: &Path) -> Result<Arc<CapabilityRoot>, SandboxError> {
    use std::os::unix::fs::MetadataExt;

    let canonical = std::fs::canonicalize(path)
        .map_err(|error| SandboxError::Io(format!("resolve {}: {error}", path.display())))?;
    let dir = Dir::open_ambient_dir(&canonical, ambient_authority())
        .map_err(|error| SandboxError::Io(format!("open {}: {error}", canonical.display())))?;

    // Bind the display path and capability to the same inode. If the root path is exchanged while
    // the jail is being constructed, fail instead of capturing an attacker-selected directory.
    let expected = std::fs::metadata(&canonical)
        .map_err(|error| SandboxError::Io(format!("stat {}: {error}", canonical.display())))?;
    let actual = dir
        .try_clone()
        .and_then(|clone| clone.into_std_file().metadata())
        .map_err(|error| SandboxError::Io(format!("stat open root: {error}")))?;
    if !actual.is_dir() || expected.dev() != actual.dev() || expected.ino() != actual.ino() {
        return Err(SandboxError::Io(format!(
            "sandbox root changed while it was being opened: {}",
            canonical.display()
        )));
    }

    let lexical_absolute = if path.is_absolute() {
        normalize_absolute(path)
    } else {
        let cwd = std::env::current_dir()
            .map_err(|error| SandboxError::Io(format!("resolve current directory: {error}")))?;
        normalize_absolute(&cwd.join(path))
    };
    let mut aliases = vec![canonical.clone()];
    if lexical_absolute != canonical {
        aliases.push(lexical_absolute);
    }

    Ok(Arc::new(CapabilityRoot {
        path: canonical,
        aliases,
        dir,
    }))
}

#[cfg(not(unix))]
fn open_capability_root(_path: &Path) -> Result<Arc<CapabilityRoot>, SandboxError> {
    Err(unsupported_platform())
}

#[cfg(not(unix))]
fn unsupported_platform() -> SandboxError {
    SandboxError::Io(
        "jailed filesystem access requires descriptor-relative Unix support (Linux or macOS)"
            .into(),
    )
}

fn normalize_relative(path: &Path) -> Result<PathBuf, SandboxError> {
    let mut parts = Vec::<OsString>::new();
    for component in path.components() {
        match component {
            Component::CurDir => {}
            Component::Normal(part) => parts.push(part.to_owned()),
            Component::ParentDir => {
                if parts.pop().is_none() {
                    return Err(SandboxError::Escape(path.display().to_string()));
                }
            }
            Component::Prefix(_) | Component::RootDir => {
                return Err(SandboxError::Escape(path.display().to_string()));
            }
        }
    }
    let mut normalized = PathBuf::new();
    for part in parts {
        normalized.push(part);
    }
    Ok(dot_if_empty(&normalized))
}

fn normalize_absolute(path: &Path) -> PathBuf {
    let mut parts = Vec::<OsString>::new();
    for component in path.components() {
        match component {
            Component::Normal(part) => parts.push(part.to_owned()),
            Component::ParentDir => {
                let _ = parts.pop();
            }
            Component::Prefix(_) | Component::RootDir | Component::CurDir => {}
        }
    }
    let mut normalized = PathBuf::from(std::path::MAIN_SEPARATOR_STR);
    for part in parts {
        normalized.push(part);
    }
    normalized
}

fn dot_if_empty(path: &Path) -> PathBuf {
    if path.as_os_str().is_empty() {
        PathBuf::from(".")
    } else {
        path.to_path_buf()
    }
}

fn open_ambient_file(path: &Path, mode: FileMode) -> Result<File, SandboxError> {
    let mut options = std::fs::OpenOptions::new();
    match mode {
        FileMode::Read => {
            options.read(true);
        }
        FileMode::Edit => {
            options.read(true).write(true);
        }
        FileMode::Write => {
            // Truncate only after fstat proves that the opened object is a regular file.
            options.write(true).create(true);
        }
    }
    #[cfg(unix)]
    options.custom_flags(libc::O_NONBLOCK);
    let file = options
        .open(path)
        .map_err(|error| SandboxError::Io(format!("open {}: {error}", path.display())))?;
    finish_regular_file(file, mode, path)
}

#[cfg(unix)]
fn open_parent_dir(
    path: &JailedPath,
    create_missing: bool,
) -> Result<(Dir, OsString), SandboxError> {
    let name = path
        .relative
        .file_name()
        .ok_or_else(|| SandboxError::Io(format!("{} has no file name", path.display.display())))?
        .to_owned();
    let parent = path.relative.parent().unwrap_or_else(|| Path::new("."));
    let dir = traverse_directories(&path.root, parent, create_missing, &path.display)?;
    Ok((dir, name))
}

#[cfg(unix)]
fn traverse_directories(
    root: &CapabilityRoot,
    relative: &Path,
    create_missing: bool,
    display: &Path,
) -> Result<Dir, SandboxError> {
    let mut current = root
        .dir
        .try_clone()
        .map_err(|error| SandboxError::Io(format!("clone sandbox root: {error}")))?;

    for component in relative.components() {
        let Component::Normal(name) = component else {
            continue;
        };
        reject_symlink(&current, name, display)?;
        match open_dir_at(&current, name, display) {
            Ok(next) => current = next,
            Err(SandboxError::Io(_)) if create_missing => {
                match current.create_dir(name) {
                    Ok(()) => {}
                    Err(error) if error.kind() == std::io::ErrorKind::AlreadyExists => {}
                    Err(error) => {
                        return Err(SandboxError::Io(format!(
                            "create directory for {}: {error}",
                            display.display()
                        )));
                    }
                }
                // An attacker may have won the create race with a symlink. The second no-follow
                // check and descriptor-relative open make that race fail closed.
                reject_symlink(&current, name, display)?;
                current = open_dir_at(&current, name, display)?;
            }
            Err(error) => return Err(error),
        }
    }
    Ok(current)
}

#[cfg(unix)]
fn open_directory_path(path: &JailedPath) -> Result<Option<Dir>, SandboxError> {
    if path.relative == Path::new(".") {
        return path
            .root
            .dir
            .try_clone()
            .map(Some)
            .map_err(|error| SandboxError::Io(format!("clone sandbox root: {error}")));
    }

    let name = path
        .relative
        .file_name()
        .ok_or_else(|| SandboxError::Io(format!("{} has no file name", path.display.display())))?;
    let parent_path = path.relative.parent().unwrap_or_else(|| Path::new("."));
    let parent = traverse_directories(&path.root, parent_path, false, &path.display)?;
    reject_symlink(&parent, name, &path.display)?;
    let metadata = parent
        .symlink_metadata(name)
        .map_err(|error| SandboxError::Io(format!("stat {}: {error}", path.display.display())))?;
    if !metadata.is_dir() {
        return Ok(None);
    }
    open_dir_at(&parent, name, &path.display).map(Some)
}

#[cfg(unix)]
fn reject_symlink(dir: &Dir, name: &OsStr, display: &Path) -> Result<(), SandboxError> {
    match dir.symlink_metadata(name) {
        Ok(metadata) if metadata.file_type().is_symlink() => Err(SandboxError::Escape(format!(
            "{} contains a symlink component",
            display.display()
        ))),
        Ok(_) => Ok(()),
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(()),
        Err(error) => Err(SandboxError::Io(format!(
            "stat {}: {error}",
            display.display()
        ))),
    }
}

#[cfg(unix)]
fn open_dir_at(dir: &Dir, name: &OsStr, display: &Path) -> Result<Dir, SandboxError> {
    let mut options = OpenOptions::new();
    options
        .read(true)
        .custom_flags(libc::O_DIRECTORY | libc::O_NOFOLLOW);
    dir.open_with(name, &options)
        .map(|file| Dir::from_std_file(file.into_std()))
        .map_err(|error| capability_error("open directory", display, error))
}

#[cfg(unix)]
fn open_file_at(
    dir: &Dir,
    name: &OsStr,
    mode: FileMode,
    display: &Path,
) -> Result<File, SandboxError> {
    reject_regular_file_boundary(dir, name, mode, display)?;
    let mut options = OpenOptions::new();
    match mode {
        FileMode::Read => {
            options.read(true);
        }
        FileMode::Edit => {
            options.read(true).write(true);
        }
        FileMode::Write => {
            // Avoid O_TRUNC until the opened descriptor has been verified as a regular file.
            options.write(true).create(true);
        }
    }
    options.custom_flags(libc::O_NOFOLLOW | libc::O_NONBLOCK);
    let file = dir
        .open_with(name, &options)
        .map(fs::File::into_std)
        .map_err(|error| capability_error("open file", display, error))?;
    finish_regular_file(file, mode, display)
}

#[cfg(unix)]
fn reject_regular_file_boundary(
    dir: &Dir,
    name: &OsStr,
    mode: FileMode,
    display: &Path,
) -> Result<(), SandboxError> {
    match dir.symlink_metadata(name) {
        Ok(metadata) if metadata.file_type().is_symlink() => Err(SandboxError::Escape(format!(
            "{} contains a symlink component",
            display.display()
        ))),
        Ok(metadata) if !metadata.is_file() => Err(SandboxError::Escape(format!(
            "{} is not a regular file",
            display.display()
        ))),
        Ok(_) => Ok(()),
        Err(error)
            if error.kind() == std::io::ErrorKind::NotFound && matches!(mode, FileMode::Write) =>
        {
            Ok(())
        }
        Err(error) => Err(SandboxError::Io(format!(
            "stat {}: {error}",
            display.display()
        ))),
    }
}

fn finish_regular_file(file: File, mode: FileMode, display: &Path) -> Result<File, SandboxError> {
    let metadata = file.metadata().map_err(|error| {
        SandboxError::Io(format!("stat open file {}: {error}", display.display()))
    })?;
    if !metadata.is_file() {
        return Err(SandboxError::Escape(format!(
            "{} is not a regular file",
            display.display()
        )));
    }
    if matches!(mode, FileMode::Write) {
        file.set_len(0).map_err(|error| {
            SandboxError::Io(format!("truncate {}: {error}", display.display()))
        })?;
    }
    Ok(file)
}

#[cfg(unix)]
fn capability_error(operation: &str, display: &Path, error: std::io::Error) -> SandboxError {
    if error.raw_os_error() == Some(libc::ELOOP) {
        SandboxError::Escape(format!(
            "{} became a symlink while it was being opened",
            display.display()
        ))
    } else {
        SandboxError::Io(format!("{operation} {}: {error}", display.display()))
    }
}

#[cfg(unix)]
fn walk_capability<F>(dir: &Dir, display: &Path, visit: &mut F)
where
    F: for<'a> FnMut(SandboxedWalkFile<'a>),
{
    let Ok(entries) = dir.entries() else {
        return;
    };
    for entry in entries.flatten() {
        let Ok(file_type) = entry.file_type() else {
            continue;
        };
        if file_type.is_symlink() {
            continue;
        }
        let name = entry.file_name();
        let child_display = display.join(&name);
        if file_type.is_dir() {
            if let Ok(child) = open_dir_at(dir, &name, &child_display) {
                walk_capability(&child, &child_display, visit);
            }
        } else if file_type.is_file() {
            visit(SandboxedWalkFile {
                display: child_display,
                source: WalkSource::Capability { parent: dir, name },
            });
        }
    }
}

fn walk_ambient<F>(dir: &Path, visit: &mut F)
where
    F: for<'a> FnMut(SandboxedWalkFile<'a>),
{
    let Ok(entries) = std::fs::read_dir(dir) else {
        return;
    };
    for entry in entries.flatten() {
        let Ok(file_type) = entry.file_type() else {
            continue;
        };
        let path = entry.path();
        if file_type.is_dir() {
            walk_ambient(&path, visit);
        } else if file_type.is_file() {
            visit(SandboxedWalkFile {
                display: path.clone(),
                source: WalkSource::Ambient(&path),
            });
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::{Read, Write};

    fn read(mut file: File) -> String {
        let mut value = String::new();
        file.read_to_string(&mut value).unwrap();
        value
    }

    #[test]
    fn relative_and_absolute_paths_use_the_root_capability() {
        let dir = tempfile::tempdir().unwrap();
        let sb = Sandbox::jail(dir.path()).unwrap();

        let mut target = sb.open_write("nested/a.txt").unwrap();
        target.write_all(b"hi").unwrap();
        drop(target);

        assert_eq!(read(sb.open_read("nested/a.txt").unwrap()), "hi");
        assert_eq!(
            read(sb.open_read(dir.path().join("nested/a.txt")).unwrap()),
            "hi"
        );
    }

    #[test]
    fn multiple_roots_accept_absolute_paths_and_keep_relative_paths_primary() {
        let first = tempfile::tempdir().unwrap();
        let second = tempfile::tempdir().unwrap();
        std::fs::write(first.path().join("first.txt"), "first").unwrap();
        std::fs::write(second.path().join("second.txt"), "second").unwrap();
        let sb = Sandbox::with_roots(vec![
            first.path().to_path_buf(),
            second.path().to_path_buf(),
        ])
        .unwrap();

        assert_eq!(read(sb.open_read("first.txt").unwrap()), "first");
        assert_eq!(
            read(sb.open_read(second.path().join("second.txt")).unwrap()),
            "second"
        );
    }

    #[test]
    fn rejects_dot_dot_and_absolute_escape() {
        let dir = tempfile::tempdir().unwrap();
        let sb = Sandbox::jail(dir.path()).unwrap();
        assert!(matches!(
            sb.open_write("../evil.txt"),
            Err(SandboxError::Escape(_))
        ));
        let error = sb.open_write("/etc/shadow").unwrap_err();
        assert!(matches!(error, SandboxError::Escape(_)));
        let typed: crate::error::AikitError = error.into();
        assert_eq!(typed.info().code, crate::error::ErrorCode::Sandbox);
    }

    #[cfg(unix)]
    #[test]
    fn rejects_final_and_intermediate_symlinks_even_when_they_point_inside() {
        use std::os::unix::fs::symlink;

        let dir = tempfile::tempdir().unwrap();
        std::fs::create_dir(dir.path().join("real")).unwrap();
        std::fs::write(dir.path().join("real/file.txt"), "safe").unwrap();
        symlink("real/file.txt", dir.path().join("final.txt")).unwrap();
        symlink("real", dir.path().join("middle")).unwrap();
        let sb = Sandbox::jail(dir.path()).unwrap();

        for result in [
            sb.open_read("final.txt"),
            sb.open_edit("final.txt"),
            sb.open_write("final.txt"),
            sb.open_read("middle/file.txt"),
            sb.open_write("middle/new.txt"),
        ] {
            assert!(matches!(result, Err(SandboxError::Escape(_))));
        }
    }

    #[cfg(unix)]
    #[test]
    fn root_path_replacement_cannot_redirect_a_later_open() {
        use std::os::unix::fs::symlink;

        let holder = tempfile::tempdir().unwrap();
        let root = holder.path().join("root");
        let captured = holder.path().join("captured");
        let outside = holder.path().join("outside");
        std::fs::create_dir(&root).unwrap();
        std::fs::create_dir(&outside).unwrap();
        std::fs::write(root.join("value.txt"), "captured").unwrap();
        std::fs::write(outside.join("value.txt"), "outside").unwrap();
        let sb = Sandbox::jail(&root).unwrap();

        std::fs::rename(&root, &captured).unwrap();
        symlink(&outside, &root).unwrap();

        assert_eq!(
            read(sb.open_read(root.join("value.txt")).unwrap()),
            "captured"
        );
    }

    #[cfg(unix)]
    #[test]
    fn swapping_an_opened_parent_for_a_symlink_cannot_redirect_the_final_open() {
        use std::os::unix::fs::symlink;

        let holder = tempfile::tempdir().unwrap();
        let root = holder.path().join("root");
        let outside = holder.path().join("outside");
        std::fs::create_dir_all(root.join("work")).unwrap();
        std::fs::create_dir(&outside).unwrap();
        std::fs::write(root.join("work/value.txt"), "inside").unwrap();
        std::fs::write(outside.join("value.txt"), "outside").unwrap();
        let sb = Sandbox::jail(&root).unwrap();
        let mapped = sb.map_jailed(Path::new("work/value.txt")).unwrap();
        let (parent, name) = open_parent_dir(&mapped, false).unwrap();

        std::fs::rename(root.join("work"), root.join("captured")).unwrap();
        symlink(&outside, root.join("work")).unwrap();

        let file = open_file_at(&parent, &name, FileMode::Read, &mapped.display).unwrap();
        assert_eq!(read(file), "inside");
    }

    #[test]
    fn unrestricted_retains_explicit_ambient_access() {
        let sb = Sandbox::unrestricted();
        assert!(sb.is_unrestricted());
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("ambient.txt");
        let mut file = sb.open_write(&path).unwrap();
        file.write_all(b"ambient").unwrap();
        drop(file);
        assert_eq!(read(sb.open_read(&path).unwrap()), "ambient");
    }

    #[cfg(unix)]
    #[test]
    fn non_regular_entries_are_rejected_without_blocking_on_a_fifo() {
        use std::ffi::CString;
        use std::os::unix::ffi::OsStrExt;
        use std::os::unix::fs::FileTypeExt;

        let dir = tempfile::tempdir().unwrap();
        let fifo = dir.path().join("pipe");
        let fifo_c = CString::new(fifo.as_os_str().as_bytes()).unwrap();
        // SAFETY: `fifo_c` is a valid, NUL-terminated path owned for the duration of the call.
        assert_eq!(unsafe { libc::mkfifo(fifo_c.as_ptr(), 0o600) }, 0);
        let sb = Sandbox::jail(dir.path()).unwrap();

        for result in [
            sb.open_read("pipe"),
            sb.open_edit("pipe"),
            sb.open_write("pipe"),
            sb.open_read("."),
        ] {
            assert!(matches!(result, Err(SandboxError::Escape(_))));
        }
        assert!(std::fs::symlink_metadata(&fifo)
            .unwrap()
            .file_type()
            .is_fifo());
    }
}
