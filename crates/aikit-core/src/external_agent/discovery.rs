use super::{
    ExternalAgentError, ExternalAgentErrorCode, ExternalAgentKind, ExternalAgentResult,
    RuntimeCandidate, RuntimeDiscoverySource, RuntimeInstallOrigin,
};
use std::collections::BTreeMap;
use std::ffi::OsStr;
use std::path::{Path, PathBuf};

pub fn discover_runtime_candidates(
    kind: ExternalAgentKind,
    explicit: Option<&Path>,
) -> ExternalAgentResult<Vec<RuntimeCandidate>> {
    let path = std::env::var_os("PATH");
    discover_runtime_candidates_from(kind, explicit, path.as_deref(), known_locations(kind))
}

/// Resolve one support executable (for example `npx`) without invoking a shell or the executable.
pub fn discover_support_executable(command: &str) -> ExternalAgentResult<PathBuf> {
    if command.is_empty()
        || command.len() > 64
        || !command
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'-' | b'_'))
    {
        return Err(ExternalAgentError::invalid_input(
            "support executable name must be 1-64 ASCII letters, digits, '-' or '_'",
        ));
    }
    let path = std::env::var_os("PATH").ok_or_else(|| {
        ExternalAgentError::new(
            ExternalAgentErrorCode::NotFound,
            format!("support executable `{command}` was not found because PATH is unset"),
        )
    })?;
    for directory in std::env::split_paths(&path) {
        for name in executable_names(command) {
            let path = directory.join(name);
            let Ok(metadata) = std::fs::metadata(&path) else {
                continue;
            };
            if metadata.is_file() && is_executable(&metadata) {
                return std::fs::canonicalize(&path).map_err(|error| {
                    ExternalAgentError::io(format!(
                        "failed to canonicalize support executable {}: {error}",
                        path.display()
                    ))
                });
            }
        }
    }
    Err(ExternalAgentError::new(
        ExternalAgentErrorCode::NotFound,
        format!("support executable `{command}` was not found on PATH"),
    ))
}

fn discover_runtime_candidates_from(
    kind: ExternalAgentKind,
    explicit: Option<&Path>,
    path: Option<&OsStr>,
    known: Vec<PathBuf>,
) -> ExternalAgentResult<Vec<RuntimeCandidate>> {
    let mut paths = Vec::new();
    if let Some(path) = explicit {
        paths.push((path.to_path_buf(), RuntimeDiscoverySource::Explicit));
    }

    if let Some(path) = path {
        for directory in std::env::split_paths(&path) {
            for name in executable_names(kind.command_name()) {
                paths.push((directory.join(name), RuntimeDiscoverySource::Path));
            }
        }
    }

    for path in known {
        paths.push((path, RuntimeDiscoverySource::KnownLocation));
    }

    let mut candidates: Vec<RuntimeCandidate> = Vec::new();
    let mut by_real_path: BTreeMap<PathBuf, usize> = BTreeMap::new();
    for (path, source) in paths {
        match inspect_candidate(kind, &path, source) {
            Ok(candidate) => {
                if let Some(index) = by_real_path.get(&candidate.real_executable).copied() {
                    let existing = &mut candidates[index];
                    if !existing.aliases.contains(&candidate.executable)
                        && existing.executable != candidate.executable
                    {
                        existing.aliases.push(candidate.executable);
                    }
                    if !existing.discovered_by.contains(&source) {
                        existing.discovered_by.push(source);
                    }
                } else {
                    by_real_path.insert(candidate.real_executable.clone(), candidates.len());
                    candidates.push(candidate);
                }
            }
            // An explicit path is an operator decision, not a hint. Falling back to another PATH
            // entry could launch a different account, version, or install origin than requested.
            Err(error) if source == RuntimeDiscoverySource::Explicit => return Err(error),
            Err(_) => {}
        }
    }
    Ok(candidates)
}

fn inspect_candidate(
    kind: ExternalAgentKind,
    path: &Path,
    source: RuntimeDiscoverySource,
) -> ExternalAgentResult<RuntimeCandidate> {
    let metadata = std::fs::metadata(path).map_err(|error| {
        ExternalAgentError::new(
            ExternalAgentErrorCode::NotFound,
            format!("external agent executable {}: {error}", path.display()),
        )
    })?;
    if !metadata.is_file() {
        return Err(ExternalAgentError::new(
            ExternalAgentErrorCode::NotExecutable,
            format!(
                "external agent path {} is not a regular file",
                path.display()
            ),
        ));
    }
    if !is_executable(&metadata) {
        return Err(ExternalAgentError::new(
            ExternalAgentErrorCode::NotExecutable,
            format!("external agent path {} is not executable", path.display()),
        ));
    }
    let real_executable = std::fs::canonicalize(path).map_err(|error| {
        ExternalAgentError::io(format!(
            "failed to canonicalize external agent path {}: {error}",
            path.display()
        ))
    })?;
    let executable = absolute_path(path)?;
    Ok(RuntimeCandidate {
        kind,
        origin: if source == RuntimeDiscoverySource::Explicit {
            let detected = runtime_install_origin(&real_executable);
            if detected == RuntimeInstallOrigin::Unknown {
                RuntimeInstallOrigin::ExplicitPath
            } else {
                detected
            }
        } else {
            runtime_install_origin(&real_executable)
        },
        executable,
        real_executable,
        discovered_by: vec![source],
        aliases: Vec::new(),
    })
}

fn absolute_path(path: &Path) -> ExternalAgentResult<PathBuf> {
    if path.is_absolute() {
        return Ok(path.to_path_buf());
    }
    std::env::current_dir()
        .map(|cwd| cwd.join(path))
        .map_err(|error| {
            ExternalAgentError::io(format!("failed to resolve current directory: {error}"))
        })
}

#[cfg(unix)]
fn is_executable(metadata: &std::fs::Metadata) -> bool {
    use std::os::unix::fs::PermissionsExt;
    metadata.permissions().mode() & 0o111 != 0
}

#[cfg(not(unix))]
fn is_executable(_metadata: &std::fs::Metadata) -> bool {
    true
}

fn executable_names(command: &str) -> Vec<String> {
    #[cfg(windows)]
    {
        let extensions = std::env::var_os("PATHEXT")
            .and_then(|value| value.into_string().ok())
            .unwrap_or_else(|| ".COM;.EXE;.BAT;.CMD".into());
        let mut names = vec![command.to_string()];
        names.extend(
            extensions
                .split(';')
                .filter(|extension| !extension.trim().is_empty())
                .map(|extension| format!("{command}{}", extension.to_ascii_lowercase())),
        );
        names
    }
    #[cfg(not(windows))]
    {
        vec![command.to_string()]
    }
}

fn known_locations(kind: ExternalAgentKind) -> Vec<PathBuf> {
    let mut paths = Vec::new();
    if let Some(home) = home_directory() {
        match kind {
            ExternalAgentKind::Codex => {}
            ExternalAgentKind::Gemini => {}
            ExternalAgentKind::Claude => paths.push(home.join(".local/bin/claude")),
            ExternalAgentKind::Grok => {
                paths.push(home.join(".grok/bin/grok"));
                paths.push(home.join(".local/bin/grok"));
            }
        }
    }
    #[cfg(target_os = "macos")]
    if kind == ExternalAgentKind::Codex {
        paths.push(PathBuf::from(
            "/Applications/ChatGPT.app/Contents/Resources/codex",
        ));
    }
    paths
}

fn home_directory() -> Option<PathBuf> {
    std::env::var_os("HOME")
        .or_else(|| std::env::var_os("USERPROFILE"))
        .map(PathBuf::from)
}

pub fn runtime_install_origin(path: &Path) -> RuntimeInstallOrigin {
    let normalized = path
        .to_string_lossy()
        .replace('\\', "/")
        .to_ascii_lowercase();
    if let Some(home) = std::env::var_os("AIKIT_HOME") {
        let home = PathBuf::from(home);
        if path.starts_with(home.join("runtimes")) {
            return RuntimeInstallOrigin::AikitManaged;
        }
    }
    if normalized.contains("/node_modules/") {
        RuntimeInstallOrigin::Npm
    } else if normalized.contains("/homebrew/")
        || normalized.starts_with("/opt/homebrew/")
        || normalized.contains("/caskroom/")
        || normalized.contains("/cellar/")
    {
        RuntimeInstallOrigin::Homebrew
    } else if normalized.contains("/.grok/bin/") || normalized.contains("/.local/share/claude/") {
        RuntimeInstallOrigin::VendorNative
    } else if normalized.contains(".app/contents/") {
        RuntimeInstallOrigin::ApplicationBundle
    } else {
        RuntimeInstallOrigin::Unknown
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn install_origin_recognizes_managers_without_executing_them() {
        assert_eq!(
            runtime_install_origin(Path::new(
                "/opt/homebrew/lib/node_modules/@google/gemini-cli/gemini.js"
            )),
            RuntimeInstallOrigin::Npm
        );
        assert_eq!(
            runtime_install_origin(Path::new("/Users/test/.grok/bin/grok-1")),
            RuntimeInstallOrigin::VendorNative
        );
        assert_eq!(
            runtime_install_origin(Path::new(
                "/Applications/ChatGPT.app/Contents/Resources/codex"
            )),
            RuntimeInstallOrigin::ApplicationBundle
        );
    }

    #[test]
    fn support_executable_names_reject_paths() {
        assert!(discover_support_executable("../npx").is_err());
        assert!(discover_support_executable("npx;other").is_err());
    }

    #[cfg(unix)]
    #[test]
    fn explicit_and_path_aliases_are_deduplicated_by_real_path() {
        use std::os::unix::fs::{symlink, PermissionsExt};

        let directory = tempfile::tempdir().unwrap();
        let real = directory.path().join("codex-real");
        std::fs::write(&real, b"#!/bin/sh\n").unwrap();
        std::fs::set_permissions(&real, std::fs::Permissions::from_mode(0o700)).unwrap();
        let alias = directory.path().join("codex");
        symlink(&real, &alias).unwrap();

        let candidates = discover_runtime_candidates_from(
            ExternalAgentKind::Codex,
            Some(&real),
            Some(directory.path().as_os_str()),
            Vec::new(),
        )
        .unwrap();

        assert_eq!(candidates.len(), 1);
        assert_eq!(
            candidates[0].real_executable,
            std::fs::canonicalize(real).unwrap()
        );
        assert!(candidates[0].aliases.contains(&alias));
        assert_eq!(
            candidates[0].discovered_by,
            vec![
                RuntimeDiscoverySource::Explicit,
                RuntimeDiscoverySource::Path
            ]
        );
    }

    #[cfg(unix)]
    #[test]
    fn explicit_non_executable_is_rejected() {
        let directory = tempfile::tempdir().unwrap();
        let path = directory.path().join("gemini");
        std::fs::write(&path, b"not executable").unwrap();
        let error =
            discover_runtime_candidates(ExternalAgentKind::Gemini, Some(&path)).unwrap_err();
        assert_eq!(error.code, ExternalAgentErrorCode::NotExecutable);
    }
}
