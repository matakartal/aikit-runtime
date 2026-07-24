//! Sandboxed filesystem tools. In jailed mode every I/O operation is opened relative to a held
//! root-directory capability, with symlink following disabled for every path component.

use crate::error::{AikitError, Result};
use crate::governance::sandbox::{Sandbox, SandboxError};
use serde_json::Value;
use std::io::Read;
use std::path::Path;

const MAX_READ_BYTES: u64 = 1_000_000;
const MAX_WRITE_BYTES: usize = 1_000_000;
const MAX_SEARCH_FILE_BYTES: u64 = 1_000_000;
const MAX_SEARCHED_FILES: usize = 10_000;
const MAX_GREP_HITS: usize = 200;
const MAX_GREP_LINE_CHARS: usize = 4_096;
const MAX_GLOB_MATCHES: usize = 200;

fn str_arg<'a>(input: &'a Value, key: &str) -> Result<&'a str> {
    input
        .get(key)
        .and_then(Value::as_str)
        .ok_or_else(|| AikitError::ToolExecution(format!("missing '{key}' argument")))
}

/// A sandbox boundary failure remains distinguishable from a governance permission decision.
fn denied(e: SandboxError) -> AikitError {
    e.into()
}

pub fn read(sb: &Sandbox, input: &Value) -> Result<String> {
    let path = str_arg(input, "path")?;
    let mut file = sb.open_read(path).map_err(denied)?;
    read_bounded(&mut file, path, MAX_READ_BYTES)
}

pub fn write(sb: &Sandbox, input: &Value) -> Result<String> {
    let path = str_arg(input, "path")?;
    let content = str_arg(input, "content")?;
    if content.len() > MAX_WRITE_BYTES {
        return Err(AikitError::ToolExecution(format!(
            "write {path}: content exceeds {MAX_WRITE_BYTES} bytes"
        )));
    }
    sb.replace_write(path, content.as_bytes()).map_err(denied)?;
    Ok(format!("wrote {} bytes to {path}", content.len()))
}

pub fn edit(sb: &Sandbox, input: &Value) -> Result<String> {
    let path = str_arg(input, "path")?;
    let old = str_arg(input, "old_string")?;
    let new = str_arg(input, "new_string")?;
    // Read and write through one open descriptor. A concurrent rename can change the name, but it
    // cannot redirect the write to a different file after the content check.
    let mut file = sb.open_edit(path).map_err(denied)?;
    let content = read_bounded(&mut file, path, MAX_READ_BYTES)?;
    match content.matches(old).count() {
        0 => Err(AikitError::ToolExecution(format!(
            "old_string not found in {path}"
        ))),
        1 => {
            let updated = content.replacen(old, new, 1);
            if updated.len() as u64 > MAX_READ_BYTES {
                return Err(AikitError::ToolExecution(format!(
                    "edited content for {path} exceeds {MAX_READ_BYTES} bytes"
                )));
            }
            sb.commit_edit(path, &mut file, updated.as_bytes())
                .map_err(denied)?;
            Ok(format!("edited {path}"))
        }
        n => Err(AikitError::ToolExecution(format!(
            "old_string is not unique in {path} ({n} matches) — add more context"
        ))),
    }
}

fn read_bounded(file: &mut std::fs::File, path: &str, max_bytes: u64) -> Result<String> {
    let mut content = String::new();
    file.take(max_bytes.saturating_add(1))
        .read_to_string(&mut content)
        .map_err(|error| AikitError::ToolExecution(format!("read {path}: {error}")))?;
    if content.len() as u64 > max_bytes {
        return Err(AikitError::ToolExecution(format!(
            "read {path}: file exceeds {max_bytes} bytes"
        )));
    }
    Ok(content)
}

pub fn grep(sb: &Sandbox, input: &Value) -> Result<String> {
    let pattern = str_arg(input, "pattern")?;
    let re = regex::Regex::new(pattern)
        .map_err(|e| AikitError::ToolExecution(format!("bad regex: {e}")))?;
    let requested_base = input.get("path").and_then(Value::as_str);
    if requested_base.is_none() && sb.primary_root().is_none() {
        return Err(AikitError::ToolExecution("no search root".into()));
    }
    let mut hits = Vec::new();
    let mut searched_files = 0usize;
    sb.walk_files(requested_base.map(Path::new), MAX_SEARCHED_FILES, |entry| {
        if searched_files >= MAX_SEARCHED_FILES || hits.len() >= MAX_GREP_HITS {
            return false;
        }
        searched_files += 1;
        if let Ok(file) = entry.open() {
            let mut text = String::new();
            if file
                .take(MAX_SEARCH_FILE_BYTES.saturating_add(1))
                .read_to_string(&mut text)
                .is_err()
                || text.len() as u64 > MAX_SEARCH_FILE_BYTES
            {
                return true;
            }
            for (i, line) in text.lines().enumerate() {
                if re.is_match(line) {
                    let line = bounded_line(line.trim(), MAX_GREP_LINE_CHARS);
                    hits.push(format!("{}:{}: {}", entry.path().display(), i + 1, line));
                    if hits.len() >= MAX_GREP_HITS {
                        break;
                    }
                }
            }
        }
        searched_files < MAX_SEARCHED_FILES && hits.len() < MAX_GREP_HITS
    })
    .map_err(denied)?;
    if hits.is_empty() {
        Ok("no matches".into())
    } else {
        Ok(hits.join("\n"))
    }
}

fn bounded_line(line: &str, max_chars: usize) -> String {
    let mut chars = line.chars();
    let mut output: String = chars.by_ref().take(max_chars).collect();
    if chars.next().is_some() {
        output.push('…');
    }
    output
}

pub fn glob(sb: &Sandbox, input: &Value) -> Result<String> {
    let pattern = str_arg(input, "pattern")?;
    if sb.primary_root().is_none() {
        return Err(AikitError::ToolExecution("no root".into()));
    }
    let mut matches = Vec::new();
    let mut searched_files = 0usize;
    sb.walk_files(None, MAX_SEARCHED_FILES, |entry| {
        if searched_files >= MAX_SEARCHED_FILES || matches.len() >= MAX_GLOB_MATCHES {
            return false;
        }
        searched_files += 1;
        let name = entry
            .path()
            .file_name()
            .and_then(|name| name.to_str())
            .unwrap_or("");
        if glob_match(pattern, name) {
            matches.push(entry.path().display().to_string());
        }
        searched_files < MAX_SEARCHED_FILES && matches.len() < MAX_GLOB_MATCHES
    })
    .map_err(denied)?;
    if matches.is_empty() {
        Ok("no matches".into())
    } else {
        Ok(matches.join("\n"))
    }
}

/// Minimal basename glob: `*` = any run, `?` = one char. Not a full path glob.
fn glob_match(pattern: &str, name: &str) -> bool {
    let mut re = String::from("^");
    for c in pattern.chars() {
        match c {
            '*' => re.push_str(".*"),
            '?' => re.push('.'),
            '.' | '+' | '(' | ')' | '[' | ']' | '{' | '}' | '^' | '$' | '\\' | '|' => {
                re.push('\\');
                re.push(c);
            }
            other => re.push(other),
        }
    }
    re.push('$');
    regex::Regex::new(&re)
        .map(|r| r.is_match(name))
        .unwrap_or(false)
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn nested_write_read_edit_grep_and_glob_preserve_behavior() {
        let dir = tempfile::tempdir().unwrap();
        let sandbox = Sandbox::jail(dir.path()).unwrap();

        assert_eq!(
            write(
                &sandbox,
                &json!({"path": "nested/a.log", "content": "hello ERROR"})
            )
            .unwrap(),
            "wrote 11 bytes to nested/a.log"
        );
        assert_eq!(
            read(&sandbox, &json!({"path": "nested/a.log"})).unwrap(),
            "hello ERROR"
        );
        edit(
            &sandbox,
            &json!({
                "path": "nested/a.log",
                "old_string": "ERROR",
                "new_string": "fixed"
            }),
        )
        .unwrap();
        assert!(grep(&sandbox, &json!({"pattern": "fixed"}))
            .unwrap()
            .contains("nested/a.log:1: hello fixed"));
        assert!(glob(&sandbox, &json!({"pattern": "*.log"}))
            .unwrap()
            .contains("nested/a.log"));
    }

    #[cfg(unix)]
    #[test]
    fn all_file_tools_reject_final_and_intermediate_symlinks() {
        use std::os::unix::fs::symlink;

        let holder = tempfile::tempdir().unwrap();
        let root = holder.path().join("root");
        let outside = holder.path().join("outside");
        std::fs::create_dir(&root).unwrap();
        std::fs::create_dir(&outside).unwrap();
        std::fs::write(outside.join("secret.txt"), "outside secret").unwrap();
        symlink(outside.join("secret.txt"), root.join("final.txt")).unwrap();
        symlink(&outside, root.join("middle")).unwrap();
        let sandbox = Sandbox::jail(&root).unwrap();

        let calls = [
            read(&sandbox, &json!({"path": "final.txt"})),
            write(
                &sandbox,
                &json!({"path": "final.txt", "content": "overwrite"}),
            ),
            edit(
                &sandbox,
                &json!({
                    "path": "final.txt",
                    "old_string": "outside",
                    "new_string": "changed"
                }),
            ),
            read(&sandbox, &json!({"path": "middle/secret.txt"})),
            write(
                &sandbox,
                &json!({"path": "middle/new.txt", "content": "escape"}),
            ),
            grep(&sandbox, &json!({"path": "middle", "pattern": "secret"})),
        ];
        assert!(calls
            .iter()
            .all(|result| matches!(result, Err(AikitError::Sandbox(_)))));
        assert_eq!(
            std::fs::read_to_string(outside.join("secret.txt")).unwrap(),
            "outside secret"
        );
        assert!(!outside.join("new.txt").exists());

        // Glob recursively walks the real root capability and never descends through the link.
        assert_eq!(
            glob(&sandbox, &json!({"pattern": "secret.txt"})).unwrap(),
            "no matches"
        );
    }

    #[cfg(unix)]
    #[test]
    fn content_file_tools_reject_hard_link_aliases_to_outside_inodes() {
        let holder = tempfile::tempdir().unwrap();
        let root = holder.path().join("root");
        let outside = holder.path().join("outside.txt");
        std::fs::create_dir(&root).unwrap();
        std::fs::write(&outside, "outside secret").unwrap();
        std::fs::hard_link(&outside, root.join("alias.txt")).unwrap();
        let sandbox = Sandbox::jail(&root).unwrap();

        for result in [
            read(&sandbox, &json!({"path": "alias.txt"})),
            write(
                &sandbox,
                &json!({"path": "alias.txt", "content": "overwrite"}),
            ),
            edit(
                &sandbox,
                &json!({
                    "path": "alias.txt",
                    "old_string": "outside",
                    "new_string": "changed"
                }),
            ),
        ] {
            assert!(matches!(result, Err(AikitError::Sandbox(_))));
        }
        assert_eq!(std::fs::read_to_string(&outside).unwrap(), "outside secret");
        assert_eq!(
            grep(&sandbox, &json!({"pattern": "secret"})).unwrap(),
            "no matches"
        );
        // Listing the name is harmless; the content-bearing open is the protected boundary.
        assert!(glob(&sandbox, &json!({"pattern": "alias.txt"}))
            .unwrap()
            .ends_with("alias.txt"));
    }

    #[cfg(unix)]
    #[test]
    fn jailed_write_and_edit_replace_the_inode_and_preserve_permissions() {
        use std::os::unix::fs::{MetadataExt, PermissionsExt};

        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("replace.txt");
        std::fs::write(&path, "first value").unwrap();
        std::fs::set_permissions(&path, std::fs::Permissions::from_mode(0o640)).unwrap();
        let sandbox = Sandbox::jail(dir.path()).unwrap();
        let original = std::fs::metadata(&path).unwrap();

        write(
            &sandbox,
            &json!({"path": "replace.txt", "content": "second value"}),
        )
        .unwrap();
        let after_write = std::fs::metadata(&path).unwrap();
        assert_ne!(after_write.ino(), original.ino());
        assert_eq!(after_write.permissions().mode() & 0o777, 0o640);

        edit(
            &sandbox,
            &json!({
                "path": "replace.txt",
                "old_string": "second",
                "new_string": "third"
            }),
        )
        .unwrap();
        let after_edit = std::fs::metadata(&path).unwrap();
        assert_ne!(after_edit.ino(), after_write.ino());
        assert_eq!(after_edit.permissions().mode() & 0o777, 0o640);
        assert_eq!(std::fs::read_to_string(path).unwrap(), "third value");
    }

    #[test]
    fn grep_keeps_the_existing_two_hundred_hit_limit() {
        let dir = tempfile::tempdir().unwrap();
        let sandbox = Sandbox::jail(dir.path()).unwrap();
        let content = (0..250)
            .map(|index| format!("MATCH {index}"))
            .collect::<Vec<_>>()
            .join("\n");
        write(&sandbox, &json!({"path": "many.txt", "content": content})).unwrap();

        let output = grep(&sandbox, &json!({"pattern": "MATCH"})).unwrap();
        assert_eq!(output.lines().count(), 200);
    }

    #[test]
    fn read_rejects_files_over_the_memory_budget() {
        let dir = tempfile::tempdir().unwrap();
        let sandbox = Sandbox::jail(dir.path()).unwrap();
        std::fs::write(
            dir.path().join("oversized.txt"),
            vec![b'x'; MAX_READ_BYTES as usize + 1],
        )
        .unwrap();

        let error = read(&sandbox, &json!({"path": "oversized.txt"})).unwrap_err();
        assert!(error.to_string().contains("file exceeds 1000000 bytes"));
    }

    #[test]
    fn oversized_write_is_rejected_before_the_existing_file_is_truncated() {
        let dir = tempfile::tempdir().unwrap();
        let sandbox = Sandbox::jail(dir.path()).unwrap();
        std::fs::write(dir.path().join("existing.txt"), "preserve me").unwrap();

        let error = write(
            &sandbox,
            &json!({
                "path": "existing.txt",
                "content": "x".repeat(MAX_WRITE_BYTES + 1)
            }),
        )
        .unwrap_err();
        assert!(error.to_string().contains("content exceeds 1000000 bytes"));
        assert_eq!(
            std::fs::read_to_string(dir.path().join("existing.txt")).unwrap(),
            "preserve me"
        );
    }

    #[test]
    fn grep_skips_oversized_files_and_bounds_each_rendered_line() {
        let dir = tempfile::tempdir().unwrap();
        let sandbox = Sandbox::jail(dir.path()).unwrap();
        let mut oversized = b"MATCH ".to_vec();
        oversized.resize(MAX_SEARCH_FILE_BYTES as usize + 1, b'x');
        std::fs::write(dir.path().join("oversized.txt"), oversized).unwrap();
        std::fs::write(
            dir.path().join("long-line.txt"),
            format!("MATCH {}", "y".repeat(MAX_GREP_LINE_CHARS + 100)),
        )
        .unwrap();

        let output = grep(&sandbox, &json!({"pattern": "MATCH"})).unwrap();
        assert!(!output.contains("oversized.txt"));
        assert!(output.contains("long-line.txt"));
        assert!(output.ends_with('…'));
        assert!(output.len() < MAX_GREP_LINE_CHARS + 500);
    }

    #[test]
    fn glob_caps_the_number_of_retained_matches() {
        let dir = tempfile::tempdir().unwrap();
        let sandbox = Sandbox::jail(dir.path()).unwrap();
        for index in 0..250 {
            std::fs::write(dir.path().join(format!("match-{index}.txt")), "x").unwrap();
        }

        let output = glob(&sandbox, &json!({"pattern": "*.txt"})).unwrap();
        assert_eq!(output.lines().count(), MAX_GLOB_MATCHES);
    }
}
