//! Sandboxed filesystem tools. In jailed mode every I/O operation is opened relative to a held
//! root-directory capability, with symlink following disabled for every path component.

use crate::error::{AikitError, Result};
use crate::governance::sandbox::{Sandbox, SandboxError};
use serde_json::Value;
use std::io::{Read, Seek, SeekFrom, Write};
use std::path::Path;

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
    let mut content = String::new();
    file.read_to_string(&mut content)
        .map_err(|error| AikitError::ToolExecution(format!("read {path}: {error}")))?;
    Ok(content)
}

pub fn write(sb: &Sandbox, input: &Value) -> Result<String> {
    let path = str_arg(input, "path")?;
    let content = str_arg(input, "content")?;
    let mut file = sb.open_write(path).map_err(denied)?;
    file.write_all(content.as_bytes())
        .map_err(|error| AikitError::ToolExecution(format!("write {path}: {error}")))?;
    Ok(format!("wrote {} bytes to {path}", content.len()))
}

pub fn edit(sb: &Sandbox, input: &Value) -> Result<String> {
    let path = str_arg(input, "path")?;
    let old = str_arg(input, "old_string")?;
    let new = str_arg(input, "new_string")?;
    // Read and write through one open descriptor. A concurrent rename can change the name, but it
    // cannot redirect the write to a different file after the content check.
    let mut file = sb.open_edit(path).map_err(denied)?;
    let mut content = String::new();
    file.read_to_string(&mut content)
        .map_err(|error| AikitError::ToolExecution(format!("read {path}: {error}")))?;
    match content.matches(old).count() {
        0 => Err(AikitError::ToolExecution(format!(
            "old_string not found in {path}"
        ))),
        1 => {
            let updated = content.replacen(old, new, 1);
            file.seek(SeekFrom::Start(0))
                .and_then(|_| file.set_len(0))
                .and_then(|_| file.write_all(updated.as_bytes()))
                .map_err(|error| AikitError::ToolExecution(format!("write {path}: {error}")))?;
            Ok(format!("edited {path}"))
        }
        n => Err(AikitError::ToolExecution(format!(
            "old_string is not unique in {path} ({n} matches) — add more context"
        ))),
    }
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
    sb.walk_files(requested_base.map(Path::new), |entry| {
        if let Ok(mut file) = entry.open() {
            let mut text = String::new();
            if file.read_to_string(&mut text).is_err() {
                return;
            }
            for (i, line) in text.lines().enumerate() {
                if re.is_match(line) {
                    hits.push(format!(
                        "{}:{}: {}",
                        entry.path().display(),
                        i + 1,
                        line.trim()
                    ));
                }
            }
        }
    })
    .map_err(denied)?;
    if hits.is_empty() {
        Ok("no matches".into())
    } else {
        hits.truncate(200);
        Ok(hits.join("\n"))
    }
}

pub fn glob(sb: &Sandbox, input: &Value) -> Result<String> {
    let pattern = str_arg(input, "pattern")?;
    if sb.primary_root().is_none() {
        return Err(AikitError::ToolExecution("no root".into()));
    }
    let mut matches = Vec::new();
    sb.walk_files(None, |entry| {
        let name = entry
            .path()
            .file_name()
            .and_then(|name| name.to_str())
            .unwrap_or("");
        if glob_match(pattern, name) {
            matches.push(entry.path().display().to_string());
        }
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
}
