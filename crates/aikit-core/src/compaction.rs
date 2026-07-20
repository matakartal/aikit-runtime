//! Transcript compaction — keep a long-running conversation inside the context window.
//!
//! The in-process loop appends an assistant turn plus tool results every round, so `cfg.messages`
//! grows without bound. Past a token budget the provider will reject the request (or silently drop
//! the head). Compaction bounds the transcript: it keeps the first message (the task anchor) and
//! the most recent turns verbatim, and replaces the omitted middle with one condensed note.
//!
//! # Two invariants it must not break
//!
//! 1. **Reasoning replay** happens only inside an *active* tool loop — i.e. among the most recent
//!    turns, which are always kept verbatim. Older reasoning blocks are safe to drop.
//! 2. **Tool pairing**: a `tool_result` must be preceded by its `tool_use`. The kept tail must
//!    therefore never *start* with an orphan `tool_result` whose `tool_use` was compacted away —
//!    [`compact_messages`] advances the cut forward until the tail begins on a clean boundary.
//!
//! The default note is extractive (a truncated concatenation of the dropped text) so compaction is
//! deterministic and keyless. A model-generated summary (as some coding agents do, compacting at
//! ~85% of the context window with a cheap model) is the richer follow-up; the policy shape leaves
//! room for it.

use crate::types::{ContentBlock, Message};

/// When and how much of the transcript to compact.
#[derive(Debug, Clone)]
pub struct CompactionPolicy {
    /// Approximate token ceiling for the working transcript. When the *estimated* tokens exceed
    /// this, older turns are compacted. `0` disables compaction (the default) — no behaviour change
    /// for callers who do not opt in.
    pub max_context_tokens: u64,
    /// How many of the most recent messages to always keep verbatim (never compacted). The cut may
    /// move *earlier* than this to preserve tool pairing, never later.
    pub keep_recent_messages: usize,
}

impl Default for CompactionPolicy {
    /// Disabled. Compaction is strictly opt-in so existing runs are unaffected.
    fn default() -> Self {
        CompactionPolicy {
            max_context_tokens: 0,
            keep_recent_messages: 8,
        }
    }
}

impl CompactionPolicy {
    /// Enable compaction at `max_context_tokens`, keeping the `keep_recent` most recent messages.
    pub fn new(max_context_tokens: u64, keep_recent: usize) -> Self {
        CompactionPolicy {
            max_context_tokens,
            keep_recent_messages: keep_recent.max(1),
        }
    }

    pub fn is_enabled(&self) -> bool {
        self.max_context_tokens > 0
    }
}

/// A rough token estimate (~4 chars/token + a small per-message overhead). This is intentionally an
/// *estimate*, not a provider-exact tokenizer — enough to decide when to compact without pulling in
/// per-provider tokenizer tables.
pub fn estimate_tokens(messages: &[Message]) -> u64 {
    messages.iter().map(estimate_message_tokens).sum()
}

fn estimate_message_tokens(m: &Message) -> u64 {
    let chars: usize = m
        .content
        .iter()
        .map(|b| match b {
            ContentBlock::Text { text } => text.len(),
            ContentBlock::Reasoning { text, .. } => text.len(),
            ContentBlock::ToolResult { content, .. } => content.len(),
            ContentBlock::ToolUse { name, input, .. } => name.len() + input.to_string().len(),
            ContentBlock::Citation { text, .. } => text.len(),
            // Media is heavy and not text; charge a rough flat cost.
            ContentBlock::Media { .. } | ContentBlock::MediaInput { .. } => 512,
        })
        .sum();
    (chars as u64 / 4) + 4
}

fn has_tool_result(m: &Message) -> bool {
    m.content
        .iter()
        .any(|b| matches!(b, ContentBlock::ToolResult { .. }))
}

/// Compact `messages` if the policy is enabled and the estimate exceeds the budget. Returns the new
/// transcript, or `None` when no compaction is needed or possible.
///
/// Result shape: `[ first message, condensed note, ...recent tail ]`. The tail is guaranteed not to
/// begin with an orphan `tool_result`.
pub fn compact_messages(messages: &[Message], policy: &CompactionPolicy) -> Option<Vec<Message>> {
    if !policy.is_enabled() || estimate_tokens(messages) <= policy.max_context_tokens {
        return None;
    }
    let len = messages.len();

    // Provisional cut: keep the last `keep_recent_messages`.
    let mut cut = len.saturating_sub(policy.keep_recent_messages);
    // Never start the kept tail on an orphan tool_result — advance past any leading ones.
    while cut < len && has_tool_result(&messages[cut]) {
        cut += 1;
    }
    // Need to actually drop something (indices 1..cut) and keep a non-empty tail.
    if cut <= 1 || cut >= len {
        return None;
    }

    let dropped = &messages[1..cut];
    let mut out = Vec::with_capacity(2 + (len - cut));
    out.push(messages[0].clone());
    out.push(summarize_dropped(dropped));
    out.extend_from_slice(&messages[cut..]);
    Some(out)
}

/// Build the condensed note that replaces the dropped middle. Extractive and deterministic.
fn summarize_dropped(dropped: &[Message]) -> Message {
    let mut text = String::new();
    for m in dropped {
        for b in &m.content {
            match b {
                ContentBlock::Text { text: t } => {
                    text.push_str(t.trim());
                    text.push(' ');
                }
                ContentBlock::ToolUse { name, .. } => {
                    text.push_str(&format!("[tool {name}] "));
                }
                ContentBlock::ToolResult { content, .. } => {
                    let head: String = content.chars().take(80).collect();
                    text.push_str(&format!("[result: {}] ", head.trim()));
                }
                _ => {}
            }
        }
    }
    let condensed: String = text.trim().chars().take(1000).collect();
    // A user-role note is safe to inject mid-conversation on every provider (unlike a mid-list
    // system message, which some adapters hoist).
    Message::user(format!(
        "[aikit compacted {} earlier message(s) to fit the context window. Condensed summary of the omitted turns:\n{condensed}]",
        dropped.len()
    ))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::types::{ContentBlock, Message, Role};
    use serde_json::json;

    fn text_msg(role: Role, body: &str) -> Message {
        Message {
            role,
            content: vec![ContentBlock::Text { text: body.into() }],
        }
    }

    fn assistant_tool_use(id: &str) -> Message {
        Message {
            role: Role::Assistant,
            content: vec![ContentBlock::ToolUse {
                id: id.into(),
                name: "search".into(),
                input: json!({}),
            }],
        }
    }

    fn tool_result(id: &str) -> Message {
        Message {
            role: Role::User,
            content: vec![ContentBlock::ToolResult {
                tool_use_id: id.into(),
                content: "rows".into(),
                is_error: false,
            }],
        }
    }

    #[test]
    fn disabled_policy_never_compacts() {
        let msgs = vec![text_msg(Role::User, &"x".repeat(10_000)); 20];
        assert!(compact_messages(&msgs, &CompactionPolicy::default()).is_none());
    }

    #[test]
    fn under_budget_is_left_alone() {
        let msgs = vec![text_msg(Role::User, "short"); 4];
        let policy = CompactionPolicy::new(10_000, 2);
        assert!(compact_messages(&msgs, &policy).is_none());
    }

    #[test]
    fn over_budget_keeps_anchor_and_tail_and_shrinks() {
        // 20 fat messages; a tiny budget forces compaction.
        let mut msgs = vec![text_msg(Role::User, "TASK: do the thing")];
        for i in 0..20 {
            msgs.push(text_msg(
                Role::Assistant,
                &format!("turn {i} {}", "y".repeat(200)),
            ));
        }
        let policy = CompactionPolicy::new(200, 4);
        let out = compact_messages(&msgs, &policy).expect("should compact");
        // Anchor preserved, note injected, tail kept, overall smaller.
        assert!(out.len() < msgs.len());
        assert_eq!(
            out[0].content, msgs[0].content,
            "first (task) message preserved"
        );
        // The injected note mentions compaction.
        let note = match &out[1].content[0] {
            ContentBlock::Text { text } => text.clone(),
            _ => String::new(),
        };
        assert!(note.contains("compacted"), "note missing: {note}");
        // The last kept messages are the real recent tail.
        assert_eq!(out.last().unwrap().content, msgs.last().unwrap().content);
    }

    #[test]
    fn never_starts_the_tail_with_an_orphan_tool_result() {
        // Layout: task, then many (assistant tool_use, tool_result) pairs. A naive cut could land
        // right before a tool_result, orphaning it. Compaction must advance the cut.
        let mut msgs = vec![text_msg(Role::User, "TASK")];
        for i in 0..12 {
            msgs.push(assistant_tool_use(&format!("c{i}")));
            msgs.push(tool_result(&format!("c{i}")));
        }
        // keep_recent = 5 would provisionally cut mid-pair; force compaction with a tiny budget.
        let policy = CompactionPolicy::new(50, 5);
        let out = compact_messages(&msgs, &policy).expect("should compact");
        // The message right after the injected note (start of the kept tail) must NOT be a bare
        // tool_result.
        let tail_start = &out[2];
        assert!(
            !has_tool_result(tail_start),
            "tail starts with an orphan tool_result: {:?}",
            tail_start.content
        );
    }

    #[test]
    fn estimate_grows_with_content() {
        let small = vec![text_msg(Role::User, "hi")];
        let big = vec![text_msg(Role::User, &"word ".repeat(1000))];
        assert!(estimate_tokens(&big) > estimate_tokens(&small) * 10);
    }
}
