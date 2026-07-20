"""Runtime contract for binding-owned audit and persistent local state."""

import asyncio
import gc
import json
import os
import shutil
import tempfile
from pathlib import Path
from typing import Any, Dict, List

from aikit import (
    AikitError,
    Agent,
    DurableRun,
    normalize_cedar_decision,
    normalize_opa_decision,
    model_capability_state,
    resolve_model_catalog,
    seal_governance_binding,
    seal_policy_snapshot,
    shipped_model_catalog,
    validate_media_artifact,
    validate_media_input,
    validate_model_profile,
)


PROFILES = [
    {
        "provider": "mock",
        "model": "mock-1",
        "context_window_tokens": 8192,
        "max_output_tokens": 1024,
        "pricing": None,
        "quality_score": 1,
        "skills": [],
        "capabilities": [],
    }
]

TOOL_SCHEMA = {
    "type": "object",
    "required": ["q"],
    "properties": {"q": {"type": "string"}},
    "additionalProperties": False,
}

OBJECT_SCHEMA = {
    "type": "object",
    "required": ["currency", "status"],
    "properties": {
        "currency": {"type": "string", "enum": ["EUR"]},
        "status": {"type": "string", "enum": ["ok"]},
    },
    "additionalProperties": False,
}


def sdk_contract_helpers() -> Dict[str, bool]:
    digest = "a" * 64
    media = {
        "media_type": "image/png",
        "source": {"kind": "artifact", "artifact_id": "artifact-image-1"},
        "sha256": digest,
        "size_bytes": 12,
    }
    assert validate_media_input(media) == media
    inline_media = {
        "media_type": "application/octet-stream",
        "source": {"kind": "bytes", "data": [97, 98, 99]},
        "sha256": "ba7816bf8f01cfea414140de5dae2223b00361a396177a9cb410ff61f20015ad",
        "size_bytes": 3,
    }
    assert validate_media_input(inline_media) == inline_media
    artifact = {
        "artifact_id": "artifact-image-1",
        "media_type": "image/png",
        "sha256": digest,
        "size_bytes": 12,
    }
    assert validate_media_artifact(artifact) == artifact

    def media_rejected(candidate: Dict[str, Any]) -> bool:
        try:
            validate_media_input(candidate)  # type: ignore[arg-type]
        except ValueError:
            return True
        return False

    abc_hash = "ba7816bf8f01cfea414140de5dae2223b00361a396177a9cb410ff61f20015ad"
    base64_media = {
        "media_type": "application/octet-stream",
        "source": {"kind": "base64", "data": "YWJj"},
        "sha256": abc_hash,
        "size_bytes": 3,
    }
    url_media = {
        "media_type": "image/png",
        "source": {"kind": "url", "url": "https://example.com/image.png"},
        "sha256": digest,
        "size_bytes": 1,
    }
    media_validation = {
        "artifact_empty_rejected": media_rejected(
            {**media, "source": {"kind": "artifact", "artifact_id": " "}}
        ),
        "base64_hash_rejected": media_rejected(
            {**base64_media, "sha256": "0" * 64}
        ),
        "base64_invalid_rejected": media_rejected(
            {**base64_media, "source": {"kind": "base64", "data": "%%%INVALID"}}
        ),
        "base64_size_rejected": media_rejected({**base64_media, "size_bytes": 2}),
        "base64_valid": validate_media_input(base64_media) == base64_media,
        "bytes_hash_rejected": media_rejected({**inline_media, "sha256": "0" * 64}),
        "bytes_size_rejected": media_rejected({**inline_media, "size_bytes": 2}),
        "bytes_valid": validate_media_input(inline_media) == inline_media,
        "credential_url_rejected": media_rejected(
            {
                **url_media,
                "source": {
                    "kind": "url",
                    "url": "https://user:secret@example.com/image.png",
                },
            }
        ),
        "mime_case_insensitive_valid": validate_media_input(
            {**base64_media, "media_type": "Image/PNG"}  # type: ignore[arg-type]
        )["media_type"]
        == "Image/PNG",
        "mime_extra_slash_rejected": media_rejected(
            {**url_media, "media_type": "image/png/extra"}
        ),
        "mime_parameter_rejected": media_rejected(
            {**url_media, "media_type": "image/png; charset=utf-8"}
        ),
        "mime_whitespace_rejected": media_rejected(
            {**url_media, "media_type": "image /png"}
        ),
        "relative_url_rejected": media_rejected(
            {**url_media, "source": {"kind": "url", "url": "/image.png"}}
        ),
        "unknown_field_rejected": media_rejected({**url_media, "unexpected": True}),
        "url_reference_valid": validate_media_input(url_media) == url_media,
        "url_scheme_rejected": media_rejected(
            {**url_media, "source": {"kind": "url", "url": "file:///tmp/image.png"}}
        ),
    }
    assert all(media_validation.values()), media_validation
    for invalid in [
        {**media, "sha256": digest.upper()},
        {**media, "size_bytes": 0},
        {**inline_media, "sha256": "0" * 64},
    ]:
        try:
            validate_media_input(invalid)
        except ValueError:
            pass
        else:
            raise AssertionError("invalid MediaInput must fail closed")
    try:
        validate_media_artifact({**artifact, "artifact_id": ""})
    except ValueError:
        pass
    else:
        raise AssertionError("blank artifact reference must fail closed")

    shipped = shipped_model_catalog()
    assert len(shipped["sources"]) == 8
    assert len(shipped["profiles"]) == 8
    assert validate_model_profile(shipped["profiles"][0]) == shipped["profiles"][0]
    assert model_capability_state(shipped["profiles"][0], "realtime_duplex") in {
        "supported",
        "unsupported",
        "unknown",
    }
    original_limit = shipped["profiles"][0]["max_output_tokens"]
    override = {**shipped["profiles"][0], "max_output_tokens": original_limit - 1}
    resolved = resolve_model_catalog([override])
    assert resolved["override_count"] == 1
    assert resolved["shipped_hash"] != resolved["overrides_hash"]
    assert shipped_model_catalog()["profiles"][0]["max_output_tokens"] == original_limit

    metadata = {
        "policy_rule_id": "package/aikit/allow",
        "input_summary": "tool=Read path=/workspace/a.txt",
        "risk_evidence": ["workspace_path"],
        "evaluator_revision": "rev-1",
    }
    opa = normalize_opa_decision(
        {"result": {"effect": "allow", "rule_id": "allow.read"}}, metadata
    )
    assert opa["engine"] == "opa" and opa["effect"] == "allow"
    try:
        normalize_opa_decision(
            {"result": {"effect": "allow", "partial": True}}, metadata
        )
    except ValueError:
        pass
    else:
        raise AssertionError("partial OPA decisions must fail closed")
    cedar = normalize_cedar_decision(
        {
            "decision": "Allow",
            "permit_policy_ids": ["permit.read"],
            "forbid_policy_ids": ["forbid.secret"],
        },
        metadata,
    )
    assert cedar["engine"] == "cedar" and cedar["effect"] == "deny"

    request_cases = [
        ("confirmation", lambda run: run.request_confirmation("confirm", "Proceed?")),
        (
            "missing_input",
            lambda run: run.request_input(
                "input", "Currency?", {"type": "string", "enum": ["EUR"]}
            ),
        ),
        (
            "output_review",
            lambda run: run.request_output_review(
                "review", "Review output", {"status": "draft"}
            ),
        ),
        (
            "edit_retry",
            lambda run: run.request_edit_retry(
                "retry", "Edit or retry", {"status": "invalid"}, "status mismatch"
            ),
        ),
    ]
    for index, (kind, request) in enumerate(request_cases):
        run = DurableRun("session-sdk", f"run-sdk-{index}")
        approval_id = request(run)
        approval = run.snapshot()["projection"]["approvals"][approval_id]
        assert approval["payload"]["kind"] == kind
        outcome = run.resolve_approval(
            f"resume-{index}", approval_id, True, {"accepted": True}
        )
        assert outcome["type"] == "resumed" and run.status == "running"

    policy_snapshot = seal_policy_snapshot(
        {
            "schema_version": 1,
            "default_effect": "deny",
            "rules": [
                {
                    "id": "allow.read",
                    "scope": {"scope": "tool", "tool": "Read"},
                    "effect": "allow",
                }
            ],
        }
    )
    governed = DurableRun.with_policy_snapshot(
        "session-governed", "run-governed", policy_snapshot
    )
    assert governed.policy_snapshot_hash == policy_snapshot["hash"]
    assert governed.snapshot()["events"][1]["kind"]["type"] == "governance_binding_pinned"
    assert governed.governance_binding == governed.snapshot()["events"][1]["kind"]["binding"]
    typed_id = governed.request_typed_approval(
        {
            "logical_key": "customer-id",
            "kind": "missing_input",
            "prompt": "Customer id?",
            "payload": {"field": "customer_id"},
            "policy_snapshot_hash": policy_snapshot["hash"],
            "requested_at_unix_ms": 100,
            "expires_at_unix_ms": 200,
        }
    )
    typed = governed.snapshot()["projection"]["approvals"][typed_id]
    assert typed["kind"] == "missing_input"
    assert typed["policy_snapshot_hash"] == policy_snapshot["hash"]
    assert typed["governance_binding"] == governed.governance_binding
    assert typed["requested_at_unix_ms"] == 100
    assert typed["expires_at_unix_ms"] == 200
    before_clock_rejection = governed.snapshot()
    try:
        governed.resolve_approval("resume-without-clock", typed_id, True, "cust-1")
    except RuntimeError:
        pass
    else:
        raise AssertionError("typed approval resolution must require an explicit clock")
    assert governed.snapshot() == before_clock_rejection
    restarted = DurableRun.from_state(governed.snapshot())
    typed_outcome = restarted.resolve_approval_at(
        "resume-with-clock", typed_id, True, 150, "cust-1"
    )
    assert typed_outcome["type"] == "resumed"
    resolved = restarted.snapshot()["projection"]["approvals"][typed_id]
    assert resolved["response"] == "cust-1"
    assert resolved["resolved_at_unix_ms"] == 150
    assert resolved["timed_out"] is False

    scoped_binding = seal_governance_binding(
        policy_snapshot,
        "run-scoped",
        tenant_id="tenant-a",
        agent_id="agent-a",
    )
    scoped = DurableRun.with_governance_binding(
        "session-scoped", "run-scoped", scoped_binding
    )
    assert scoped.governance_binding == scoped_binding
    assert scoped.policy_snapshot_hash == policy_snapshot["hash"]
    tampered_binding = {**scoped_binding, "tenant_id": "tenant-b"}
    try:
        DurableRun.with_governance_binding(
            "session-tampered", "run-scoped", tampered_binding
        )
    except ValueError:
        pass
    else:
        raise AssertionError("tampered governance binding was accepted")
    try:
        DurableRun.with_governance_binding(
            "session-mismatch", "different-run", scoped_binding
        )
    except RuntimeError:
        pass
    else:
        raise AssertionError("mismatched governance binding run_id was accepted")
    media_validation["governance_binding_valid"] = True

    timeout_run = DurableRun("session-timeout", "run-timeout")
    timeout_id = timeout_run.request_typed_approval(
        {
            "logical_key": "review",
            "kind": "output_review",
            "prompt": "Review output",
            "payload": {"status": "draft"},
            "requested_at_unix_ms": 100,
            "expires_at_unix_ms": 110,
        }
    )
    event_count = len(timeout_run.snapshot()["events"])
    assert timeout_run.expire_approvals("sweep-1", 110) == [timeout_id]
    assert len(timeout_run.snapshot()["events"]) == event_count + 1
    assert timeout_run.expire_approvals("sweep-1", 110) == []
    expired = timeout_run.snapshot()["projection"]["approvals"][timeout_id]
    assert expired["status"] == "rejected" and expired["timed_out"] is True
    assert expired["resolved_at_unix_ms"] == 110
    assert timeout_run.apply_command_at(
        {"command": "resume", "command_id": "resume-timeout"}, 110
    )["type"] == "resumed"
    return media_validation


def child_spec(identifier: str) -> Dict[str, Any]:
    return {
        "id": identifier,
        "prompt": f"run {identifier}",
        "system": None,
        "route": {
            "policy": {"kind": "explicit", "model": "mock-1"},
            "max_cost_usd": None,
            "required_skills": [],
            "required_capabilities": [],
        },
        "allowed_tools": [],
        "max_turns": 2,
        "max_tokens": 64,
        "estimated_input_tokens": 8,
    }


async def drain(stream: Any) -> Dict[str, Any]:
    async for _event in stream:
        pass
    return stream.outcome()


async def drain_object(stream: Any) -> None:
    async for _event in stream:
        pass


async def compatibility_contract() -> Dict[str, bool]:
    agent = Agent.from_env({})
    provider_options = {"mock": {"future_option": True}}

    strict_stream = agent.run(
        "strict-default", {"provider_options": provider_options}
    )
    strict_events = [event async for event in strict_stream]
    strict_outcome = strict_stream.outcome()
    assert strict_outcome["terminal_status"] == "failed"
    assert strict_events[-1]["type"] == "error"
    assert strict_events[-1]["info"]["code"] == "provider_invalid_request"

    warning_runs: Dict[str, bool] = {}
    for mode in ["warn", "best_effort"]:
        stream = agent.run(
            mode,
            {
                "provider_options": provider_options,
                "compatibility_mode": mode,
            },
        )
        events = [event async for event in stream]
        warning = next(event["warning"] for event in events if event["type"] == "warning")
        outcome = stream.outcome()
        assert warning["parameter"] == "future_option"
        assert outcome["warnings"][0]["parameter"] == "future_option"
        warning_runs[mode] = True

    try:
        agent.run("invalid-mode", {"compatibility_mode": "loose"})
    except ValueError:
        pass
    else:
        raise AssertionError("invalid compatibility mode was accepted")

    try:
        await agent.generate_object(
            "strict object", OBJECT_SCHEMA, provider_options=provider_options
        )
    except AikitError as error:
        assert error.code == "provider_invalid_request"
    else:
        raise AssertionError("strict structured provider option was accepted")
    warned_object = await agent.generate_object(
        "warn object",
        OBJECT_SCHEMA,
        provider_options=provider_options,
        compatibility_mode="warn",
    )
    assert warned_object["warnings"][0]["parameter"] == "future_option"

    object_stream = agent.stream_object(
        "best effort object",
        OBJECT_SCHEMA,
        provider_options=provider_options,
        compatibility_mode="best_effort",
    )
    object_events = [event async for event in object_stream]
    assert any(
        event["type"] == "delta"
        and event["delta"]["type"] == "warning"
        and event["delta"]["warning"]["parameter"] == "future_option"
        for event in object_events
    )
    completed = next(event for event in object_events if event["type"] == "completed")
    assert completed["object"]["warnings"][0]["parameter"] == "future_option"

    return {
        "compatibility_best_effort_warning": warning_runs["best_effort"],
        "compatibility_default_strict": True,
        "compatibility_invalid_mode_rejected": True,
        "compatibility_object_strict": True,
        "compatibility_object_warning": True,
        "compatibility_warn_warning": warning_runs["warn"],
    }


def read_jsonl(path: Path) -> List[Dict[str, Any]]:
    return [json.loads(line) for line in path.read_text().splitlines() if line]


def invalid_configuration_rejected(path: Path) -> bool:
    checks = []
    for payload_policy, failure_mode in [
        ("invalid", "fail_closed"),
        ("metadata_only", "invalid"),
    ]:
        try:
            Agent.from_env({}).configure_jsonl_audit(
                str(path),
                payload_policy=payload_policy,
                failure_mode=failure_mode,
            )
        except ValueError:
            checks.append(True)
        else:
            checks.append(False)
    return all(checks)


def symlink_guard(tmp: Path) -> str:
    if os.name == "nt":
        return "not_applicable"
    target = tmp / "audit-target.jsonl"
    link = tmp / "audit-link.jsonl"
    target.write_text("")
    link.symlink_to(target)
    try:
        Agent.from_env({}).configure_jsonl_audit(str(link))
    except RuntimeError:
        return "rejected"
    return "accepted"


async def main() -> None:
    media_validation = sdk_contract_helpers()
    media_validation.update(await compatibility_contract())
    tmp = Path(tempfile.mkdtemp(prefix="aikit-production-state-"))
    try:
        metadata_path = tmp / "metadata.jsonl"
        full_path = tmp / "full.jsonl"
        memory_path = tmp / "memory.json"
        session_path = tmp / "sessions.json"
        sqlite_path = tmp / "state.db"

        invalid_enums = invalid_configuration_rejected(tmp / "invalid.jsonl")
        symlink = symlink_guard(tmp)
        try:
            Agent.from_env({}).configure_jsonl_audit(
                str(tmp / "missing" / "audit.jsonl")
            )
        except RuntimeError:
            immediate_open_error = True
        else:
            immediate_open_error = False

        agent = Agent.from_env({})
        agent.configure_jsonl_audit(str(metadata_path))
        agent.use_session_file(str(session_path))

        tool_calls = 0

        async def metadata_tool(_tool_input: Dict[str, Any]) -> str:
            nonlocal tool_calls
            tool_calls += 1
            return "META_OUTPUT_SECRET"

        async def metadata_rewrite(_context: Dict[str, Any]) -> Dict[str, Any]:
            return {"action": "rewrite", "input": {"q": "META_INPUT_SECRET"}}

        agent.add_tool("search", "search", TOOL_SCHEMA, metadata_tool)
        agent.on_pre_tool_use(metadata_rewrite, tool="search")

        await agent.generate_text("generate")
        await drain(agent.stream_text("stream"))
        await drain(agent.run("run"))
        client = agent.client()
        await drain(client.query("client"))

        before_deny = tool_calls
        agent.set_permissions([{"effect": "deny", "tool": "search"}])
        await agent.generate_text("denied")
        assert tool_calls == before_deny, "denied tool reached the host callback"
        agent.set_permissions([], default_mode="allow")

        await agent.generate_object("object", OBJECT_SCHEMA)
        await drain_object(agent.stream_object("object stream", OBJECT_SCHEMA))

        memory_writer = Agent.from_env({})
        memory_writer.use_memory_file(str(memory_path), namespace="tenant-a")
        memory_writer.remember("customer_note", "Ada prefers EUR")
        persisted_memory = json.loads(memory_path.read_text())
        memory_file_persisted = any(
            entry["namespace"] == "tenant-a"
            and entry["key"] == "customer_note"
            and entry["value"] == "Ada prefers EUR"
            for entry in persisted_memory
        )
        # Drop the sole store owner so the core's Weak process registry cannot satisfy reopen.
        del memory_writer
        gc.collect()
        memory_reopened = Agent.from_env({})
        memory_reopened.use_memory_file(str(memory_path), namespace="tenant-a")
        recalled = memory_reopened.recall("EUR")
        memory_isolated = Agent.from_env({})
        memory_isolated.use_memory_file(str(memory_path), namespace="tenant-b")

        sqlite_writer = Agent.from_env({})
        sqlite_writer.use_sqlite_memory(str(sqlite_path), namespace="tenant-a")
        sqlite_writer.remember("sqlite_note", "durable SQLite")
        sqlite_reader = Agent.from_env({})
        sqlite_reader.use_sqlite_memory(str(sqlite_path), namespace="tenant-a")
        assert sqlite_reader.recall("SQLite")[0]["key"] == "sqlite_note"

        sqlite_sessions = Agent.from_env({})
        sqlite_sessions.use_sqlite_sessions(str(sqlite_path))
        sqlite_created = await sqlite_sessions.run_subagent(
            child_spec("sqlite-session"), PROFILES
        )
        assert sqlite_created["status"] == "succeeded", sqlite_created
        sqlite_reopened = Agent.from_env({})
        sqlite_reopened.use_sqlite_sessions(str(sqlite_path))
        sqlite_resumed = await sqlite_reopened.resume_subagent(
            "sqlite-session", child_spec("sqlite-session-resumed"), PROFILES
        )
        assert sqlite_resumed["status"] == "succeeded", sqlite_resumed

        network_tools = Agent.from_env({})
        network_tools.register_web_tools(
            ["example.com"], "https://example.com/search?q={query}"
        )
        browser_denied = Agent.from_env({})
        try:
            browser_denied.register_browser_tools(
                "http://127.0.0.1:4444",
                "session",
                ["example.com"],
                external_egress_enforced=False,
            )
        except ValueError as error:
            assert "BrowserEgressPolicy::ExternallyEnforced" in str(error)
        else:
            raise AssertionError("browser registration must fail without egress enforcement")
        assert "BrowserNavigate" not in browser_denied.capabilities()["tools"]
        network_tools.register_browser_tools(
            "http://127.0.0.1:4444",
            "session",
            ["example.com"],
            external_egress_enforced=True,
        )
        network_names = set(network_tools.capabilities()["tools"])
        assert {"WebFetch", "WebSearch", "BrowserNavigate", "BrowserSnapshot"} <= network_names

        created = await agent.run_subagent(child_spec("persist-session"), PROFILES)
        assert created["status"] == "succeeded", created

        crashed_database = json.loads(session_path.read_text())
        persisted_before_recovery = crashed_database["sessions"]["persist-session"]
        crashed_database.setdefault("execution_leases", {})["persist-session"] = {
            "owner": "crashed-worker",
            "token": "lease-" + "00" * 16,
            "expires_at_unix_ms": 0,
        }
        session_path.write_text(json.dumps(crashed_database))

        reopened = Agent.from_env({})
        reopened.configure_jsonl_audit(str(metadata_path))
        reopened.use_session_file(str(session_path))
        before_denied_recovery = session_path.read_text()
        try:
            reopened.recover_expired_session(
                "persist-session", side_effects_reconciled=False
            )
        except ValueError:
            pass
        else:
            raise AssertionError("recovery must require explicit side-effect reconciliation")
        assert session_path.read_text() == before_denied_recovery

        blocked_resume = await reopened.resume_subagent(
            "persist-session", child_spec("blocked-expired-resume"), PROFILES
        )
        assert blocked_resume["status"] == "session_conflict", blocked_resume
        still_blocked = json.loads(session_path.read_text())
        assert still_blocked["sessions"]["persist-session"] == persisted_before_recovery
        assert "persist-session" in still_blocked["execution_leases"]

        recovered_revision = reopened.recover_expired_session(
            "persist-session", side_effects_reconciled=True
        )
        assert recovered_revision == 1
        recovered_database = json.loads(session_path.read_text())
        assert recovered_database["sessions"]["persist-session"] == persisted_before_recovery
        assert "persist-session" not in recovered_database.get("execution_leases", {})

        resumed = await reopened.resume_subagent(
            "persist-session", child_spec("persist-session-resumed"), PROFILES
        )
        assert resumed["status"] == "succeeded", resumed

        fresh_database = json.loads(session_path.read_text())
        fresh_database.setdefault("execution_leases", {})["fresh-crash"] = {
            "owner": "crashed-worker",
            "token": "lease-" + "11" * 16,
            "expires_at_unix_ms": 0,
        }
        session_path.write_text(json.dumps(fresh_database))
        assert (
            reopened.recover_expired_session(
                "fresh-crash", side_effects_reconciled=True
            )
            == 0
        )
        cleared_fresh = json.loads(session_path.read_text())
        assert "fresh-crash" not in cleared_fresh.get("execution_leases", {})
        assert "fresh-crash" not in cleared_fresh["sessions"]
        fresh_after_recovery = await reopened.run_subagent(
            child_spec("fresh-crash"), PROFILES
        )
        assert fresh_after_recovery["status"] == "succeeded", fresh_after_recovery
        assert fresh_after_recovery["session_revision"] == 1

        fan = await agent.fan_out(
            [child_spec("fan-a"), child_spec("fan-b")],
            PROFILES,
            max_parallelism=2,
        )
        assert all(result["status"] == "succeeded" for result in fan), fan
        council = await agent.council(
            [child_spec("council-a"), child_spec("council-b")],
            child_spec("council-synthesis"),
            PROFILES,
            min_successes=2,
            max_parallelism=2,
        )
        assert council["status"] == {"kind": "succeeded"}, council

        full = Agent.from_env({})
        full.configure_jsonl_audit(
            str(full_path), payload_policy="full", failure_mode="best_effort"
        )

        async def full_tool(_tool_input: Dict[str, Any]) -> str:
            return "FULL_OUTPUT_SECRET"

        async def full_rewrite(_context: Dict[str, Any]) -> Dict[str, Any]:
            return {"action": "rewrite", "input": {"q": "FULL_INPUT_SECRET"}}

        full.add_tool("search", "search", TOOL_SCHEMA, full_tool)
        full.on_pre_tool_use(full_rewrite, tool="search")
        await full.generate_text("full")

        metadata_records = read_jsonl(metadata_path)
        metadata_text = metadata_path.read_text()
        full_text = full_path.read_text()
        event_types = {record["type"] for record in metadata_records}
        required_events = {
            "permission_decision",
            "run_started",
            "run_stopped",
            "structured_output_attempt",
            "structured_output_completed",
            "subagent_started",
            "subagent_completed",
            "tool_started",
            "tool_completed",
        }
        assert required_events <= event_types, sorted(required_events - event_types)

        run_ids = [
            record["run_id"]
            for record in metadata_records
            if record["type"] == "run_started"
        ]
        top_level_run_ids = [
            record["run_id"]
            for record in metadata_records
            if record["type"] == "run_started"
            and record.get("parent_run_id") is None
        ]
        structured_ids = {
            record["run_id"]
            for record in metadata_records
            if record["type"] == "structured_output_attempt"
        }
        subagent_starts = [
            record
            for record in metadata_records
            if record["type"] == "subagent_started"
        ]
        expected_children = {
            "persist-session",
            "persist-session-resumed",
            "fan-a",
            "fan-b",
            "council-a",
            "council-b",
            "council-synthesis",
        }
        observed_children = {record["subagent_id"] for record in subagent_starts}
        parent_ids = {record.get("parent_run_id") for record in subagent_starts}

        metadata_redacted = (
            "META_INPUT_SECRET" not in metadata_text
            and "META_OUTPUT_SECRET" not in metadata_text
            and '"input"' not in metadata_text
            and '"output_preview"' not in metadata_text
        )
        full_captured = (
            "FULL_INPUT_SECRET" in full_text
            and "FULL_OUTPUT_SECRET" in full_text
            and '"input"' in full_text
            and '"output_preview"' in full_text
        )

        summary = {
            "audit": {
                "deny_recorded": any(
                    record["type"] == "permission_decision"
                    and record["decision"] == "deny"
                    for record in metadata_records
                ),
                "events_present": sorted(required_events),
                "full_captured": full_captured,
                "immediate_open_error": immediate_open_error,
                "invalid_enums_rejected": invalid_enums,
                "metadata_redacted": metadata_redacted,
                "orchestration_paths": expected_children <= observed_children,
                "parent_correlated": all(
                    record.get("parent_run_id") for record in subagent_starts
                )
                and len(parent_ids) >= 4,
                "provider_metadata_omitted": "provider_metadata" not in metadata_text,
                "run_ids_unique": len(run_ids) == len(set(run_ids)),
                "structured_run_ids_unique": len(structured_ids) == 2,
                "symlink_guard": symlink,
                "text_paths": len(top_level_run_ids) == 5
                and len(set(top_level_run_ids)) == 5,
            },
            "memory": {
                "file_persisted": memory_file_persisted,
                "namespace_isolated": memory_isolated.recall("EUR") == [],
                "reopened": bool(recalled)
                and recalled[0]["key"] == "customer_note"
                and recalled[0]["value"] == "Ada prefers EUR",
            },
            "session": {
                "file_persisted": json.loads(session_path.read_text())["sessions"][
                    "persist-session"
                ]["revision"]
                == 2,
                "reopened": resumed["session_revision"] == 2,
                "revisions": [
                    created["session_revision"],
                    resumed["session_revision"],
                ],
            },
            "sdk": media_validation,
        }
        assert all(
            value is True
            for section in summary.values()
            for key, value in section.items()
            if key not in {"events_present", "revisions", "symlink_guard"}
        ), summary
        assert symlink in {"rejected", "not_applicable"}, summary
        assert summary["session"]["revisions"] == [1, 2], summary
        print(
            "PRODUCTION_STATE_JSON="
            + json.dumps(summary, sort_keys=True, separators=(",", ":"))
        )
    finally:
        shutil.rmtree(tmp, ignore_errors=True)


if __name__ == "__main__":
    asyncio.run(main())
