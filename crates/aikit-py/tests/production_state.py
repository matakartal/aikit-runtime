"""Runtime contract for binding-owned audit and persistent local state."""

import asyncio
import gc
import json
import os
import shutil
import tempfile
from pathlib import Path
from typing import Any, Dict, List

from aikit import Agent


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
    tmp = Path(tempfile.mkdtemp(prefix="aikit-production-state-"))
    try:
        metadata_path = tmp / "metadata.jsonl"
        full_path = tmp / "full.jsonl"
        memory_path = tmp / "memory.json"
        session_path = tmp / "sessions.json"

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

        created = await agent.run_subagent(child_spec("persist-session"), PROFILES)
        assert created["status"] == "succeeded", created

        reopened = Agent.from_env({})
        reopened.configure_jsonl_audit(str(metadata_path))
        reopened.use_session_file(str(session_path))
        resumed = await reopened.resume_subagent(
            "persist-session", child_spec("persist-session-resumed"), PROFILES
        )
        assert resumed["status"] == "succeeded", resumed

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
