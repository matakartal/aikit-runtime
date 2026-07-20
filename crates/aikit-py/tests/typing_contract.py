"""Static contract fixture for the public Python binding surface."""

from typing import TYPE_CHECKING, Literal, Optional
from typing_extensions import assert_type

from pydantic import BaseModel

from aikit import (
    AikitError,
    Agent,
    ApprovalRequest,
    ApprovalResponse,
    Client,
    ContainmentCapabilityReport,
    ContentPart,
    ErrorInfo,
    ErrorCode,
    EvalGate,
    EvalVerdict,
    FailureContext,
    GeneratedText,
    HookResponse,
    JsonValue,
    legacy,
    Message,
    McpConnection,
    McpToolFilter,
    ModelProfile,
    ModelRouteRequirements,
    ObjectStream,
    OutputPart,
    PromptInput,
    ProviderMetadata,
    QueryStream,
    ResumeCommand,
    RunOutcome,
    RunOptions,
    SemanticValidationDecision,
    StreamDelta,
    SubagentResult,
    SubagentSpec,
    Tool,
    connect_mcp_http,
    connect_mcp_stdio,
    evaluate_outcome,
    query,
    tool,
)

if TYPE_CHECKING:
    McpConnection()  # type: ignore[call-arg]  # factory-only native handle


class Invoice(BaseModel):
    currency: Literal["EUR"]
    status: Literal["ok"]


agent = Agent.from_env({})
messages: list[Message] = [
    {
        "role": "system",
        "content": [{"type": "text", "text": "Describe the supplied media."}],
    },
    {
        "role": "user",
        "content": [
            {"type": "text", "text": "What is visible?"},
            {
                "type": "media",
                "media_type": "image/png",
                "source": {"kind": "url", "url": "https://example.com/chart.png"},
            },
        ],
    },
]
prompt_input: PromptInput = messages
canonical_content: ContentPart = {"type": "text", "text": "canonical"}
materialized_output: OutputPart = {
    "type": "structured_data",
    "value": {"status": "ok"},
}
provider_metadata: ProviderMetadata = {"mock": [{"request_id": "fixture"}]}
assert_type(canonical_content, ContentPart)
assert_type(materialized_output, OutputPart)
assert_type(provider_metadata, ProviderMetadata)

eval_outcome: RunOutcome = {
    "messages": messages,
    "usage": {
        "input_tokens": 8,
        "output_tokens": 5,
        "cache_creation_input_tokens": 0,
        "cache_read_input_tokens": 0,
        "reasoning_tokens": 0,
    },
    "terminal_status": "completed",
    "stop_reason": "stop",
    "model_attempts": ["mock-1"],
    "invocation_start_message_index": 0,
}
eval_gates: list[EvalGate] = [
    {"type": "output_contains", "value": "chart"},
    {"type": "terminal_status", "status": "completed"},
    {"type": "tool_sequence", "names": ["search"], "exact": False},
    {"type": "no_tool_errors"},
    {"type": "max_total_tokens", "value": 32},
]
assert_type(evaluate_outcome(eval_outcome, eval_gates), EvalVerdict)


async def semantic_validator(_value: JsonValue) -> SemanticValidationDecision:
    return {"action": "retry", "reason": "business invariant not met"}


@tool(
    "typed_tool",
    "typed tool definition",
    {"type": "object", "properties": {"q": {"type": "string"}}},
)
async def typed_tool(tool_input: dict[str, JsonValue]) -> str:
    return str(tool_input.get("q", ""))


assert_type(typed_tool, Tool)
agent.add_tool_definition(typed_tool)
one_shot_stream: QueryStream = query(prompt_input, tools=[typed_tool])
agent.configure_jsonl_audit(
    "/tmp/aikit-audit.jsonl",
    payload_policy="metadata_only",
    failure_mode="fail_closed",
)
agent.use_memory_file("/tmp/aikit-memory.json", namespace="tenant-a")
agent.use_session_file("/tmp/aikit-sessions.json")
agent.use_sqlite_memory("/tmp/aikit-state.db", namespace="tenant-a")
agent.use_sqlite_sessions("/tmp/aikit-state.db")
assert_type(
    agent.recover_expired_session(
        "typed-session", side_effects_reconciled=True
    ),
    int,
)
agent.register_web_tools(["example.com"], "https://example.com/search?q={query}")
agent.register_browser_tools(
    "http://127.0.0.1:4444",
    "session",
    ["example.com"],
    external_egress_enforced=True,
)
stream = agent.stream_object(
    "stream an invoice",
    Invoice,
    provider_options={"openai": {"temperature": 0}},
    validator=semantic_validator,
)
assert_type(stream, ObjectStream[Invoice])


async def consume() -> None:
    async for event in stream:
        if event["type"] == "completed":
            assert_type(event["object"]["value"], Invoice)


async def consume_canonical_messages() -> None:
    generated = await agent.generate_text(messages)
    assert_type(generated, GeneratedText)

    text_stream = agent.stream_text(messages)
    async for delta in text_stream:
        assert_type(delta, StreamDelta)
        if delta["type"] == "error":
            assert_type(delta["info"], ErrorInfo)

    object_result = await agent.generate_object(
        messages, Invoice, validator=semantic_validator
    )
    assert_type(object_result["value"], Invoice)
    object_stream = agent.stream_object(messages, Invoice)
    assert_type(object_stream, ObjectStream[Invoice])

    try:
        await agent.generate_text(messages, model="unknown-model")
    except AikitError as error:
        assert_type(error.code, ErrorCode)
        assert_type(error.info, ErrorInfo)


async def approve(_request: ApprovalRequest) -> ApprovalResponse:
    return {
        "decision": "allow",
        "updated_permissions": ["allow_exact_input", "allow_tool"],
    }


agent.can_use_tool(approve)


async def post_tool_failure(_context: FailureContext) -> HookResponse:
    return None


agent.on_post_tool_failure(post_tool_failure, tool="search")
configuration_failure: FailureContext = {
    "run_id": "typed",
    "turn": 0,
    "stage": "configuration",
    "tool_use_id": None,
    "tool": None,
    "error": "typed",
}
validation_failure: FailureContext = {
    **configuration_failure,
    "stage": "tool_input_validation",
}
agent.register_builtin_tools(["/tmp/workspace", "/tmp/shared"])
agent.enable_bash_with_required_containment(
    {
        "image": f"example/aikit@sha256:{'a' * 64}",
        "pids_limit": 64,
        "memory_mib": 512,
        "cpus": 1,
        "tmpfs_mib": 64,
    }
)
agent.enable_capability_requests(["Bash"])
agent.enable_default_guardrails([r"ignore previous instructions"])

async def configure_mcp() -> None:
    tool_filter: McpToolFilter = {"allow": ["read_file", "search"], "deny": ["write_file"]}
    http_server: McpConnection = await connect_mcp_http(
        "https://mcp.example.com", "remote", tool_filter=tool_filter
    )
    stdio_server: McpConnection = await connect_mcp_stdio(
        "server", [], "local", env={}, inherit_env=False, tool_filter={"deny": ["Bash"]}
    )
    agent.register_mcp(http_server)
    await http_server.list_resources()
    await http_server.list_prompts()
    await http_server.read_resource("file:///guide")
    await http_server.get_prompt("review", {})
    void_server = stdio_server
    assert_type(void_server, McpConnection)
    assert_type(legacy.McpServer, type[McpConnection])

resume_without_approvals: ResumeCommand = {"command": "resume", "command_id": "resume-1"}
assert_type(resume_without_approvals, ResumeCommand)

run_options: RunOptions = {
    "model": "mock-1",
    "fallback_models": ["mock-2"],
    "max_tokens": 128,
    "max_turns": 4,
    "provider_options": {"openai": {"temperature": 0}},
    "budget": {"max_total_tokens": 1000},
    "retry": {"max_attempts_per_model": 2},
    "compaction": {"max_context_tokens": 4096, "keep_recent_messages": 8},
}
run_stream: QueryStream = agent.run(messages, run_options)
client = Client(agent)
client_stream: QueryStream = client.query(messages, run_options)


async def close_runs() -> None:
    outcome = run_stream.outcome()
    assert_type(outcome, RunOutcome)
    assert_type(outcome.get("final_text"), Optional[str])
    assert_type(outcome.get("provider_metadata"), Optional[dict[str, list[JsonValue]]])
    run_stream.cancel()
    assert_type(run_stream.is_cancelled(), bool)
    await run_stream.aclose()
    await client_stream.aclose()


async def inspect_containment() -> None:
    report = await agent.builtin_containment_capabilities()
    assert_type(report, ContainmentCapabilityReport)


profiles: list[ModelProfile] = [
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
routed_options: RunOptions = {
    "routing": {
        "profiles": profiles,
        "request": {
            "policy": {"kind": "explicit", "model": "mock-1"},
            "active_providers": [],
            "estimated_input_tokens": 8,
            "required_output_tokens": 64,
            "max_cost_usd": None,
            "required_skills": [],
            "required_capabilities": [],
        },
    }
}
routed_stream: QueryStream = agent.run(messages, routed_options)
route_requirements: ModelRouteRequirements = {
    "policy": {"kind": "explicit", "model": "mock-1"},
    "max_cost_usd": None,
    "required_skills": [],
    "required_capabilities": [],
}
subagent_spec: SubagentSpec = {
    "id": "typed-session",
    "prompt": "typed child",
    "system": None,
    "route": route_requirements,
    "allowed_tools": [],
    "max_turns": 2,
    "max_tokens": 64,
    "estimated_input_tokens": 8,
}
subtask_alias = agent.subtask(
    "typed-alias",
    "typed alias child",
    route_requirements,
    allowed_tools=["typed_tool"],
    max_turns=2,
    max_tokens=64,
    estimated_input_tokens=8,
)
assert_type(subtask_alias, SubagentSpec)


async def typed_subagent_resume() -> None:
    created = await agent.run_subagent(subagent_spec, profiles)
    assert_type(created, SubagentResult)
    assert_type(created.get("error_info"), Optional[ErrorInfo])
    resumed = await agent.resume_subagent("typed-session", subagent_spec, profiles)
    assert_type(resumed, SubagentResult)
    parallel = await agent.parallel([subtask_alias], profiles)
    assert_type(parallel, list[SubagentResult])
