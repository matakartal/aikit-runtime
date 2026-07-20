from typing import Any, AsyncIterator, Awaitable, Callable, Dict, Generic, List, Literal, Mapping, NoReturn, Optional, Protocol, Sequence, TypedDict, TypeVar, Union, overload

JsonPrimitive = Union[str, int, float, bool, None]
JsonValue = Union[JsonPrimitive, List["JsonValue"], Dict[str, "JsonValue"]]
ProviderOptions = Mapping[str, Mapping[str, JsonValue]]
ProviderMetadata = Dict[str, List[JsonValue]]
T = TypeVar("T")
T_co = TypeVar("T_co", covariant=True)


class Usage(TypedDict):
    input_tokens: int
    output_tokens: int
    cache_creation_input_tokens: int
    cache_read_input_tokens: int
    reasoning_tokens: int


ErrorCode = Literal[
    "provider_auth",
    "provider_rate_limit",
    "provider_timeout",
    "provider_transport",
    "provider_server",
    "provider_invalid_request",
    "provider_protocol",
    "provider_safety",
    "configuration",
    "permission_denied",
    "sandbox",
    "budget_exceeded",
    "max_turns",
    "tool_execution",
    "structured_output",
    "session",
    "conflict",
    "cancelled",
    "audit",
    "hook",
    "unknown",
]


class ErrorInfo(TypedDict):
    code: ErrorCode
    message: str
    provider: Optional[str]
    model: Optional[str]
    status: Optional[int]
    retry_after_ms: Optional[int]
    retryable: bool


class AikitError(RuntimeError):
    """Typed terminal runtime failure; branch on ``code`` instead of parsing text."""

    code: ErrorCode
    info: ErrorInfo


class TextBlock(TypedDict):
    type: Literal["text"]
    text: str


class _ReasoningBlockRequired(TypedDict):
    type: Literal["reasoning"]
    text: str


class ReasoningBlock(_ReasoningBlockRequired, total=False):
    signature: str
    provider: str
    opaque: JsonValue


class ToolUseBlock(TypedDict):
    type: Literal["tool_use"]
    id: str
    name: str
    input: JsonValue


class _ToolResultBlockRequired(TypedDict):
    type: Literal["tool_result"]
    tool_use_id: str
    content: str


class ToolResultBlock(_ToolResultBlockRequired, total=False):
    is_error: bool


class UrlMediaSource(TypedDict):
    kind: Literal["url"]
    url: str


class Base64MediaSource(TypedDict):
    kind: Literal["base64"]
    data: str


MediaSource = Union[UrlMediaSource, Base64MediaSource]


class UrlMediaInputSource(TypedDict):
    kind: Literal["url"]
    url: str


class Base64MediaInputSource(TypedDict):
    kind: Literal["base64"]
    data: str


class BytesMediaInputSource(TypedDict):
    kind: Literal["bytes"]
    data: List[int]


class ArtifactMediaInputSource(TypedDict):
    kind: Literal["artifact"]
    artifact_id: str


MediaInputSource = Union[
    UrlMediaInputSource,
    Base64MediaInputSource,
    BytesMediaInputSource,
    ArtifactMediaInputSource,
]


class MediaInput(TypedDict):
    media_type: str
    source: MediaInputSource
    sha256: str
    size_bytes: int


class OutputTextPart(TypedDict):
    type: Literal["text"]
    text: str


class _OutputReasoningPartRequired(TypedDict):
    type: Literal["reasoning"]
    text: str


class OutputReasoningPart(_OutputReasoningPartRequired, total=False):
    signature: str
    provider: str
    opaque: JsonValue


class OutputImagePart(TypedDict):
    type: Literal["image"]
    media: MediaInput


class OutputAudioPart(TypedDict):
    type: Literal["audio"]
    media: MediaInput


class _OutputFilePartRequired(TypedDict):
    type: Literal["file"]
    media: MediaInput


class OutputFilePart(_OutputFilePartRequired, total=False):
    filename: str


class _OutputTranscriptPartRequired(TypedDict):
    type: Literal["transcript"]
    text: str


class OutputTranscriptPart(_OutputTranscriptPartRequired, total=False):
    language: str


class OutputToolCallPart(TypedDict):
    type: Literal["tool_call"]
    id: str
    name: str
    input: JsonValue


class OutputToolResultPart(TypedDict):
    type: Literal["tool_result"]
    tool_use_id: str
    content: str
    is_error: bool


class OutputStructuredDataPart(TypedDict):
    type: Literal["structured_data"]
    value: JsonValue


class _OutputCitationPartRequired(TypedDict):
    type: Literal["citation"]
    text: str


class OutputCitationPart(_OutputCitationPartRequired, total=False):
    source: str
    metadata: JsonValue


OutputPart = Union[
    OutputTextPart,
    OutputReasoningPart,
    OutputImagePart,
    OutputAudioPart,
    OutputFilePart,
    OutputTranscriptPart,
    OutputToolCallPart,
    OutputToolResultPart,
    OutputStructuredDataPart,
    OutputCitationPart,
]


class MediaBlock(TypedDict):
    type: Literal["media"]
    media_type: str
    source: MediaSource


class _CitationBlockRequired(TypedDict):
    type: Literal["citation"]
    text: str


class CitationBlock(_CitationBlockRequired, total=False):
    source: str
    metadata: JsonValue


ContentBlock = Union[
    TextBlock,
    ReasoningBlock,
    ToolUseBlock,
    ToolResultBlock,
    MediaBlock,
    CitationBlock,
]

# Canonical v0.3 name; ContentBlock remains the v0.x compatibility spelling.
ContentPart = ContentBlock


class Message(TypedDict):
    role: Literal["system", "user", "assistant", "tool"]
    content: List[ContentBlock]


PromptInput = Union[str, Sequence[Message]]


class GeneratedText(TypedDict):
    text: str
    usage: Usage
    stop_reason: Optional[str]
    messages: List[Message]
    provider_metadata: Dict[str, List[JsonValue]]


class GeneratedObject(TypedDict, Generic[T]):
    value: T
    fidelity: Literal["native_constrained", "forced_tool_call", "prompted_and_parsed"]
    attempts: int
    provider_metadata: Dict[str, List[JsonValue]]


class SemanticAccept(TypedDict):
    action: Literal["accept"]


class SemanticRetry(TypedDict):
    action: Literal["retry"]
    reason: str


class SemanticReject(TypedDict):
    action: Literal["reject"]
    reason: str


SemanticValidationDecision = Union[
    Literal["accept"], SemanticAccept, SemanticRetry, SemanticReject
]
SemanticValidator = Callable[[JsonValue], Awaitable[SemanticValidationDecision]]


class ObjectAttemptStarted(TypedDict):
    type: Literal["attempt_started"]
    attempt: int
    total_attempts: int
    fidelity: Literal["native_constrained", "forced_tool_call", "prompted_and_parsed"]
    repair: bool


class ObjectDelta(TypedDict):
    type: Literal["delta"]
    attempt: int
    delta: "StreamDelta"


class ObjectValidationFailed(TypedDict):
    type: Literal["validation_failed"]
    attempt: int
    error: str
    will_retry: bool


class ObjectCompleted(TypedDict, Generic[T]):
    type: Literal["completed"]
    object: GeneratedObject[T]


ObjectStreamEvent = Union[
    ObjectAttemptStarted,
    ObjectDelta,
    ObjectValidationFailed,
    ObjectCompleted[T],
]


class StructuredSchema(Protocol[T_co]):
    def model_json_schema(self) -> Mapping[str, JsonValue]: ...
    def model_validate(self, value: Any) -> T_co: ...


class ProviderCapabilityView(TypedDict):
    provider: str
    supports_reasoning: bool
    supports_prompt_cache: bool
    supports_vision: bool
    supports_citations: bool
    structured_output: Literal[
        "native_constrained", "forced_tool_call", "prompted_and_parsed"
    ]
    structured_output_features: "StructuredOutputCapabilities"


class StructuredOutputCapabilities(TypedDict):
    native_schema: CapabilityState
    forced_tool: CapabilityState
    prompted_parse: CapabilityState
    schema_with_tools: CapabilityState
    streaming_schema: CapabilityState
    parallel_tools: CapabilityState


class AgentCapabilities(TypedDict):
    providers: List[ProviderCapabilityView]
    tools: List[str]
    runtime_features: List[str]


class _PermissionRuleRequired(TypedDict):
    effect: Literal["allow", "deny", "ask"]
    tool: str


class PermissionRule(_PermissionRuleRequired, total=False):
    id: str
    pattern: str
    field: str


class ApprovalRequest(TypedDict):
    run_id: str
    turn: int
    tool_use_id: str
    tool: str
    input: JsonValue


PermissionUpdate = Literal["allow_exact_input", "allow_tool"]


class ApprovalAllowResponse(TypedDict, total=False):
    action: Literal["allow"]
    decision: Literal["allow"]
    updated_input: JsonValue
    updated_permissions: List[PermissionUpdate]


class ApprovalDenyResponse(TypedDict, total=False):
    action: Literal["deny"]
    decision: Literal["deny"]
    message: str
    interrupt: bool


ApprovalResponse = Union[
    bool,
    Literal["allow", "deny"],
    ApprovalAllowResponse,
    ApprovalDenyResponse,
]


class PromptContext(TypedDict):
    run_id: str
    prompt: str


class PreToolUseContext(TypedDict):
    run_id: str
    turn: int
    tool_use_id: str
    tool: str
    input: JsonValue


class PostToolUseContext(PreToolUseContext):
    output: str
    duration_ms: int


class FailureContext(TypedDict):
    run_id: str
    turn: int
    stage: Literal[
        "configuration",
        "provider_start",
        "provider_stream",
        "tool_not_advertised",
        "pre_tool_use",
        "permission",
        "tool_execution",
        "tool_input_validation",
        "post_tool_use",
        "max_turns",
        "malformed_tool_call",
        "budget",
        "audit",
    ]
    tool_use_id: Optional[str]
    tool: Optional[str]
    error: str


class StopContext(TypedDict):
    run_id: str
    turns: int
    reason: str
    usage: Usage


HookResponse = Optional[Union[Literal["continue"], Mapping[str, JsonValue]]]


class ModelPricing(TypedDict):
    input_per_million_usd: float
    output_per_million_usd: float
    cache_read_per_million_usd: Optional[float]
    cache_write_per_million_usd: Optional[float]


class _RunPricingRequired(TypedDict):
    input_per_million_usd: float
    output_per_million_usd: float


class RunPricing(_RunPricingRequired, total=False):
    cache_read_per_million_usd: float
    cache_write_per_million_usd: float


class BudgetPolicy(TypedDict, total=False):
    max_total_tokens: int
    max_cost_usd: float
    pricing: RunPricing


class RetryPolicy(TypedDict, total=False):
    max_attempts_per_model: int
    base_delay_ms: int
    max_delay_ms: int
    per_attempt_timeout_ms: int


CapabilityState = Literal["supported", "unsupported", "unknown"]
CompatibilityMode = Literal["strict", "warn", "best_effort"]
ModelCapability = Union[
    Literal[
        "reasoning",
        "prompt_cache",
        "citations",
        "vision",
        "native_structured_output",
        "tool_use",
        "image_generation",
        "document_input",
        "audio_input",
        "transcription",
        "speech_generation",
        "realtime_duplex",
    ],
    Dict[str, str],
]


class _ModelProfileRequired(TypedDict):
    provider: str
    model: str
    context_window_tokens: int
    max_output_tokens: int
    pricing: Optional[ModelPricing]
    quality_score: int
    skills: List[str]
    capabilities: List[ModelCapability]


class ModelProfile(_ModelProfileRequired, total=False):
    capability_states: Dict[str, CapabilityState]


ModalityRequirement = Literal[
    "text",
    "reasoning",
    "image_input",
    "image_generation",
    "document_input",
    "audio_input",
    "transcription",
    "speech_generation",
    "realtime_duplex",
    "tool_use",
    "structured_output",
]


class _MediaArtifactRequired(TypedDict):
    artifact_id: str
    media_type: str
    sha256: str
    size_bytes: int


class MediaArtifact(_MediaArtifactRequired, total=False):
    provider: str
    model: str


class _GeneratedImageRequired(TypedDict):
    artifact: MediaArtifact


class GeneratedImage(_GeneratedImageRequired, total=False):
    revised_prompt: str
    provider_metadata: JsonValue


class _GeneratedAudioRequired(TypedDict):
    artifact: MediaArtifact


class GeneratedAudio(_GeneratedAudioRequired, total=False):
    duration_ms: int
    voice: str
    provider_metadata: JsonValue


class _TranscriptSegmentRequired(TypedDict):
    start_ms: int
    end_ms: int
    text: str


class TranscriptSegment(_TranscriptSegmentRequired, total=False):
    speaker: str
    confidence: float


class _TranscriptRequired(TypedDict):
    text: str
    segments: List[TranscriptSegment]


class Transcript(_TranscriptRequired, total=False):
    language: str
    provider_metadata: JsonValue


class VoiceActivityPolicy(TypedDict):
    enabled: bool
    threshold: float
    prefix_padding_ms: int
    silence_duration_ms: int
    interrupt_response: bool


class RouteRequest(TypedDict):
    policy: Dict[str, JsonValue]
    active_providers: List[str]
    estimated_input_tokens: int
    required_output_tokens: int
    max_cost_usd: Optional[float]
    required_skills: List[str]
    required_capabilities: List[ModelCapability]


class RoutingOptions(TypedDict):
    profiles: Sequence[ModelProfile]
    request: RouteRequest


class CompactionOptions(TypedDict):
    max_context_tokens: int
    keep_recent_messages: int


class RunOptions(TypedDict, total=False):
    model: str
    fallback_models: List[str]
    max_tokens: int
    max_turns: int
    provider_options: ProviderOptions
    budget: BudgetPolicy
    retry: RetryPolicy
    routing: RoutingOptions
    compaction: CompactionOptions


class RouteDecision(TypedDict):
    profile: ModelProfile
    estimated_cost_usd: Optional[float]
    policy: Dict[str, JsonValue]
    eligible_models: int


class MemoryEntry(TypedDict):
    namespace: str
    key: str
    value: JsonValue
    plane: Literal["working", "episodic", "semantic"]
    revision: int
    provenance: "MemoryProvenance"
    tags: List[str]
    importance: int
    created_unix_ms: int
    updated_unix_ms: int


class MemoryProvenance(TypedDict, total=False):
    source_run_id: str
    source_event_sequence: int
    model_generated: bool


class BudgetLimits(TypedDict, total=False):
    max_model_calls: Optional[int]
    max_input_tokens: Optional[int]
    max_output_tokens: Optional[int]
    max_cost_micro_usd: Optional[int]
    wall_time_ms: Optional[int]


class RequiredContainment(TypedDict):
    mode: Literal["required"]
    backend: Literal["auto", "seatbelt", "docker"]


class UncontainedContainment(TypedDict):
    mode: Literal["uncontained"]


ContainmentRequirement = Union[RequiredContainment, UncontainedContainment]


class ContainmentGuarantees(TypedDict):
    filesystem_write_boundary: bool
    sensitive_home_read_boundary: bool
    network_boundary: bool
    descendant_inheritance: bool
    syscall_filter: bool
    resource_limits: bool


class BackendCapability(TypedDict):
    backend: Literal["seatbelt", "docker", "uncontained"]
    available: bool
    guarantees: ContainmentGuarantees
    detail: str


class ContainmentCapabilityReport(TypedDict):
    requirement: ContainmentRequirement
    selected_backend: Optional[Literal["seatbelt", "docker", "uncontained"]]
    fail_closed: bool
    backends: List[BackendCapability]


class _DockerContainmentOptionsRequired(TypedDict):
    image: str


class DockerContainmentOptions(_DockerContainmentOptionsRequired, total=False):
    executable: str
    pids_limit: int
    memory_mib: int
    cpus: int
    tmpfs_mib: int


class ModelRouteRequirements(TypedDict):
    policy: Dict[str, JsonValue]
    max_cost_usd: Optional[float]
    required_skills: List[str]
    required_capabilities: List[Union[str, Dict[str, str]]]


class SubagentSpec(TypedDict):
    id: str
    prompt: str
    system: Optional[str]
    route: ModelRouteRequirements
    allowed_tools: List[str]
    max_turns: int
    max_tokens: int
    estimated_input_tokens: int


class _RunOutcomeRequired(TypedDict):
    messages: List[Message]
    usage: Usage
    terminal_status: Literal[
        "running", "completed", "failed", "budget_exceeded", "max_turns", "cancelled"
    ]
    stop_reason: Optional[str]
    model_attempts: List[str]


class RunOutcome(_RunOutcomeRequired, total=False):
    final_text: str
    provider_metadata: Dict[str, List[JsonValue]]
    invocation_start_message_index: int


EvalTerminalStatus = Literal[
    "completed", "failed", "budget_exceeded", "max_turns", "cancelled"
]


class EvalOutputExactGate(TypedDict):
    type: Literal["output_exact"]
    value: str


class EvalOutputContainsGate(TypedDict):
    type: Literal["output_contains"]
    value: str


class EvalOutputNotContainsGate(TypedDict):
    type: Literal["output_not_contains"]
    value: str


class EvalTerminalStatusGate(TypedDict):
    type: Literal["terminal_status"]
    status: EvalTerminalStatus


class EvalCalledToolGate(TypedDict):
    type: Literal["called_tool"]
    name: str


class EvalDidNotCallToolGate(TypedDict):
    type: Literal["did_not_call_tool"]
    name: str


class _EvalToolSequenceGateRequired(TypedDict):
    type: Literal["tool_sequence"]
    names: List[str]


class EvalToolSequenceGate(_EvalToolSequenceGateRequired, total=False):
    exact: bool


class EvalNoToolErrorsGate(TypedDict):
    type: Literal["no_tool_errors"]


class EvalMaxTurnsGate(TypedDict):
    type: Literal["max_turns"]
    value: int


class EvalMaxInputTokensGate(TypedDict):
    type: Literal["max_input_tokens"]
    value: int


class EvalMaxOutputTokensGate(TypedDict):
    type: Literal["max_output_tokens"]
    value: int


class EvalMaxTotalTokensGate(TypedDict):
    type: Literal["max_total_tokens"]
    value: int


class EvalMaxModelAttemptsGate(TypedDict):
    type: Literal["max_model_attempts"]
    value: int


EvalGate = Union[
    EvalOutputExactGate,
    EvalOutputContainsGate,
    EvalOutputNotContainsGate,
    EvalTerminalStatusGate,
    EvalCalledToolGate,
    EvalDidNotCallToolGate,
    EvalToolSequenceGate,
    EvalNoToolErrorsGate,
    EvalMaxTurnsGate,
    EvalMaxInputTokensGate,
    EvalMaxOutputTokensGate,
    EvalMaxTotalTokensGate,
    EvalMaxModelAttemptsGate,
]


class EvalCheck(TypedDict):
    gate: str
    passed: bool
    message: str


class EvalVerdict(TypedDict):
    passed: bool
    passed_checks: int
    total_checks: int
    score: float
    checks: List[EvalCheck]


class _SubagentResultRequired(TypedDict):
    id: str
    status: Literal[
        "succeeded",
        "invalid_spec",
        "route_rejected",
        "budget_rejected",
        "max_turns",
        "failed",
        "session_rejected",
        "session_conflict",
        "audit_rejected",
    ]
    model: Optional[str]
    final_text: Optional[str]
    outcome: RunOutcome
    error: Optional[Dict[str, JsonValue]]
    session_revision: Optional[int]


class SubagentResult(_SubagentResultRequired, total=False):
    error_info: ErrorInfo


class CouncilResult(TypedDict):
    status: Dict[str, JsonValue]
    members: List[SubagentResult]
    synthesis: Optional[SubagentResult]


class MessageStartDelta(TypedDict):
    type: Literal["message_start"]
    model: str


class TextDelta(TypedDict):
    type: Literal["text_delta"]
    text: str


class ReasoningDelta(TypedDict):
    type: Literal["reasoning_delta"]
    text: str


class _ReasoningCompleteDeltaRequired(TypedDict):
    type: Literal["reasoning_complete"]
    text: str


class ReasoningCompleteDelta(_ReasoningCompleteDeltaRequired, total=False):
    signature: str
    opaque: JsonValue


class ToolCallStartDelta(TypedDict):
    type: Literal["tool_call_start"]
    id: str
    name: str


class ToolCallInputDelta(TypedDict):
    type: Literal["tool_call_input"]
    id: str
    input: JsonValue


class ToolResultDelta(TypedDict):
    type: Literal["tool_result"]
    tool_use_id: str
    content: str
    is_error: bool


class _CitationDeltaRequired(TypedDict):
    type: Literal["citation"]
    text: str
    source: Optional[str]


class CitationDelta(_CitationDeltaRequired, total=False):
    metadata: JsonValue


class ProviderMetadataDelta(TypedDict):
    type: Literal["provider_metadata"]
    provider: str
    metadata: JsonValue


class UsageDelta(Usage):
    type: Literal["usage"]


class MessageStopDelta(TypedDict):
    type: Literal["message_stop"]
    stop_reason: str


class ErrorDelta(TypedDict):
    type: Literal["error"]
    message: str
    info: ErrorInfo


StreamDelta = Union[
    MessageStartDelta,
    TextDelta,
    ReasoningDelta,
    ReasoningCompleteDelta,
    ToolCallStartDelta,
    ToolCallInputDelta,
    ToolResultDelta,
    CitationDelta,
    ProviderMetadataDelta,
    UsageDelta,
    MessageStopDelta,
    ErrorDelta,
]


class _ProviderWarningRequired(TypedDict):
    code: str
    message: str


class ProviderWarning(_ProviderWarningRequired, total=False):
    parameter: str
    provider: str
    model: str


StreamBlockKind = Literal[
    "text",
    "reasoning",
    "tool_call",
    "tool_result",
    "citation",
    "image",
    "audio",
    "transcript",
    "structured_data",
]


class _StreamEventEnvelope(TypedDict):
    event_id: str
    sequence: int


class ResponseStartEvent(_StreamEventEnvelope):
    type: Literal["response_start"]
    response_id: str
    model: str


class _BlockStartEventRequired(_StreamEventEnvelope):
    type: Literal["block_start"]
    block_id: str
    block_kind: StreamBlockKind


class BlockStartEvent(_BlockStartEventRequired, total=False):
    name: str


class BlockDeltaEvent(_StreamEventEnvelope):
    type: Literal["block_delta"]
    block_id: str
    delta: JsonValue


class _BlockEndEventRequired(_StreamEventEnvelope):
    type: Literal["block_end"]
    block_id: str


class BlockEndEvent(_BlockEndEventRequired, total=False):
    value: JsonValue


class ProviderMetadataEvent(_StreamEventEnvelope):
    type: Literal["provider_metadata"]
    provider: str
    metadata: JsonValue


class StreamUsageEvent(_StreamEventEnvelope):
    type: Literal["usage"]
    usage: Usage


class StreamWarningEvent(_StreamEventEnvelope):
    type: Literal["warning"]
    warning: ProviderWarning


class ResponseEndEvent(_StreamEventEnvelope):
    type: Literal["response_end"]
    response_id: str
    stop_reason: str


class StreamErrorEvent(_StreamEventEnvelope):
    type: Literal["error"]
    message: str
    info: ErrorInfo


class RawProviderEvent(_StreamEventEnvelope):
    type: Literal["raw_provider_event"]
    provider: str
    event: JsonValue


StreamEvent = Union[
    ResponseStartEvent,
    BlockStartEvent,
    BlockDeltaEvent,
    BlockEndEvent,
    ProviderMetadataEvent,
    StreamUsageEvent,
    StreamWarningEvent,
    ResponseEndEvent,
    StreamErrorEvent,
    RawProviderEvent,
]


class QueryEventStream(AsyncIterator[StreamEvent]):
    def __aiter__(self) -> "QueryEventStream": ...
    async def __anext__(self) -> StreamEvent: ...
    def cancel(self) -> None: ...
    def is_cancelled(self) -> bool: ...
    async def aclose(self) -> RunOutcome: ...
    def outcome(self) -> RunOutcome: ...


class QueryStream(AsyncIterator[StreamDelta]):
    def __aiter__(self) -> "QueryStream": ...
    async def __anext__(self) -> StreamDelta: ...
    def events(self, response_id: str) -> QueryEventStream: ...
    async def __aenter__(self) -> "QueryStream": ...
    async def __aexit__(self, exc_type: Any, exc_value: Any, traceback: Any) -> bool: ...
    def cancel(self) -> None: ...
    def is_cancelled(self) -> bool: ...
    async def aclose(self) -> RunOutcome: ...
    def outcome(self) -> RunOutcome: ...


class ObjectStream(AsyncIterator[ObjectStreamEvent[T]], Generic[T]):
    def __aiter__(self) -> "ObjectStream[T]": ...
    async def __anext__(self) -> ObjectStreamEvent[T]: ...


class Tool(Protocol):
    name: str
    description: str
    input_schema: Mapping[str, JsonValue]

    async def __call__(self, tool_input: Dict[str, JsonValue]) -> str: ...


ToolCallback = Callable[[Dict[str, JsonValue]], Awaitable[str]]

class McpToolFilter(TypedDict, total=False):
    """Exact, case-sensitive MCP tool visibility policy; deny always wins."""
    allow: Sequence[str]
    deny: Sequence[str]

class McpConnection:
    def __init__(self, _factory_only: NoReturn) -> None: ...
    async def list_resources(self, cursor: Optional[str] = ...) -> List[JsonValue]: ...
    async def read_resource(self, uri: str) -> JsonValue: ...
    async def list_prompts(self, cursor: Optional[str] = ...) -> List[JsonValue]: ...
    async def get_prompt(self, name: str, arguments: JsonValue) -> JsonValue: ...

class _LegacyNamespace:
    """Deprecated v0.x compatibility namespace."""
    McpServer: type[McpConnection]

legacy: _LegacyNamespace

async def connect_mcp_http(endpoint: str, name: str, bearer_token: Optional[str] = ..., tool_filter: Optional[McpToolFilter] = ...) -> McpConnection: ...
async def connect_mcp_stdio(program: str, args: Sequence[str], name: str, env: Optional[Mapping[str, str]] = ..., inherit_env: bool = ..., tool_filter: Optional[McpToolFilter] = ...) -> McpConnection: ...


@overload
def tool(
    name: str,
    description: str,
    input_schema: Mapping[str, JsonValue],
    callback: ToolCallback,
) -> Tool: ...


@overload
def tool(
    name: str,
    description: str,
    input_schema: Mapping[str, JsonValue],
    callback: None = ...,
) -> Callable[[ToolCallback], Tool]: ...


class Agent:
    def __init__(self) -> None: ...

    @staticmethod
    def from_env(env: Mapping[str, str]) -> "Agent": ...

    def configure_jsonl_audit(
        self,
        path: str,
        payload_policy: Literal["metadata_only", "full"] = ...,
        failure_mode: Literal["fail_closed", "best_effort"] = ...,
    ) -> None: ...
    def use_memory_file(self, path: str, namespace: str = ...) -> None: ...
    def use_session_file(self, path: str) -> None: ...
    def use_sqlite_memory(self, path: str, namespace: str = ...) -> None: ...
    def use_sqlite_sessions(self, path: str) -> None: ...
    def recover_expired_session(
        self,
        session_id: str,
        *,
        side_effects_reconciled: Literal[True],
    ) -> int:
        """Clear an expired lease after reconciliation; does not execute or resume work."""
        ...
    def register_web_tools(self, allowed_hosts: Sequence[str], search_endpoint: Optional[str] = ..., max_response_bytes: Optional[int] = ...) -> None: ...
    def register_browser_tools(
        self,
        webdriver_endpoint: str,
        session_id: str,
        allowed_hosts: Sequence[str],
        *,
        external_egress_enforced: Literal[True],
    ) -> None:
        """Register browser tools after asserting an exact external host/public-IP boundary."""
        ...
    def register_mcp(self, server: McpConnection) -> None: ...
    def enable_capability_requests(self, gated_tools: Sequence[str]) -> None: ...
    def enable_default_guardrails(self, blocked_input_patterns: Sequence[str] = ...) -> None: ...
    def add_key(self, key: str, provider: Optional[str] = ...) -> str: ...
    def active_providers(self) -> List[str]: ...
    def has_provider(self, provider: str) -> bool: ...
    def capabilities(self) -> AgentCapabilities: ...
    def add_tool(
        self,
        name: str,
        description: str,
        input_schema: Mapping[str, JsonValue],
        callback: Callable[[Dict[str, JsonValue]], Awaitable[str]],
    ) -> None: ...
    def add_tool_definition(self, definition: Tool) -> None: ...
    def register_builtin_tools(self, roots: Sequence[str]) -> None:
        """Register jailed Read/Write/Edit/Glob/Grep tools; Bash stays disabled."""
        ...
    def enable_bash_with_required_containment(
        self, docker: Optional[DockerContainmentOptions] = ...
    ) -> None:
        """Add Bash under Required(Auto), optionally with a digest-pinned Docker fallback."""
        ...
    async def builtin_containment_capabilities(self) -> ContainmentCapabilityReport:
        """Probe the required Bash backends without weakening containment."""
        ...
    def set_permissions(
        self,
        rules: Optional[Sequence[PermissionRule]] = ...,
        default_mode: Literal["allow", "deny", "ask"] = ...,
    ) -> None: ...
    def can_use_tool(
        self,
        callback: Callable[[ApprovalRequest], Awaitable[ApprovalResponse]],
    ) -> None: ...
    def on_user_prompt(
        self,
        callback: Callable[[PromptContext], Awaitable[HookResponse]],
    ) -> None: ...
    def on_pre_tool_use(
        self,
        callback: Callable[[PreToolUseContext], Awaitable[HookResponse]],
        tool: Optional[str] = ...,
    ) -> None: ...
    def on_post_tool_use(
        self,
        callback: Callable[[PostToolUseContext], Awaitable[HookResponse]],
        tool: Optional[str] = ...,
    ) -> None: ...
    def on_post_tool_failure(
        self,
        callback: Callable[[FailureContext], Awaitable[HookResponse]],
        tool: Optional[str] = ...,
    ) -> None: ...
    def on_failure(
        self,
        callback: Callable[[FailureContext], Awaitable[HookResponse]],
    ) -> None: ...
    def on_stop(
        self,
        callback: Callable[[StopContext], Awaitable[None]],
    ) -> None: ...

    async def generate_text(
        self,
        prompt: PromptInput,
        model: Optional[str] = ...,
        max_tokens: int = ...,
    ) -> GeneratedText: ...

    def stream_text(
        self,
        prompt: PromptInput,
        model: Optional[str] = ...,
        max_tokens: int = ...,
    ) -> QueryStream: ...

    def run(self, prompt: PromptInput, options: Optional[RunOptions] = ...) -> QueryStream: ...
    def client(self) -> "Client": ...

    @overload
    async def generate_object(
        self,
        prompt: PromptInput,
        schema: StructuredSchema[T],
        model: Optional[str] = ...,
        max_retries: int = ...,
        max_tokens: int = ...,
        name: Optional[str] = ...,
        provider_options: Optional[ProviderOptions] = ...,
        validator: Optional[SemanticValidator] = ...,
    ) -> GeneratedObject[T]: ...

    @overload
    async def generate_object(
        self,
        prompt: PromptInput,
        schema: Mapping[str, JsonValue],
        model: Optional[str] = ...,
        max_retries: int = ...,
        max_tokens: int = ...,
        name: Optional[str] = ...,
        provider_options: Optional[ProviderOptions] = ...,
        validator: Optional[SemanticValidator] = ...,
    ) -> GeneratedObject[JsonValue]: ...

    @overload
    def stream_object(
        self,
        prompt: PromptInput,
        schema: StructuredSchema[T],
        model: Optional[str] = ...,
        max_retries: int = ...,
        max_tokens: int = ...,
        name: Optional[str] = ...,
        provider_options: Optional[ProviderOptions] = ...,
        validator: Optional[SemanticValidator] = ...,
    ) -> ObjectStream[T]: ...

    @overload
    def stream_object(
        self,
        prompt: PromptInput,
        schema: Mapping[str, JsonValue],
        model: Optional[str] = ...,
        max_retries: int = ...,
        max_tokens: int = ...,
        name: Optional[str] = ...,
        provider_options: Optional[ProviderOptions] = ...,
        validator: Optional[SemanticValidator] = ...,
    ) -> ObjectStream[JsonValue]: ...

    def remember(self, key: str, value: JsonValue) -> None: ...
    def remember_cas(
        self,
        key: str,
        value: JsonValue,
        expected_revision: int,
        plane: Literal["working", "episodic", "semantic"] = ...,
        provenance: Optional[MemoryProvenance] = ...,
    ) -> int: ...
    def recall(self, query: str, limit: int = ...) -> List[MemoryEntry]: ...
    def route(
        self, profiles: Sequence[ModelProfile], request: RouteRequest
    ) -> RouteDecision: ...

    async def run_subagent(
        self,
        spec: SubagentSpec,
        profiles: Sequence[ModelProfile],
        budget: Optional[BudgetLimits] = ...,
        max_parallelism: int = ...,
    ) -> SubagentResult: ...

    def subtask(
        self,
        id: str,
        prompt: str,
        route: ModelRouteRequirements,
        system: Optional[str] = ...,
        allowed_tools: Optional[Sequence[str]] = ...,
        max_turns: int = ...,
        max_tokens: int = ...,
        estimated_input_tokens: int = ...,
    ) -> SubagentSpec: ...

    async def resume_subagent(
        self,
        session_id: str,
        spec: SubagentSpec,
        profiles: Sequence[ModelProfile],
        budget: Optional[BudgetLimits] = ...,
        max_parallelism: int = ...,
    ) -> SubagentResult: ...

    async def fan_out(
        self,
        specs: Sequence[SubagentSpec],
        profiles: Sequence[ModelProfile],
        budget: Optional[BudgetLimits] = ...,
        max_parallelism: int = ...,
    ) -> List[SubagentResult]: ...

    async def parallel(
        self,
        specs: Sequence[SubagentSpec],
        profiles: Sequence[ModelProfile],
        budget: Optional[BudgetLimits] = ...,
        max_parallelism: int = ...,
    ) -> List[SubagentResult]: ...

    async def council(
        self,
        members: Sequence[SubagentSpec],
        synthesizer: SubagentSpec,
        profiles: Sequence[ModelProfile],
        min_successes: int = ...,
        budget: Optional[BudgetLimits] = ...,
        max_parallelism: int = ...,
    ) -> CouncilResult: ...

    def __repr__(self) -> str: ...


DurabilityMode = Literal["sync", "async", "exit"]
DurableRunStatus = Literal[
    "running", "paused", "reconcile_required", "completed", "failed", "cancelled"
]


class DurableRunState(TypedDict):
    schema_version: int
    session_id: str
    run_id: str
    durability: DurabilityMode
    parent_run_id: Optional[str]
    events: List[Dict[str, JsonValue]]
    checkpoints: Dict[str, JsonValue]
    projection: Dict[str, JsonValue]


class Checkpoint(TypedDict):
    checkpoint_id: str
    run_id: str
    event_sequence: int
    parent_checkpoint_id: Optional[str]
    label: Optional[str]
    projection: Dict[str, JsonValue]


class _ResumeCommandRequired(TypedDict):
    command: Literal["resume"]
    command_id: str

class ResumeCommand(_ResumeCommandRequired, total=False):
    approvals: List[Dict[str, JsonValue]]


class ForkCommand(TypedDict):
    command: Literal["fork"]
    command_id: str
    new_run_id: str
    checkpoint_id: str
    side_effects_reconciled: bool


class RewindCommand(TypedDict):
    command: Literal["rewind"]
    command_id: str
    checkpoint_id: str
    side_effects_reconciled: bool


class _CancelCommandRequired(TypedDict):
    command: Literal["cancel"]
    command_id: str


class CancelCommand(_CancelCommandRequired, total=False):
    reason: Optional[str]


DurableCommand = Union[ResumeCommand, ForkCommand, RewindCommand, CancelCommand]


class DurableCommandResult(TypedDict, total=False):
    type: Literal["resumed", "forked", "rewound", "cancelled"]
    sequence: int
    checkpoint_id: str
    run: DurableRunState


class DurableRun:
    def __init__(
        self, session_id: str, run_id: str, durability: DurabilityMode = ...
    ) -> None: ...
    @staticmethod
    def from_state(state: DurableRunState) -> "DurableRun": ...
    @property
    def schema_version(self) -> int: ...
    @property
    def session_id(self) -> str: ...
    @property
    def run_id(self) -> str: ...
    @property
    def durability(self) -> DurabilityMode: ...
    @property
    def status(self) -> DurableRunStatus: ...
    def snapshot(self) -> DurableRunState: ...
    def replace_state(
        self, mutation_id: str, state: JsonValue
    ) -> DurableRunState: ...
    def checkpoint(
        self, checkpoint_key: str, label: Optional[str] = ...
    ) -> Checkpoint: ...
    def pause(self, pause_id: str, reason: str) -> None: ...
    def request_approval(
        self,
        logical_key: str,
        prompt: str,
        payload: JsonValue,
        activity_id: Optional[str] = ...,
    ) -> str: ...
    def complete(self, completion_id: str) -> None: ...
    def fail(self, failure_id: str, error: str) -> None: ...
    def apply_command(self, command: DurableCommand) -> DurableCommandResult: ...


class SimpleTraceAssertion(TypedDict):
    type: Literal[
        "stream_sequence_monotonic",
        "stream_blocks_balanced",
        "durable_sequence_monotonic",
        "no_duplicate_activity_completion",
        "all_required_reconciliations_resolved",
    ]


class ApprovalResolvedTraceAssertion(TypedDict):
    type: Literal["approval_resolved"]
    approval_id: str
    approved: bool


class RunStatusTraceAssertion(TypedDict):
    type: Literal["run_status"]
    status: DurableRunStatus


TraceAssertion = Union[
    SimpleTraceAssertion,
    ApprovalResolvedTraceAssertion,
    RunStatusTraceAssertion,
]


class EvalSuite(TypedDict):
    schema_version: int
    name: str
    assertions: List[TraceAssertion]


class TraceInput(TypedDict, total=False):
    stream_events: List[StreamEvent]
    durable_events: List[Dict[str, JsonValue]]
    run_status: Optional[DurableRunStatus]


class TraceCheck(TypedDict):
    assertion: str
    passed: bool
    message: str


class TraceEvalResult(TypedDict):
    suite: str
    passed: bool
    passed_checks: int
    total_checks: int
    checks: List[TraceCheck]


class Client:
    def __init__(self, agent: Agent) -> None: ...
    def query(self, prompt: PromptInput, options: Optional[RunOptions] = ...) -> QueryStream: ...


def query(
    prompt: PromptInput,
    tools: Optional[Sequence[Tool]] = ...,
    model: Optional[str] = ...,
    permissions: Optional[Sequence[PermissionRule]] = ...,
    options: Optional[RunOptions] = ...,
) -> QueryStream: ...


def evaluate_outcome(outcome: RunOutcome, gates: Sequence[EvalGate]) -> EvalVerdict: ...


def evaluate_trace(suite: EvalSuite, trace: TraceInput) -> TraceEvalResult: ...


__version__: str
