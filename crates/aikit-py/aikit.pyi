from typing import Any, AsyncIterator, Awaitable, Callable, Dict, Generic, List, Literal, Mapping, Optional, Protocol, Sequence, TypedDict, TypeVar, Union, overload

JsonPrimitive = Union[str, int, float, bool, None]
JsonValue = Union[JsonPrimitive, List["JsonValue"], Dict[str, "JsonValue"]]
ProviderOptions = Mapping[str, Mapping[str, JsonValue]]
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


class ModelProfile(TypedDict):
    provider: str
    model: str
    context_window_tokens: int
    max_output_tokens: int
    pricing: Optional[ModelPricing]
    quality_score: int
    skills: List[str]
    capabilities: List[Union[str, Dict[str, str]]]


class RouteRequest(TypedDict):
    policy: Dict[str, JsonValue]
    active_providers: List[str]
    estimated_input_tokens: int
    required_output_tokens: int
    max_cost_usd: Optional[float]
    required_skills: List[str]
    required_capabilities: List[Union[str, Dict[str, str]]]


class RoutingOptions(TypedDict):
    profiles: Sequence[ModelProfile]
    request: RouteRequest


class RunOptions(TypedDict, total=False):
    model: str
    fallback_models: List[str]
    max_tokens: int
    max_turns: int
    provider_options: ProviderOptions
    budget: BudgetPolicy
    retry: RetryPolicy
    routing: RoutingOptions


class RouteDecision(TypedDict):
    profile: ModelProfile
    estimated_cost_usd: Optional[float]
    policy: Dict[str, JsonValue]
    eligible_models: int


class MemoryEntry(TypedDict):
    namespace: str
    key: str
    value: JsonValue
    tags: List[str]
    importance: int
    created_unix_ms: int
    updated_unix_ms: int


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


class QueryStream(AsyncIterator[StreamDelta]):
    def __aiter__(self) -> "QueryStream": ...
    async def __anext__(self) -> StreamDelta: ...
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
    ) -> ObjectStream[JsonValue]: ...

    def remember(self, key: str, value: JsonValue) -> None: ...
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


__version__: str
