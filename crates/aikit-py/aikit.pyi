"""Typed Python binding for the aikit Rust runtime (distribution name ``aikit-runtime``).

Start with ``Agent``. ``Agent.from_env({})`` builds a keyless agent whose default
``mock-1`` / ``mock-structured`` models are deterministic and need no network, so every
example below runs offline. Activate live providers with ``Agent.add_key`` or by passing
provider environment variables to ``Agent.from_env``.

Runtime failures raise ``AikitError`` (branch on ``exc.code``); malformed host input raises
``ValueError`` before any provider call starts. All governance (permissions, hooks,
approvals, budgets, containment) is evaluated in the Rust core; Python supplies callbacks
and structured configuration only.

This is an alpha (0.3.x) source-first binding; see the package README for the build steps.
"""

from typing import Any, AsyncIterator, Awaitable, Callable, Dict, Generic, List, Literal, Mapping, NoReturn, Optional, Protocol, Sequence, TypedDict, TypeVar, Union, final, overload

JsonPrimitive = Union[str, int, float, bool, None]
JsonValue = Union[JsonPrimitive, List["JsonValue"], Dict[str, "JsonValue"]]
ProviderOptions = Mapping[str, Mapping[str, JsonValue]]
ProviderMetadata = Dict[str, List[JsonValue]]
T = TypeVar("T")
T_co = TypeVar("T_co", covariant=True)


# Stable ``AikitError.code`` / ``ErrorInfo["code"]`` values. Branch on these rather than on
# message text. Common first-run codes: ``provider_auth`` (missing or rejected credential:
# call ``Agent.add_key``), ``configuration`` (invalid setup), ``permission_denied`` (policy
# denied a tool), ``budget_exceeded`` / ``max_turns`` (run limits reached).
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


class _ErrorInfoRequired(TypedDict):
    code: ErrorCode
    message: str
    provider: Optional[str]
    model: Optional[str]
    status: Optional[int]
    retry_after_ms: Optional[int]
    retryable: bool


class ErrorInfo(_ErrorInfoRequired, total=False):
    """Stable, redacted error envelope carried by ``AikitError.info`` and error deltas."""
    warnings: List["ProviderWarning"]


class AikitError(RuntimeError):
    """Typed terminal runtime failure; branch on ``code`` instead of parsing text.

    Raised by awaited generation calls, when starting a run that cannot be configured
    (for example ``provider_auth`` for a model whose provider has no key), by iterating a
    failed ``ObjectStream``, and by durable-run transitions (``code == "conflict"``).
    ``QueryStream`` failures after start do not raise: they arrive as an ``"error"`` delta
    and as ``outcome()["terminal_status"]``. ``info`` carries the same stable, redacted
    envelope (provider, model, HTTP status, ``retryable``, ``retry_after_ms``) the Rust
    core records; the message text is for humans and may change between releases.

    Invalid host input (unknown option fields, malformed schemas or messages) raises
    ``ValueError`` instead, before any provider work starts. ``McpConnection`` transport
    failures raise a plain ``RuntimeError``. The external-agent helpers raise
    ``AikitError`` with the code only as a ``"<code>: "`` message prefix, so read
    ``getattr(exc, "code", None)`` defensively there.

    For example, retry only when the core says it is safe::

        try:
            result = await agent.generate_text("hi", model="claude-sonnet-4-5")
        except aikit.AikitError as exc:
            if exc.code == "provider_auth":
                ...  # call agent.add_key(...) first
            elif exc.info["retryable"]:
                ...  # back off for exc.info["retry_after_ms"] and retry
            else:
                raise
    """

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


def validate_media_input(media: MediaInput) -> MediaInput:
    """Validate and return a canonical provider-neutral media input; raises ``ValueError``."""
    ...


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


class MediaInputBlock(TypedDict):
    type: Literal["media_input"]
    media: MediaInput


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
    MediaInputBlock,
    CitationBlock,
]

# Canonical v0.3 name; ContentBlock remains the v0.x compatibility spelling.
ContentPart = ContentBlock


class Message(TypedDict):
    role: Literal["system", "user", "assistant", "tool"]
    content: List[ContentBlock]


PromptInput = Union[str, Sequence[Message]]


class GeneratedText(TypedDict):
    """Result of ``Agent.generate_text``."""
    text: str
    usage: Usage
    stop_reason: Optional[str]
    messages: List[Message]
    provider_metadata: Dict[str, List[JsonValue]]
    warnings: List["ProviderWarning"]


class GeneratedObject(TypedDict, Generic[T]):
    value: T
    fidelity: Literal["native_constrained", "forced_tool_call", "prompted_and_parsed"]
    attempts: int
    provider_metadata: Dict[str, List[JsonValue]]
    warnings: List["ProviderWarning"]


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
    """One declarative rule; ``pattern`` is a regex over the input's string values (or ``field``)."""
    id: str
    pattern: str
    field: str


class ApprovalRequest(TypedDict):
    """What the ``can_use_tool`` approver receives for an ``"ask"`` decision."""
    run_id: str
    turn: int
    tool_use_id: str
    tool: str
    input: JsonValue


PermissionUpdate = Literal["allow_exact_input", "allow_tool"]


class _ApprovalAllowActionRequired(TypedDict):
    action: Literal["allow"]


class ApprovalAllowActionResponse(_ApprovalAllowActionRequired, total=False):
    updated_input: JsonValue
    updated_permissions: List[PermissionUpdate]


class _ApprovalAllowDecisionRequired(TypedDict):
    decision: Literal["allow"]


class ApprovalAllowDecisionResponse(_ApprovalAllowDecisionRequired, total=False):
    updated_input: JsonValue
    updated_permissions: List[PermissionUpdate]


ApprovalAllowResponse = Union[
    ApprovalAllowActionResponse,
    ApprovalAllowDecisionResponse,
]


class _ApprovalDenyActionRequired(TypedDict):
    action: Literal["deny"]


class ApprovalDenyActionResponse(_ApprovalDenyActionRequired, total=False):
    message: str
    interrupt: bool


class _ApprovalDenyDecisionRequired(TypedDict):
    decision: Literal["deny"]


class ApprovalDenyDecisionResponse(_ApprovalDenyDecisionRequired, total=False):
    message: str
    interrupt: bool


ApprovalDenyResponse = Union[
    ApprovalDenyActionResponse,
    ApprovalDenyDecisionResponse,
]


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


class HookContinueResponse(TypedDict):
    action: Literal["continue"]


class PromptHookRewriteResponse(TypedDict):
    action: Literal["rewrite"]
    prompt: str


class _HookBlockRequired(TypedDict):
    action: Literal["block"]


class HookBlockResponse(_HookBlockRequired, total=False):
    message: str


class PreToolHookRewriteResponse(TypedDict):
    action: Literal["rewrite"]
    input: JsonValue


class PostToolHookRewriteResponse(TypedDict):
    action: Literal["rewrite"]
    output: str


class _PostToolHookErrorRequired(TypedDict):
    action: Literal["error", "mark_error"]


class PostToolHookErrorResponse(_PostToolHookErrorRequired, total=False):
    message: str


class FailureHookRewriteResponse(TypedDict):
    action: Literal["rewrite"]
    error: str


PromptHookResponse = Optional[Union[
    Literal["continue"],
    HookContinueResponse,
    PromptHookRewriteResponse,
    HookBlockResponse,
]]
PreToolHookResponse = Optional[Union[
    Literal["continue"],
    HookContinueResponse,
    PreToolHookRewriteResponse,
    HookBlockResponse,
]]
PostToolHookResponse = Optional[Union[
    Literal["continue"],
    HookContinueResponse,
    PostToolHookRewriteResponse,
    PostToolHookErrorResponse,
]]
FailureHookResponse = Optional[Union[
    Literal["continue"],
    HookContinueResponse,
    FailureHookRewriteResponse,
]]
HookResponse = Union[
    PromptHookResponse,
    PreToolHookResponse,
    PostToolHookResponse,
    FailureHookResponse,
]


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
    """Per-run token and/or USD cost ceiling; exceeding it ends the run as ``budget_exceeded``."""
    max_total_tokens: int
    max_cost_usd: float
    pricing: RunPricing


class RetryPolicy(TypedDict, total=False):
    """Bounded per-model retry and backoff for retryable provider failures."""
    max_attempts_per_model: int
    base_delay_ms: int
    max_delay_ms: int
    per_attempt_timeout_ms: int


CapabilityState = Literal["supported", "unsupported", "unknown"]
CompatibilityMode = Literal["strict", "warn", "best_effort"]


class CustomModelCapability(TypedDict):
    custom: str


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
    CustomModelCapability,
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


def validate_model_profile(profile: ModelProfile) -> ModelProfile:
    """Validate one profile with the routing catalog's invariants; raises ``ValueError``."""
    ...
def model_capability_state(
    profile: ModelProfile, capability: ModelCapability
) -> CapabilityState:
    """Return ``"supported"``, ``"unsupported"``, or ``"unknown"`` for one capability.

    ``"unknown"`` is never collapsed to ``False``; strict routing treats it as unmet.

    Raises:
        ValueError: Malformed profile or capability.
    """
    ...


class CatalogSource(TypedDict):
    provider: str
    reference: str
    url: str


class ModelCatalogSnapshot(TypedDict):
    schema_version: int
    catalog_version: str
    verified_at: str
    sources: List[CatalogSource]
    profiles: List[ModelProfile]


class ResolvedModelCatalog(ModelCatalogSnapshot):
    shipped_hash: str
    overrides_hash: str
    override_count: int


def shipped_model_catalog() -> ModelCatalogSnapshot:
    """Return the reviewed, versioned model catalog compiled into this package (no network)."""
    ...
def resolve_model_catalog(overrides: Sequence[ModelProfile] = ...) -> ResolvedModelCatalog:
    """Overlay caller-owned profiles on the shipped catalog without mutating it.

    Raises:
        ValueError: An override profile is invalid.
    """
    ...


class _ExternalDecisionMetadataRequired(TypedDict):
    policy_rule_id: str
    input_summary: str


class ExternalDecisionMetadata(_ExternalDecisionMetadataRequired, total=False):
    risk_evidence: List[str]
    evaluator_revision: Optional[str]


class GlobalPolicyScope(TypedDict):
    scope: Literal["global"]


class TenantPolicyScope(TypedDict):
    scope: Literal["tenant"]
    tenant_id: str


class AgentPolicyScope(TypedDict):
    scope: Literal["agent"]
    agent_id: str


class RunPolicyScope(TypedDict):
    scope: Literal["run"]
    run_id: str


class ToolPolicyScope(TypedDict):
    scope: Literal["tool"]
    tool: str


PolicyScope = Union[
    GlobalPolicyScope,
    TenantPolicyScope,
    AgentPolicyScope,
    RunPolicyScope,
    ToolPolicyScope,
]


class _ScopedPolicyRuleRequired(TypedDict):
    id: str
    scope: PolicyScope
    effect: Literal["allow", "ask", "deny"]


class ScopedPolicyRule(_ScopedPolicyRuleRequired, total=False):
    reason: Optional[str]


class PolicyDocument(TypedDict):
    schema_version: int
    default_effect: Literal["allow", "ask", "deny"]
    rules: List[ScopedPolicyRule]


class PolicySnapshot(TypedDict):
    policy: PolicyDocument
    hash: str


def seal_policy_snapshot(policy: PolicyDocument) -> PolicySnapshot:
    """Seal a policy document into the integrity-checked snapshot ``DurableRun`` accepts.

    Raises:
        ValueError: Malformed policy document.
    """
    ...


class _GovernanceBindingRequired(TypedDict):
    schema_version: int
    policy_snapshot_hash: str
    run_id: str
    binding_hash: str


class GovernanceBinding(_GovernanceBindingRequired, total=False):
    tenant_id: str
    agent_id: str


def seal_governance_binding(
    policy_snapshot: PolicySnapshot,
    run_id: str,
    tenant_id: Optional[str] = ...,
    agent_id: Optional[str] = ...,
) -> GovernanceBinding:
    """Seal a policy snapshot together with its tenant/agent/run scope identity.

    Pass the result to ``DurableRun.with_governance_binding``.

    Raises:
        ValueError: Malformed snapshot or scope identifiers.
    """
    ...


class AuditablePolicyDecision(TypedDict):
    engine: Literal["opa", "cedar", "native"]
    effect: Literal["allow", "ask", "deny"]
    decision_id: Optional[str]
    deciding_rule_id: Optional[str]
    matched_rule_ids: List[str]
    input_summary: str
    risk_evidence: List[str]
    evaluator_revision: Optional[str]


class _OpaDetailedResultRequired(TypedDict):
    effect: Literal["allow", "ask", "deny"]


class OpaDetailedResult(_OpaDetailedResultRequired, total=False):
    rule_id: str
    matched_rule_ids: List[str]
    risk_evidence: List[str]
    partial: bool


class _OpaDecisionResponseRequired(TypedDict):
    result: Union[bool, OpaDetailedResult]


class OpaDecisionResponse(_OpaDecisionResponseRequired, total=False):
    decision_id: str
    metrics: JsonValue
    provenance: JsonValue
    warning: str


class _CedarDecisionResponseRequired(TypedDict):
    decision: Literal["Allow", "Deny"]


class CedarDiagnostics(TypedDict, total=False):
    reasons: List[str]
    errors: List[str]


class CedarDecisionResponse(_CedarDecisionResponseRequired, total=False):
    decision_id: str
    permit_policy_ids: List[str]
    forbid_policy_ids: List[str]
    diagnostics: CedarDiagnostics
    evaluator_revision: str


def normalize_opa_decision(
    response: OpaDecisionResponse, metadata: ExternalDecisionMetadata
) -> AuditablePolicyDecision:
    """Normalize a completed OPA Data API response into auditable, fail-closed evidence.

    aikit does not call OPA; pass the response you already received. A missing/null
    (undefined) or partial result is an error, so it can never be read as an allow.

    Raises:
        ValueError: Malformed, undefined, or partial response, or malformed metadata.
    """
    ...
def normalize_cedar_decision(
    response: CedarDecisionResponse, metadata: ExternalDecisionMetadata
) -> AuditablePolicyDecision:
    """Normalize a completed Cedar authorization response into auditable, deny-wins evidence.

    aikit does not call Cedar. A matched forbid or an evaluator diagnostic error is a deny
    even if an upstream wrapper labels it ``Allow``.

    Raises:
        ValueError: Malformed response or metadata.
    """
    ...


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


def validate_media_artifact(artifact: MediaArtifact) -> MediaArtifact:
    """Validate and return immutable media-artifact metadata; raises ``ValueError``."""
    ...


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


class _CompactionOptionsRequired(TypedDict):
    max_context_tokens: int


class CompactionOptions(_CompactionOptionsRequired, total=False):
    keep_recent_messages: int
    summary_model: str
    summary_max_tokens: int


class RunOptions(TypedDict, total=False):
    """Options for ``Agent.run`` / ``Client.query`` / ``aikit.query``; unknown keys raise ``ValueError``."""
    model: str
    fallback_models: List[str]
    max_tokens: int
    max_turns: int
    provider_options: ProviderOptions
    compatibility_mode: CompatibilityMode
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
    """Canonical record of one run; pass a terminal one to ``evaluate_outcome``."""
    final_text: str
    provider_metadata: Dict[str, List[JsonValue]]
    warnings: List["ProviderWarning"]
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


class WarningDelta(TypedDict):
    type: Literal["warning"]
    warning: "ProviderWarning"


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
    WarningDelta,
    UsageDelta,
    MessageStopDelta,
    ErrorDelta,
]




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
    """Versioned start/delta/end event view over the same run as its ``QueryStream``.

    Obtain it with ``QueryStream.events(response_id)``. It shares the underlying run, so
    consume either the delta view or this event view, never both concurrently. Use
    ``async with`` for deterministic cleanup on early exit, exactly like ``QueryStream``.
    """

    def __aiter__(self) -> "QueryEventStream": ...
    async def __anext__(self) -> StreamEvent: ...
    async def __aenter__(self) -> "QueryEventStream": ...
    async def __aexit__(self, exc_type: Any, exc_value: Any, traceback: Any) -> bool: ...
    def cancel(self) -> None:
        """Request cooperative cancellation without waiting; see ``QueryStream.cancel``."""
        ...
    def is_cancelled(self) -> bool:
        """Return whether cancellation was requested for the shared run."""
        ...
    async def aclose(self) -> RunOutcome:
        """Cancel, drain the run, and return its terminal ``RunOutcome``."""
        ...
    def outcome(self) -> RunOutcome:
        """Return the current recorder snapshot; terminal after exhaustion or ``aclose()``."""
        ...


class QueryStream(AsyncIterator[StreamDelta]):
    """Single-consumer async iterator of canonical ``StreamDelta`` dicts for one run.

    Returned by ``Agent.run``, ``Agent.stream_text``, ``Client.query`` and ``aikit.query``.
    Branch on each delta's ``"type"`` (``"text_delta"``, ``"tool_call_start"``,
    ``"usage"``, ``"error"``, ...). Failures after the run starts are delivered as an
    ``"error"`` delta and recorded in ``outcome()["terminal_status"]``; they are not
    raised from ``__anext__``. Driving ``__anext__`` concurrently raises ``RuntimeError``.

    Prefer ``async with`` so leaving the loop early still runs Stop hooks, audit emission,
    and recorder finalization (a plain ``break`` does not call ``aclose()``)::

        async with agent.run("Summarize the report", {"max_turns": 4}) as stream:
            async for delta in stream:
                if delta["type"] == "text_delta":
                    print(delta["text"], end="")
        print(stream.outcome()["terminal_status"])
    """

    def __aiter__(self) -> "QueryStream": ...
    async def __anext__(self) -> StreamDelta: ...
    def events(self, response_id: str) -> QueryEventStream:
        """Switch to the versioned start/delta/end event view of this same run.

        Args:
            response_id: Caller-chosen, non-empty id stamped on every emitted event.

        Raises:
            ValueError: ``response_id`` is empty or whitespace.
        """
        ...
    async def __aenter__(self) -> "QueryStream": ...
    async def __aexit__(self, exc_type: Any, exc_value: Any, traceback: Any) -> bool: ...
    def cancel(self) -> None:
        """Request cooperative cancellation immediately and return without waiting.

        Use ``await stream.aclose()`` instead when you must wait for Stop hooks, audit
        emission, and recorder finalization before continuing.
        """
        ...
    def is_cancelled(self) -> bool:
        """Return whether cancellation was requested for this run."""
        ...
    async def aclose(self) -> RunOutcome:
        """Cancel, drain the core driver, and return the terminal canonical ``RunOutcome``."""
        ...
    def outcome(self) -> RunOutcome:
        """Return the current recorder snapshot.

        The snapshot is terminal once the stream is exhausted or ``aclose()`` returned;
        before that ``terminal_status`` may still be ``"running"``. Pass a terminal
        outcome to ``aikit.evaluate_outcome`` for deterministic gates.
        """
        ...


class ObjectStream(AsyncIterator[ObjectStreamEvent[T]], Generic[T]):
    """Single-consumer async iterator of structured-output events from ``Agent.stream_object``.

    Yields ``attempt_started``, ``delta``, and ``validation_failed`` events, then exactly
    one ``completed`` event whose ``object["value"]`` is schema-valid (materialized as the
    Pydantic model when a Pydantic v2 class was passed as the schema). A terminal failure
    raises ``AikitError`` from ``__anext__``.
    """

    def __aiter__(self) -> "ObjectStream[T]": ...
    async def __anext__(self) -> ObjectStreamEvent[T]: ...


class Tool(Protocol):
    """A host tool definition: metadata plus an ``async`` callable returning ``str``.

    ``aikit.tool(...)`` produces one from any ``async def``; any object with these three
    attributes that is itself awaitable-callable also satisfies the protocol. The
    callable receives the model's input dict and must resolve to ``str`` (anything else
    is recorded as a tool error and fed back to the model).
    """

    name: str
    description: str
    input_schema: Mapping[str, JsonValue]

    async def __call__(self, tool_input: Dict[str, JsonValue]) -> str: ...


ToolCallback = Callable[[Dict[str, JsonValue]], Awaitable[str]]


A2aRole = Literal["ROLE_USER", "ROLE_AGENT"]
A2aTaskState = Literal[
    "TASK_STATE_SUBMITTED",
    "TASK_STATE_WORKING",
    "TASK_STATE_INPUT_REQUIRED",
    "TASK_STATE_AUTH_REQUIRED",
    "TASK_STATE_COMPLETED",
    "TASK_STATE_FAILED",
    "TASK_STATE_CANCELED",
    "TASK_STATE_REJECTED",
]
A2aGovernanceDenialCode = Literal[
    "invalid_request",
    "missing_principal",
    "missing_scope",
    "principal_mismatch",
    "unknown_target",
    "state_conflict",
    "invalid_approval",
    "duplicate_conflict",
]


class A2aTextPart(TypedDict):
    kind: Literal["text"]
    text: str


class A2aDataPart(TypedDict):
    kind: Literal["data"]
    data: JsonValue


class A2aFilePart(TypedDict):
    kind: Literal["file"]
    uri: str
    media_type: str


A2aPart = Union[A2aTextPart, A2aDataPart, A2aFilePart]


class _A2aMessageRequired(TypedDict):
    message_id: str
    role: A2aRole
    parts: List[A2aPart]


class A2aMessage(_A2aMessageRequired, total=False):
    context_id: str
    task_id: str
    metadata: Dict[str, JsonValue]


class A2aListTasksRequest(TypedDict, total=False):
    tenant: str
    contextId: str
    status: A2aTaskState
    pageSize: int
    pageToken: str


class _A2aCorrelationIdentityRequired(TypedDict):
    correlation_id: str
    request_id: str


class A2aCorrelationIdentity(_A2aCorrelationIdentityRequired, total=False):
    session_id: str
    run_id: str


class _A2aProtocolPrincipalRequired(TypedDict):
    subject: str


class A2aProtocolPrincipal(_A2aProtocolPrincipalRequired, total=False):
    tenant_id: str
    scopes: Sequence[str]


class A2aGovernanceAllowed(TypedDict):
    status: Literal["allowed"]


class A2aGovernanceDenied(TypedDict):
    status: Literal["denied"]
    code: A2aGovernanceDenialCode
    reason: str


A2aGovernanceAuthorization = Union[A2aGovernanceAllowed, A2aGovernanceDenied]


class _A2aGovernanceEnvelopeRequired(TypedDict):
    schema_version: int
    protocol: Literal["a2a"]
    correlation: A2aCorrelationIdentity
    operation: str
    target: str
    required_scopes: List[str]
    authorization: A2aGovernanceAuthorization


class A2aGovernanceEnvelope(_A2aGovernanceEnvelopeRequired, total=False):
    principal: A2aProtocolPrincipal


class A2aRunMapping(TypedDict):
    context_id: str
    session_id: str
    task_id: str
    run_id: str
    message_id: str


class _A2aTaskRecordRequired(TypedDict):
    """Internal mapper record; never serialize this directly as an A2A wire Task DTO."""

    mapping: A2aRunMapping
    state: A2aTaskState
    owner_subject: str
    created_revision: int
    updated_revision: int


class A2aTaskRecord(_A2aTaskRecordRequired, total=False):
    owner_tenant_id: str
    status_message: str


class _A2aMessageReceiptRequired(TypedDict):
    message: A2aMessage
    mapping: A2aRunMapping
    owner_subject: str
    accepted_revision: int


class A2aMessageReceipt(_A2aMessageReceiptRequired, total=False):
    owner_tenant_id: str


A2aDispatchOutboxState = Literal["queued", "running", "reconcile_pending", "settled"]
A2aSendResponsePolicy = Literal["blocking", "immediate", "streaming"]


class _A2aDispatchOutboxRecordRequired(TypedDict):
    dispatch_id: str
    owner_subject: str
    message_id: str
    task_id: str
    context_id: str
    session_id: str
    run_id: str
    message: A2aMessage
    envelope: A2aGovernanceEnvelope
    state: A2aDispatchOutboxState
    response: JsonValue
    response_policy: A2aSendResponsePolicy
    attempts: int
    created_revision: int
    updated_revision: int


class A2aDispatchOutboxRecord(_A2aDispatchOutboxRecordRequired, total=False):
    owner_tenant_id: str
    resumed_from: A2aTaskState
    immediate_response: A2aTaskRecord
    last_error: str


class A2aTaskPage(TypedDict):
    tasks: List[A2aTaskRecord]
    nextPageToken: str
    pageSize: int
    totalSize: int


class A2aDispatchMessageAction(TypedDict):
    kind: Literal["dispatch_message"]
    message: A2aMessage
    mapping: A2aRunMapping
    resumed_from: Optional[A2aTaskState]


class A2aDuplicateMessageAction(TypedDict):
    kind: Literal["duplicate_message"]
    receipt: A2aMessageReceipt


class A2aGetTaskAction(TypedDict):
    kind: Literal["get_task"]
    task: A2aTaskRecord


class A2aListTasksAction(TypedDict):
    kind: Literal["list_tasks"]
    page: A2aTaskPage


class A2aCancelTaskAction(TypedDict):
    kind: Literal["cancel_task"]
    task: A2aTaskRecord


A2aAction = Union[
    A2aDispatchMessageAction,
    A2aDuplicateMessageAction,
    A2aGetTaskAction,
    A2aListTasksAction,
    A2aCancelTaskAction,
]


class A2aGovernedActionAllowed(TypedDict):
    envelope: A2aGovernanceEnvelope
    action: A2aAction


class A2aGovernedActionDenied(TypedDict):
    envelope: A2aGovernanceEnvelope


A2aGovernedAction = Union[A2aGovernedActionAllowed, A2aGovernedActionDenied]


class _A2aContextOwnerRequired(TypedDict):
    subject: str


class A2aContextOwner(_A2aContextOwnerRequired, total=False):
    tenant_id: str


class A2aMapperState(TypedDict):
    """Canonical persisted mapping state, not an A2A network payload."""

    schema_version: int
    contexts: Dict[str, str]
    context_owners: Dict[str, A2aContextOwner]
    tasks: Dict[str, A2aTaskRecord]
    receipts: Dict[str, A2aMessageReceipt]
    dispatch_outbox: Dict[str, A2aDispatchOutboxRecord]
    cancellation_outbox: Dict[str, JsonValue]
    pending_events: Dict[str, JsonValue]
    next_sequence: int
    revision: int


@final
class A2aMapper:
    """Rust-core A2A mapping state; not a network listener or wire Task serializer.

    Maps authenticated A2A operations onto owner-scoped contexts and tasks and returns a
    governed action (allowed or denied with a typed reason) for the host to execute.
    Mutating calls commit only when the whole operation succeeds. Persist ``snapshot()``
    and rebuild with ``A2aMapper.from_state``; snapshots are internal state, not A2A
    response objects. Malformed input and rejected state transitions raise
    ``ValueError`` (the message starts with a stable ``"<code>: "`` prefix).
    """

    def __init__(self) -> None: ...

    @staticmethod
    def from_state(state: A2aMapperState) -> "A2aMapper":
        """Rebuild a mapper from a ``snapshot()``; raises ``ValueError`` if it is invalid."""
        ...

    def snapshot(self) -> A2aMapperState:
        """Return the canonical persistable mapping state."""
        ...
    def send_message(
        self,
        message: A2aMessage,
        correlation: A2aCorrelationIdentity,
        principal: Optional[A2aProtocolPrincipal] = ...,
        now_unix_ms: Optional[int] = ...,
    ) -> A2aGovernedAction:
        """Map an idempotent ``SendMessage`` into a dispatch, duplicate, or denial action.

        ``now_unix_ms`` defaults to the host wall clock.
        """
        ...
    def list_tasks(
        self,
        request: A2aListTasksRequest,
        correlation: A2aCorrelationIdentity,
        principal: Optional[A2aProtocolPrincipal] = ...,
    ) -> A2aGovernedAction:
        """Return the caller-owned tasks page as a governed action (read-only)."""
        ...
    def get_task(
        self,
        task_id: str,
        correlation: A2aCorrelationIdentity,
        principal: Optional[A2aProtocolPrincipal] = ...,
    ) -> A2aGovernedAction:
        """Return one caller-owned task as a governed action (read-only)."""
        ...
    def cancel_task(
        self,
        task_id: str,
        correlation: A2aCorrelationIdentity,
        principal: Optional[A2aProtocolPrincipal] = ...,
    ) -> A2aGovernedAction:
        """Decide a cancellation for one caller-owned task and record it when allowed."""
        ...
    def mark_dispatch_running(self, dispatch_id: str) -> A2aMapperState:
        """Claim a queued/reconcile-pending dispatch and return the snapshot with its new attempt.

        Pass that exact attempt number to ``transition_dispatch_task``.
        """
        ...
    def mark_dispatch_reconcile_pending(
        self, dispatch_id: str, error: str
    ) -> A2aMapperState:
        """Move a queued/running dispatch into startup-safe reconciliation.

        Only a sanitized reason is persisted, not the raw ``error`` text.
        """
        ...
    def transition_dispatch_task(
        self,
        dispatch_id: str,
        expected_attempt: int,
        state: A2aTaskState,
        status_message: Optional[str] = ...,
        now_unix_ms: Optional[int] = ...,
    ) -> A2aMapperState:
        """Settle a dispatch's task state, fenced by ``expected_attempt``.

        A stale callback from an older attempt is rejected instead of overwriting a newer
        attempt.
        """
        ...
    def transition_task(
        self,
        task_id: str,
        state: A2aTaskState,
        status_message: Optional[str] = ...,
        now_unix_ms: Optional[int] = ...,
    ) -> A2aMapperState:
        """Admin-only task state change; open dispatches must use ``transition_dispatch_task``."""
        ...


class McpToolFilter(TypedDict, total=False):
    """Exact, case-sensitive MCP tool visibility policy; deny always wins."""
    allow: Sequence[str]
    deny: Sequence[str]

async def connect_mcp_http(
    endpoint: str,
    name: str,
    bearer_token: Optional[str] = ...,
    tool_filter: Optional[McpToolFilter] = ...,
) -> "McpConnection":
    """Connect to a Streamable HTTP MCP server, initialize it, and discover its tools.

    Args:
        endpoint: Absolute ``https`` URL of the MCP endpoint (plain ``http`` is accepted
            only for ``localhost`` / loopback addresses).
        name: Local connection name used to identify this server's tools.
        bearer_token: Optional token sent as ``Authorization: Bearer ...``.
        tool_filter: Optional exact allow/deny visibility policy; ``deny`` always wins.
            Omit for allow-all; ``{"allow": []}`` exposes no tools.

    Returns:
        A connection to pass to ``Agent.register_mcp``.

    Raises:
        ValueError: Invalid or non-HTTPS remote endpoint, or invalid ``tool_filter``.
        RuntimeError: Initialization, discovery, or a bounded transport limit failed.

    Example::

        server = await aikit.connect_mcp_http(
            "https://mcp.example.com", "work", tool_filter={"allow": ["search"]}
        )
        agent.register_mcp(server)
    """
    ...


async def connect_mcp_stdio(
    program: str,
    args: Sequence[str],
    name: str,
    env: Optional[Mapping[str, str]] = ...,
    inherit_env: bool = ...,
    tool_filter: Optional[McpToolFilter] = ...,
) -> "McpConnection":
    """Spawn a local MCP server over stdio, initialize it, and discover its tools.

    Args:
        program: Executable to spawn (no shell is involved).
        args: Arguments passed to ``program``.
        name: Local connection name used to identify this server's tools.
        env: Extra environment variables for the child process.
        inherit_env: Defaults to ``False``: the child does not inherit this process's
            environment (and therefore none of its API keys) unless you opt in.
        tool_filter: Optional exact allow/deny visibility policy; ``deny`` always wins.

    Returns:
        A connection to pass to ``Agent.register_mcp``.

    Raises:
        ValueError: Invalid ``tool_filter``.
        RuntimeError: Spawn, initialization, or tool discovery failed.
    """
    ...


class McpConnection:
    """A live, initialized MCP client connection with its filtered tool list.

    Create it with ``connect_mcp_http`` or ``connect_mcp_stdio`` (there is no public
    constructor), then call ``Agent.register_mcp``. Resource and prompt helpers raise
    ``RuntimeError`` on protocol or transport failure.
    """

    def __init__(self, _factory_only: NoReturn) -> None: ...
    async def list_resources(self, cursor: Optional[str] = ...) -> List[JsonValue]:
        """List server resources, following pagination (from ``cursor`` when given) within bounds."""
        ...
    async def read_resource(self, uri: str) -> JsonValue:
        """Read one resource by URI and return the server's JSON result."""
        ...
    async def list_prompts(self, cursor: Optional[str] = ...) -> List[JsonValue]:
        """List server prompts, following pagination (from ``cursor`` when given) within bounds."""
        ...
    async def get_prompt(self, name: str, arguments: JsonValue) -> JsonValue:
        """Render one server prompt with JSON ``arguments`` and return the server's result."""
        ...

class _LegacyNamespace:
    """Deprecated v0.x compatibility namespace; ``legacy.McpServer`` is ``McpConnection``."""
    McpServer: type[McpConnection]

legacy: _LegacyNamespace


@overload
def tool(
    name: str,
    description: str,
    input_schema: Mapping[str, JsonValue],
    callback: ToolCallback,
) -> Tool:
    """Attach tool metadata to an ``async def`` and return it as a ``Tool``.

    Without ``callback`` this is a decorator factory; with one it annotates and returns the
    same callable, which stays directly awaitable. Register the result with
    ``Agent.add_tool_definition`` or pass it in ``aikit.query(tools=[...])``.

    Raises:
        ValueError: ``input_schema`` is not JSON-compatible or ``callback`` is not callable.

    Example::

        @aikit.tool("lookup", "Look up one market symbol", {
            "type": "object",
            "properties": {"symbol": {"type": "string"}},
            "required": ["symbol"],
        })
        async def lookup(tool_input: dict) -> str:
            return f"price:{tool_input['symbol']}"

        agent.add_tool_definition(lookup)
    """
    ...


@overload
def tool(
    name: str,
    description: str,
    input_schema: Mapping[str, JsonValue],
    callback: None = ...,
) -> Callable[[ToolCallback], Tool]: ...


class Agent:
    """The governed, multi-provider agent: credentials, tools, policy, hooks, and runs.

    ``Agent()`` activates providers from this process's environment variables;
    ``Agent.from_env(mapping)`` reads only the mapping you pass, so ``from_env({})`` gives a
    keyless agent that can still use the deterministic ``mock-1`` / ``mock-structured``
    models. Configuration methods mutate the agent in place and raise ``ValueError`` on
    invalid input; configure everything before starting runs.

    A model id selects its provider by prefix (``claude*`` -> anthropic, ``gpt*`` /
    ``o1``/``o3``/``o4`` -> openai, ``gemini*`` -> google, ``deepseek*``,
    ``openrouter:``, ``groq:``, ``mistral:``, ``xai:`` / ``grok-``). Starting a run for a
    provider without a key raises ``AikitError`` with ``code == "provider_auth"``.

    For example, a keyless first run::

        import asyncio, aikit

        async def main() -> None:
            agent = aikit.Agent.from_env({})
            result = await agent.generate_text("Say hello")
            print(result["text"])

        asyncio.run(main())
    """

    def __init__(self) -> None:
        """Build an agent whose providers come from this process's environment variables."""
        ...

    @staticmethod
    def from_env(env: Mapping[str, str]) -> "Agent":
        """Build an agent from an explicit ``{VAR: value}`` mapping instead of ``os.environ``.

        Recognized variables: ``ANTHROPIC_API_KEY``, ``OPENAI_API_KEY``,
        ``DEEPSEEK_API_KEY``, ``GEMINI_API_KEY`` / ``GOOGLE_API_KEY``,
        ``OPENROUTER_API_KEY``, ``GROQ_API_KEY``, ``MISTRAL_API_KEY``, ``XAI_API_KEY``.
        Pass ``dict(os.environ)`` to opt in to the process environment explicitly, or
        ``{}`` for a keyless agent.
        """
        ...

    def configure_jsonl_audit(
        self,
        path: str,
        payload_policy: Literal["metadata_only", "full"] = ...,
        failure_mode: Literal["fail_closed", "best_effort"] = ...,
    ) -> None:
        """Persist structured audit records to an owner-only JSONL file.

        Args:
            path: Target file. Unsafe targets are rejected immediately.
            payload_policy: ``"metadata_only"`` (default) omits prompt/tool payloads;
                ``"full"`` records them.
            failure_mode: ``"fail_closed"`` (default) fails the run when an audit record
                cannot be written; ``"best_effort"`` continues.

        Raises:
            ValueError: Unknown policy or mode.
            RuntimeError: The audit file cannot be opened.
        """
        ...
    def use_memory_file(self, path: str, namespace: str = ...) -> None:
        """Back ``remember``/``recall`` with a crash-safe local JSON file.

        ``namespace`` (default ``"default"``) isolates tenants sharing one file. Concurrent
        coordination is process-local; use ``use_sqlite_memory`` across processes.

        Raises:
            ValueError: ``namespace`` is empty.
            RuntimeError: The file cannot be opened.
        """
        ...
    def use_session_file(self, path: str) -> None:
        """Persist subagent sessions to a JSON file (process-local compare-and-swap).

        Raises:
            ValueError: ``path`` is empty.
        """
        ...
    def use_sqlite_memory(self, path: str, namespace: str = ...) -> None:
        """Back ``remember``/``recall`` with a SQLite database under ``namespace``.

        Raises:
            ValueError: ``namespace`` is empty.
            RuntimeError: The database cannot be opened.
        """
        ...
    def use_sqlite_sessions(self, path: str) -> None:
        """Persist subagent sessions to a SQLite database."""
        ...
    def recover_expired_session(
        self,
        session_id: str,
        *,
        side_effects_reconciled: Literal[True],
    ) -> int:
        """Clear an expired lease after reconciliation; does not execute or resume work.

        Call this only after verifying the old worker stopped and reconciling any external
        side effects it may have completed. A later ``run_subagent`` / ``resume_subagent``
        call is a separate execution decision.

        Returns:
            The session's new revision.

        Raises:
            ValueError: ``side_effects_reconciled`` is not ``True``.
            AikitError: The store rejected the recovery (for example, the lease is live).
        """
        ...
    def register_web_tools(self, allowed_hosts: Sequence[str], search_endpoint: Optional[str] = ..., max_response_bytes: Optional[int] = ...) -> None:
        """Register ``WebFetch`` (and ``WebSearch`` when ``search_endpoint`` is set).

        Args:
            allowed_hosts: Exact HTTPS hosts the tools may reach; everything else is denied.
            search_endpoint: Optional allowlisted URL containing exactly one ``{query}``
                marker.
            max_response_bytes: Optional response cap (the default is 1,000,000 bytes).

        Raises:
            ValueError: Invalid endpoint, or a tool name is already registered.
        """
        ...
    def register_browser_tools(
        self,
        webdriver_endpoint: str,
        session_id: str,
        allowed_hosts: Sequence[str],
        *,
        external_egress_enforced: Literal[True],
    ) -> None:
        """Register browser tools after asserting an exact external host/public-IP boundary.

        ``external_egress_enforced=True`` is your assertion that network egress for the
        WebDriver session is enforced outside this process; aikit does not enforce it.

        Raises:
            ValueError: Invalid endpoint/hosts, or a tool name is already registered.
        """
        ...
    def register_mcp(self, server: McpConnection) -> None:
        """Expose a connected MCP server's (filtered) tools to this agent's runs.

        Raises:
            ValueError: One of the server's tool names is already registered.
        """
        ...
    def enable_capability_requests(self, gated_tools: Sequence[str]) -> None:
        """Gate already-registered tools behind a human-approved ``request_capability`` tool.

        The model must call ``request_capability``; the ``can_use_tool`` callback decides,
        and a gated tool is refused until granted. Grants last for one run only.

        Raises:
            ValueError: ``can_use_tool`` was not configured first, or a gated name is not a
                registered tool.
        """
        ...
    def enable_default_guardrails(self, blocked_input_patterns: Sequence[str] = ...) -> None:
        """Enable the default guardrail chain.

        Input is checked against ``blocked_input_patterns`` (regular expressions); output
        passes through the built-in secret and PII redactors.

        Raises:
            ValueError: A pattern is not a valid regular expression.
        """
        ...
    def add_key(self, key: str, provider: Optional[str] = ...) -> str:
        """Activate a provider credential and return the provider name it activated.

        Args:
            key: The API key. Its format usually identifies the provider.
            provider: Explicit provider name (``"anthropic"``, ``"openai"``,
                ``"google"``, ``"deepseek"``, ``"openrouter"``, ``"groq"``,
                ``"mistral"``, ``"xai"``). Required for ``sk-`` keys, which could be
                OpenAI or DeepSeek.

        Raises:
            ValueError: Empty, ambiguous, or unrecognized key, or unknown provider.

        Example::

            agent.add_key(os.environ["OPENAI_API_KEY"], provider="openai")
        """
        ...
    def active_providers(self) -> List[str]:
        """Return the names of providers that currently have a credential."""
        ...
    def has_provider(self, provider: str) -> bool:
        """Return whether ``provider`` currently has a credential."""
        ...
    def capabilities(self) -> AgentCapabilities:
        """Describe what this agent can do right now: providers (with fidelity) and tools."""
        ...
    def add_tool(
        self,
        name: str,
        description: str,
        input_schema: Mapping[str, JsonValue],
        callback: Callable[[Dict[str, JsonValue]], Awaitable[str]],
    ) -> None:
        """Register one host tool backed by a Python ``async def``.

        Args:
            name: Unique tool name advertised to the model.
            description: What the tool does, written for the model.
            input_schema: JSON Schema for the tool's input object.
            callback: ``async`` callable receiving the input dict and returning ``str``.

        Raises:
            ValueError: ``name`` is already registered or ``callback`` is not callable.
        """
        ...
    def add_tool_definition(self, definition: Tool) -> None:
        """Register a definition produced by ``aikit.tool(...)`` (or any ``Tool``)."""
        ...
    def register_builtin_tools(self, roots: Sequence[str]) -> None:
        """Register jailed Read/Write/Edit/Glob/Grep tools; Bash stays disabled.

        File tools are confined to ``roots`` and reject root escapes and symlinks.
        Registration fails closed on platforms without the descriptor-relative jail
        (currently Windows).

        Raises:
            ValueError: Invalid roots, or a built-in name collides with a host tool.
        """
        ...
    def enable_bash_with_required_containment(
        self, docker: Optional[DockerContainmentOptions] = ...
    ) -> None:
        """Add Bash under Required(Auto), optionally with a digest-pinned Docker fallback.

        Call ``register_builtin_tools`` first. There is no uncontained Bash mode in this
        binding; if no containment backend is available, Bash calls fail closed. Probe the
        result with ``builtin_containment_capabilities``.

        Raises:
            ValueError: Invalid Docker options.
        """
        ...
    async def builtin_containment_capabilities(self) -> ContainmentCapabilityReport:
        """Probe the required Bash backends without weakening containment.

        Raises:
            ValueError: ``enable_bash_with_required_containment`` has not been called.
        """
        ...
    def set_permissions(
        self,
        rules: Optional[Sequence[PermissionRule]] = ...,
        default_mode: Literal["allow", "deny", "ask"] = ...,
    ) -> None:
        """Replace the declarative tool permission policy.

        Args:
            rules: Rules such as ``{"id": "...", "effect": "ask", "tool":
                "lookup"}``; ``pattern`` is a regex matched against the input's string
                values (or one ``field``).
            default_mode: Effect for tools no rule matches (default ``"allow"``).

        ``"ask"`` decisions go to the ``can_use_tool`` callback; without one they fail
        closed (deny).

        Raises:
            ValueError: Invalid rule, effect, or regex.
        """
        ...
    def can_use_tool(
        self,
        callback: Callable[[ApprovalRequest], Awaitable[ApprovalResponse]],
    ) -> None:
        """Register the async approver for ``"ask"`` permission decisions.

        The callback receives an ``ApprovalRequest`` and returns ``True``/``False``,
        ``"allow"``/``"deny"``, or an exact ``{"action": "allow"|"deny", ...}`` object.
        Exceptions and malformed responses deny the tool call.

        Example::

            async def approve(request: "aikit.ApprovalRequest") -> "aikit.ApprovalResponse":
                # Quote stub-only types (or use `from __future__ import annotations`):
                # TypedDicts and aliases exist for type checkers, not at runtime.
                return "allow" if request["tool"] == "lookup" else "deny"

            agent.can_use_tool(approve)
        """
        ...
    def on_user_prompt(
        self,
        callback: Callable[[PromptContext], Awaitable[PromptHookResponse]],
    ) -> None:
        """Register an async UserPromptSubmit hook (continue, rewrite, or block).

        An exception or malformed response blocks the prompt.
        """
        ...
    def on_pre_tool_use(
        self,
        callback: Callable[[PreToolUseContext], Awaitable[PreToolHookResponse]],
        tool: Optional[str] = ...,
    ) -> None:
        """Register an async PreToolUse hook (continue, rewrite input, or block).

        ``tool`` restricts the hook to one tool name; omit it to match every tool.
        """
        ...
    def on_post_tool_use(
        self,
        callback: Callable[[PostToolUseContext], Awaitable[PostToolHookResponse]],
        tool: Optional[str] = ...,
    ) -> None:
        """Register an async PostToolUse hook (continue, rewrite output, or turn it into an error).

        ``tool`` restricts the hook to one tool name; omit it to match every tool.
        """
        ...
    def on_post_tool_failure(
        self,
        callback: Callable[[FailureContext], Awaitable[FailureHookResponse]],
        tool: Optional[str] = ...,
    ) -> None:
        """Register an async hook for tool-scoped failures.

        Runs in the dedicated PostToolFailure phase, before general ``on_failure`` hooks,
        and honors the same ``tool`` matcher as ``on_pre_tool_use``.
        """
        ...
    def on_failure(
        self,
        callback: Callable[[FailureContext], Awaitable[FailureHookResponse]],
    ) -> None:
        """Register an async hook for run failures (continue or rewrite the error text)."""
        ...
    def on_stop(
        self,
        callback: Callable[[StopContext], Awaitable[None]],
    ) -> None:
        """Register an async hook that observes every run's terminal stop."""
        ...

    async def generate_text(
        self,
        prompt: PromptInput,
        model: Optional[str] = ...,
        max_tokens: int = ...,
    ) -> GeneratedText:
        """Run the governed loop to completion and return the final text.

        Args:
            prompt: A string or a canonical ``Message`` history (text and media blocks).
            model: Model id; defaults to the keyless deterministic ``"mock-1"``.
            max_tokens: Output token cap per model call (default 1024).

        Returns:
            ``GeneratedText`` with ``text``, ``usage``, ``stop_reason``, ``messages``,
            ``provider_metadata``, and ``warnings``.

        Raises:
            AikitError: Provider, policy, budget, or tool failure (branch on ``code``).
            ValueError: Malformed ``prompt``.
        """
        ...

    def stream_text(
        self,
        prompt: PromptInput,
        model: Optional[str] = ...,
        max_tokens: int = ...,
    ) -> QueryStream:
        """Start a governed run and stream canonical deltas (defaults as ``generate_text``).

        Use ``run`` instead when you need budgets, retries, fallbacks, routing, or
        compaction.
        """
        ...

    def run(self, prompt: PromptInput, options: Optional[RunOptions] = ...) -> QueryStream:
        """Start a cancellable governed run with the full ``RunOptions`` surface.

        ``options`` is strict: unknown top-level or nested fields raise ``ValueError``, so a
        misspelled budget or retry limit is never silently ignored.

        Raises:
            AikitError: The run cannot start (for example ``provider_auth``).
            ValueError: Malformed ``prompt`` or ``options``.

        Example::

            async with agent.run(
                "Plan the migration",
                {"model": "mock-1", "max_turns": 4, "budget": {"max_total_tokens": 20_000}},
            ) as stream:
                async for delta in stream:
                    ...
        """
        ...
    def client(self) -> "Client":
        """Snapshot this configured agent into a reusable ``Client``."""
        ...

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
        compatibility_mode: CompatibilityMode = ...,
        validator: Optional[SemanticValidator] = ...,
    ) -> GeneratedObject[T]:
        """Generate one schema-validated object, repairing invalid attempts.

        Args:
            prompt: A string or a canonical ``Message`` history.
            schema: A JSON Schema mapping, or a Pydantic v2 model class (then
                ``result["value"]`` is a model instance).
            model: Model id; defaults to the keyless deterministic ``"mock-structured"``.
            max_retries: Repair attempts after the first (default 2; the core rejects
                more than 32).
            max_tokens: Output token cap per attempt (default 1024).
            name: Optional schema name forwarded to providers that use one.
            provider_options: Per-provider options, checked against the adapter catalog.
            compatibility_mode: ``"strict"`` (default) rejects unsupported options;
                ``"warn"`` / ``"best_effort"`` are explicit opt-ins that keep warnings.
            validator: Optional ``async`` business-rule check returning ``"accept"``,
                ``{"action": "retry", "reason": ...}``, or
                ``{"action": "reject", "reason": ...}``. Exceptions fail closed.

        Returns:
            ``GeneratedObject`` with ``value``, ``fidelity``, ``attempts``,
            ``provider_metadata``, and ``warnings``.

        Raises:
            AikitError: ``code == "structured_output"`` once retries are exhausted or the
                validator rejects; provider failures use their own codes.
            ValueError: Invalid schema, options, or prompt.

        Example::

            schema = {"type": "object", "properties": {"city": {"type": "string"}},
                      "required": ["city"], "additionalProperties": False}
            result = await agent.generate_object("Where is the Eiffel Tower?", schema)
            print(result["value"])
        """
        ...

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
        compatibility_mode: CompatibilityMode = ...,
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
        compatibility_mode: CompatibilityMode = ...,
        validator: Optional[SemanticValidator] = ...,
    ) -> ObjectStream[T]:
        """Stream structured-output attempts, deltas, repairs, and one final ``completed``.

        Takes the same arguments as ``generate_object``. Only the final
        ``completed["object"]["value"]`` is materialized as a Pydantic model; earlier
        events are left untouched.
        """
        ...

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
        compatibility_mode: CompatibilityMode = ...,
        validator: Optional[SemanticValidator] = ...,
    ) -> ObjectStream[JsonValue]: ...

    def remember(self, key: str, value: JsonValue) -> None:
        """Explicitly persist one JSON value; model output is never remembered automatically.

        Raises:
            RuntimeError: The memory store rejected the write.
        """
        ...
    def remember_cas(
        self,
        key: str,
        value: JsonValue,
        expected_revision: int,
        plane: Literal["working", "episodic", "semantic"] = ...,
        provenance: Optional[MemoryProvenance] = ...,
    ) -> int:
        """Write one memory entry only if its stored revision equals ``expected_revision``.

        This is the safe path for concurrent writers: a stale writer gets an error instead
        of silently winning.

        Returns:
            The entry's new revision.

        Raises:
            ValueError: Unknown ``plane`` or malformed ``provenance``.
            RuntimeError: Revision conflict or store failure.
        """
        ...
    def recall(self, query: str, limit: int = ...) -> List[MemoryEntry]:
        """Search explicit memories in this agent's namespace (default ``limit`` 10)."""
        ...
    def route(
        self, profiles: Sequence[ModelProfile], request: RouteRequest
    ) -> RouteDecision:
        """Deterministically pick a model profile for ``request`` from ``profiles``.

        Only providers with an active credential are eligible; credential values never
        enter the catalog or the result.

        Raises:
            ValueError: Malformed profiles or request.
        """
        ...

    async def run_subagent(
        self,
        spec: SubagentSpec,
        profiles: Sequence[ModelProfile],
        budget: Optional[BudgetLimits] = ...,
        max_parallelism: int = ...,
    ) -> SubagentResult:
        """Run one governed, budget-aware child agent (build ``spec`` with ``subtask``).

        The child inherits this agent's host tools, hooks, approvals, and permission
        policy, narrowed by its ``allowed_tools``. ``max_parallelism`` defaults to 4.
        """
        ...

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
    ) -> SubagentSpec:
        """Build the canonical ``SubagentSpec`` used by the subagent and council methods.

        Defaults: ``max_turns=16``, ``max_tokens=4096``, ``estimated_input_tokens=1024``.

        Raises:
            ValueError: Malformed ``route``.
        """
        ...

    async def resume_subagent(
        self,
        session_id: str,
        spec: SubagentSpec,
        profiles: Sequence[ModelProfile],
        budget: Optional[BudgetLimits] = ...,
        max_parallelism: int = ...,
    ) -> SubagentResult:
        """Resume a persisted child session through this agent's session store and CAS."""
        ...

    async def fan_out(
        self,
        specs: Sequence[SubagentSpec],
        profiles: Sequence[ModelProfile],
        budget: Optional[BudgetLimits] = ...,
        max_parallelism: int = ...,
    ) -> List[SubagentResult]:
        """Run independent children with bounded concurrency; results keep input order."""
        ...

    async def parallel(
        self,
        specs: Sequence[SubagentSpec],
        profiles: Sequence[ModelProfile],
        budget: Optional[BudgetLimits] = ...,
        max_parallelism: int = ...,
    ) -> List[SubagentResult]:
        """Alias for ``fan_out`` with identical concurrency, ordering, and governance."""
        ...

    async def council(
        self,
        members: Sequence[SubagentSpec],
        synthesizer: SubagentSpec,
        profiles: Sequence[ModelProfile],
        min_successes: int = ...,
        budget: Optional[BudgetLimits] = ...,
        max_parallelism: int = ...,
    ) -> CouncilResult:
        """Run ``members`` in parallel, then ``synthesizer`` once ``min_successes`` succeed."""
        ...

    def __repr__(self) -> str: ...


DurabilityMode = Literal["sync", "async", "exit"]
DurableRunStatus = Literal[
    "running", "paused", "reconcile_required", "completed", "failed", "cancelled"
]


class _DurableRunStateRequired(TypedDict):
    schema_version: int
    session_id: str
    run_id: str
    durability: DurabilityMode
    parent_run_id: Optional[str]
    policy_snapshot_hash: Optional[str]
    events: List[Dict[str, JsonValue]]
    checkpoints: Dict[str, JsonValue]
    projection: "DurableRunProjection"


class DurableRunState(_DurableRunStateRequired, total=False):
    governance_binding: GovernanceBinding


class Checkpoint(TypedDict):
    checkpoint_id: str
    run_id: str
    event_sequence: int
    parent_checkpoint_id: Optional[str]
    label: Optional[str]
    projection: "DurableRunProjection"


DurableApprovalKind = Literal[
    "confirmation", "missing_input", "output_review", "edit_retry"
]
DurableApprovalStatus = Literal["pending", "approved", "rejected"]


class _ApprovalResolutionRequired(TypedDict):
    approval_id: str
    approved: bool


class ApprovalResolution(_ApprovalResolutionRequired, total=False):
    response: JsonValue


class _DurableApprovalRequestRequired(TypedDict):
    logical_key: str
    kind: DurableApprovalKind
    prompt: str
    payload: JsonValue
    requested_at_unix_ms: int
    expires_at_unix_ms: int


class DurableApprovalRequest(_DurableApprovalRequestRequired, total=False):
    activity_id: str
    policy_snapshot_hash: str
    governance_binding: GovernanceBinding


class _DurableApprovalRequired(TypedDict):
    approval_id: str
    logical_key: str
    activity_id: Optional[str]
    kind: DurableApprovalKind
    prompt: str
    payload: JsonValue
    status: DurableApprovalStatus
    response: Optional[JsonValue]
    timed_out: bool
    requested_sequence: int
    resolved_sequence: Optional[int]


class DurableApproval(_DurableApprovalRequired, total=False):
    policy_snapshot_hash: str
    governance_binding: GovernanceBinding
    requested_at_unix_ms: int
    expires_at_unix_ms: int
    resolved_at_unix_ms: int


class DurableWorkerLease(TypedDict):
    owner_id: str
    lease_id: str
    acquired_at_unix_ms: int
    heartbeat_at_unix_ms: int
    expires_at_unix_ms: int


class _DurableRunProjectionRequired(TypedDict):
    branch_id: str
    status: DurableRunStatus
    state: JsonValue
    activities: Dict[str, JsonValue]
    approvals: Dict[str, DurableApproval]
    artifacts: Dict[str, List[JsonValue]]
    current_checkpoint_id: Optional[str]
    pause_reason: Optional[str]


class DurableRunProjection(_DurableRunProjectionRequired, total=False):
    worker_lease: DurableWorkerLease


class _ResumeCommandRequired(TypedDict):
    command: Literal["resume"]
    command_id: str

class ResumeCommand(_ResumeCommandRequired, total=False):
    approvals: List[ApprovalResolution]


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
    """Append-only, replay-validated durable run state with human-in-the-loop approvals.

    This is a local state machine: it persists nothing by itself. Store ``snapshot()``
    wherever you like and rebuild with ``DurableRun.from_state``; every imported snapshot
    is replay-validated against its event log. Each mutating call takes a caller-chosen id
    (``pause_id``, ``command_id``, ...) so retries are idempotent. Invalid transitions
    raise ``AikitError`` with ``code == "conflict"``; malformed input raises
    ``ValueError``.

    For example, pause for a confirmation and resume after a human answers::

        run = aikit.DurableRun("session-1", "run-1")
        approval_id = run.request_confirmation("deploy", "Deploy to production?")
        saved = run.snapshot()              # persist this
        run = aikit.DurableRun.from_state(saved)
        run.resolve_approval("cmd-1", approval_id, approved=True)
    """

    def __init__(
        self, session_id: str, run_id: str, durability: DurabilityMode = ...
    ) -> None:
        """Start a new run; ``durability`` defaults to ``"sync"``."""
        ...
    @staticmethod
    def from_state(state: DurableRunState) -> "DurableRun":
        """Rebuild a run from a ``snapshot()``; raises ``ValueError`` if replay disagrees."""
        ...
    @staticmethod
    def with_policy_snapshot(
        session_id: str,
        run_id: str,
        policy_snapshot: PolicySnapshot,
        durability: DurabilityMode = ...,
    ) -> "DurableRun":
        """Start a run with a ``seal_policy_snapshot`` result pinned before any other work."""
        ...
    @staticmethod
    def with_governance_binding(
        session_id: str,
        run_id: str,
        governance_binding: GovernanceBinding,
        durability: DurabilityMode = ...,
    ) -> "DurableRun":
        """Start a run with a ``seal_governance_binding`` result (tenant/agent/run) pinned."""
        ...
    @property
    def schema_version(self) -> int: ...
    @property
    def session_id(self) -> str: ...
    @property
    def run_id(self) -> str: ...
    @property
    def durability(self) -> DurabilityMode: ...
    @property
    def policy_snapshot_hash(self) -> Optional[str]: ...
    @property
    def governance_binding(self) -> Optional[GovernanceBinding]: ...
    @property
    def status(self) -> DurableRunStatus: ...
    def snapshot(self) -> DurableRunState:
        """Return the complete persistable state (event log plus projection)."""
        ...
    def replace_state(
        self, mutation_id: str, state: JsonValue
    ) -> DurableRunState:
        """Replace the application-defined state projection through an appended event."""
        ...
    def checkpoint(
        self, checkpoint_key: str, label: Optional[str] = ...
    ) -> Checkpoint:
        """Commit a full projection snapshot at the current event boundary."""
        ...
    def pause(self, pause_id: str, reason: str) -> None:
        """Pause the run with a recorded reason."""
        ...
    def request_approval(
        self,
        logical_key: str,
        prompt: str,
        payload: JsonValue,
        activity_id: Optional[str] = ...,
    ) -> str:
        """Persist an untyped approval request, pause the run, and return its approval id.

        The id is derived from the run, branch, and ``logical_key``. Legacy-compatible:
        the approval never expires; use ``request_typed_approval`` for deadlines.
        """
        ...
    def request_typed_approval(self, request: DurableApprovalRequest) -> str:
        """Persist a typed approval (kind, policy binding, deadline) and return its id.

        Resolve it with ``resolve_approval_at`` / ``apply_command_at`` and a trusted
        timestamp so an expired response cannot become valid after a restart.
        """
        ...
    def expire_approvals(self, expiration_id: str, now_unix_ms: int) -> List[str]:
        """Append timeout denials for typed approvals past their deadline at ``now_unix_ms``.

        Idempotent; returns the ids of the approvals it expired.
        """
        ...
    def request_confirmation(
        self,
        logical_key: str,
        prompt: str,
        details: JsonValue = ...,
        activity_id: Optional[str] = ...,
    ) -> str:
        """Persist a non-expiring yes/no confirmation, pause the run, and return its id."""
        ...
    def request_input(
        self,
        logical_key: str,
        prompt: str,
        input_schema: JsonValue = ...,
        activity_id: Optional[str] = ...,
    ) -> str:
        """Persist a non-expiring missing-input request (optional JSON Schema hint); returns its id."""
        ...
    def request_output_review(
        self,
        logical_key: str,
        prompt: str,
        output: JsonValue,
        activity_id: Optional[str] = ...,
    ) -> str:
        """Persist a non-expiring review of ``output`` (stored unmodified); returns its id."""
        ...
    def request_edit_retry(
        self,
        logical_key: str,
        prompt: str,
        output: JsonValue,
        error: Optional[str] = ...,
        activity_id: Optional[str] = ...,
    ) -> str:
        """Persist a non-expiring edit/retry decision for a rejected ``output``; returns its id."""
        ...
    def resolve_approval(
        self,
        command_id: str,
        approval_id: str,
        approved: bool,
        response: JsonValue = ...,
    ) -> DurableCommandResult:
        """Resolve exactly one pending approval through the canonical resume command.

        Typed approvals require ``resolve_approval_at`` instead.
        """
        ...
    def resolve_approval_at(
        self,
        command_id: str,
        approval_id: str,
        approved: bool,
        now_unix_ms: int,
        response: JsonValue = ...,
    ) -> DurableCommandResult:
        """Resolve one approval using an explicit trusted wall-clock value (Unix ms)."""
        ...
    def complete(self, completion_id: str) -> None:
        """Mark the run completed."""
        ...
    def fail(self, failure_id: str, error: str) -> None:
        """Mark the run failed with ``error``."""
        ...
    def apply_command(self, command: DurableCommand) -> DurableCommandResult:
        """Apply one resume, fork, rewind, or cancel command atomically."""
        ...
    def apply_command_at(
        self, command: DurableCommand, now_unix_ms: int
    ) -> DurableCommandResult:
        """Apply a command with the trusted clock that typed approval resolutions require."""
        ...


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
    """Reusable snapshot of a configured ``Agent`` (tools, policy, callbacks, credentials).

    The configuration is captured when the client is created, so finish configuring the
    agent first. Prefer ``agent.client()`` to construct one.
    """

    def __init__(self, agent: Agent) -> None: ...
    def query(self, prompt: PromptInput, options: Optional[RunOptions] = ...) -> QueryStream:
        """Start a governed run, exactly like ``Agent.run`` on the snapshotted agent."""
        ...


def query(
    prompt: PromptInput,
    tools: Optional[Sequence[Tool]] = ...,
    model: Optional[str] = ...,
    permissions: Optional[Sequence[PermissionRule]] = ...,
    options: Optional[RunOptions] = ...,
) -> QueryStream:
    """One-shot governed run without building an ``Agent`` first.

    Credentials come from this process's environment variables (as with ``Agent()``).
    ``model`` overrides ``options["model"]``; the default is the keyless ``"mock-1"``.
    Tools denied by ``permissions`` never execute; the model receives an error tool
    result instead. The default permission mode is ``"allow"``.

    Raises:
        AikitError: The run cannot start (for example ``provider_auth``).
        ValueError: Malformed prompt, permissions, or options.

    Example::

        async for delta in aikit.query("hello", tools=[lookup]):
            print(delta["type"])
    """
    ...


def evaluate_outcome(outcome: RunOutcome, gates: Sequence[EvalGate]) -> EvalVerdict:
    """Evaluate a recorded ``RunOutcome`` against deterministic gates, offline.

    Performs no provider, tool, filesystem, or network work. Unknown outcome or gate
    fields raise ``ValueError``. Text, tool, and turn gates need the runtime-recorded
    ``invocation_start_message_index``; legacy outcomes without it can still use the
    terminal-status and usage gates.

    Example::

        verdict = aikit.evaluate_outcome(stream.outcome(), [
            {"type": "terminal_status", "status": "completed"},
            {"type": "max_total_tokens", "value": 2_000},
        ])
        assert verdict["passed"], verdict["checks"]
    """
    ...


def evaluate_trace(suite: EvalSuite, trace: TraceInput) -> TraceEvalResult:
    """Check stream/durability trace assertions deterministically, offline.

    Raises:
        ValueError: Malformed ``suite`` or ``trace``.
    """
    ...


# --- BEGIN GENERATED external-agent declarations (scripts/generate-declarations.sh) ---
ExternalAgentKind = Literal["codex", "gemini", "claude", "grok"]
ExternalRuntimeProfile = Literal["external", "managed"]
ExternalUpdatePolicy = Literal["check_only", "vendor_managed", "aikit_managed"]
ExternalUpdateChannel = Literal["stable", "preview"]
ExternalUpdatePhase = Literal["idle", "checking", "available", "downloading", "staged", "activating", "current", "rolled_back", "failed"]
ExternalRefreshAction = Literal["rediscover", "refresh_model_catalog", "check_compatibility_manifest", "report_external_update", "stage_managed_update"]
ExternalRuntimeInstallOrigin = Literal["aikit_managed", "vendor_native", "homebrew", "npm", "application_bundle", "explicit_path", "unknown"]
ExternalRuntimeDiscoverySource = Literal["explicit", "path", "known_location"]


class UrlMediaInputSource(TypedDict):
    url: str
    kind: Literal["url"]


class Base64MediaInputSource(TypedDict):
    data: str
    kind: Literal["base64"]


class BytesMediaInputSource(TypedDict):
    data: List[int]
    kind: Literal["bytes"]


class ArtifactMediaInputSource(TypedDict):
    artifact_id: str
    kind: Literal["artifact"]


MediaInputSource = Union[
    UrlMediaInputSource,
    Base64MediaInputSource,
    BytesMediaInputSource,
    ArtifactMediaInputSource,
]


class _ExternalRuntimeCandidateRequired(TypedDict):
    kind: ExternalAgentKind
    executable: str
    real_executable: str
    origin: ExternalRuntimeInstallOrigin
    discovered_by: List[ExternalRuntimeDiscoverySource]


class ExternalRuntimeCandidate(_ExternalRuntimeCandidateRequired, total=False):
    aliases: List[str]


class ExternalRuntimeArtifact(TypedDict):
    target_os: str
    target_arch: str
    url: str
    sha256: str
    size_bytes: int
    executable_name: str


class _ExternalRuntimeReleaseRequired(TypedDict):
    kind: ExternalAgentKind
    channel: ExternalUpdateChannel
    version: str
    protocol: str
    contract_version: int
    artifacts: List[ExternalRuntimeArtifact]


class ExternalRuntimeRelease(_ExternalRuntimeReleaseRequired, total=False):
    minimum_aikit_version: str
    maximum_aikit_version: str


class _ExternalUpdateManifestRequired(TypedDict):
    schema_version: int
    generated_unix_ms: int
    expires_unix_ms: int
    releases: List[ExternalRuntimeRelease]


class ExternalUpdateManifest(_ExternalUpdateManifestRequired, total=False):
    minimum_aikit_version: str


class ExternalUpdateSelection(TypedDict):
    kind: ExternalAgentKind
    version: str
    channel: ExternalUpdateChannel
    protocol: str
    artifact: ExternalRuntimeArtifact


class ExternalUpdateSource(TypedDict):
    manifest_url: str
    signature_url: str
    key_id: str
    public_key_base64: str


class ExternalUpdateConfig(TypedDict):
    schema_version: int
    instance_id: str
    kind: ExternalAgentKind
    profile: ExternalRuntimeProfile
    policy: ExternalUpdatePolicy
    channel: ExternalUpdateChannel
    source: ExternalUpdateSource


class _ExternalUpdateStateSnapshotRequired(TypedDict):
    schema_version: int
    instance_id: str
    profile: ExternalRuntimeProfile
    policy: ExternalUpdatePolicy
    channel: ExternalUpdateChannel
    phase: ExternalUpdatePhase
    consecutive_failures: int


class ExternalUpdateStateSnapshot(_ExternalUpdateStateSnapshotRequired, total=False):
    current_version: str
    available_version: str
    quarantined_version: str
    last_manifest_check_unix_ms: int
    last_model_refresh_unix_ms: int
    last_discovery_unix_ms: int
    retry_after_unix_ms: int
    last_error: str


class Usage(TypedDict):
    input_tokens: int
    output_tokens: int
    cache_creation_input_tokens: int
    cache_read_input_tokens: int
    reasoning_tokens: int


class _ProviderWarningRequired(TypedDict):
    code: str
    message: str


class ProviderWarning(_ProviderWarningRequired, total=False):
    parameter: str
    provider: str
    model: str


class MediaInput(TypedDict):
    media_type: str
    source: MediaInputSource
    sha256: str
    size_bytes: int
# --- END GENERATED external-agent declarations ---


def discover_external_runtime_candidates(
    kind: str, explicit_path: Optional[str] = ...
) -> List[ExternalRuntimeCandidate]:
    """List local vendor CLI candidates for ``kind`` without executing anything.

    Args:
        kind: ``"codex"``, ``"gemini"``, ``"claude"``, or ``"grok"``.
        explicit_path: Optional exact executable to consider instead of searching.

    Raises:
        AikitError: Unknown ``kind`` or invalid path (code in the message prefix).
    """
    ...


def verify_external_update_manifest(
    manifest_bytes: bytes,
    signature_base64: str,
    key_id: str,
    trusted_keys: Dict[str, str],
    now_unix_ms: int,
) -> ExternalUpdateManifest:
    """Verify a detached Ed25519 signature over ``manifest_bytes``, then parse the manifest.

    ``trusted_keys`` maps key id to base64 public key; ``key_id`` selects the signer.
    Nothing is parsed before the signature verifies.

    Raises:
        AikitError: Bad signature, unknown or malformed key, or invalid/expired manifest.
    """
    ...


def select_external_runtime_update(
    manifest: ExternalUpdateManifest,
    kind: str,
    channel: ExternalUpdateChannel,
    aikit_version: str,
    target_os: str,
    target_arch: str,
    current_version: Optional[str] = ...,
) -> Optional[ExternalUpdateSelection]:
    """Pick the newest compatible release and artifact for one kind, channel, and target.

    Returns ``None`` when nothing newer than ``current_version`` is compatible.
    """
    ...


def validate_external_update_config(config: ExternalUpdateConfig) -> None:
    """Fail-closed check of a persisted signed-update configuration.

    Raises ``ValueError`` for a malformed document and ``AikitError`` for an invalid one.
    """
    ...


def external_update_refresh_actions(
    state: ExternalUpdateStateSnapshot, now_unix_ms: int
) -> List[ExternalRefreshAction]:
    """Return the maintenance actions due for ``state`` at the injected instant."""
    ...


def spawn_external_agent_session(
    kind: str,
    workspace: str,
    explicit_executable: Optional[str] = ...,
    instance_id: Optional[str] = ...,
    claude_bridge: Optional[str] = ...,
    claude_bridge_args: Optional[Sequence[str]] = ...,
    acknowledge_vendor_terms: bool = ...,
) -> Awaitable["ExternalAgentSession"]:
    """Discover and spawn one live vendor CLI runtime and return its session handle.

    Args:
        kind: ``"codex"``, ``"gemini"``, ``"claude"``, or ``"grok"``.
        workspace: Directory the session operates in.
        explicit_executable: Exact executable; otherwise the first discovered candidate.
        instance_id: Optional instance identity (defaults per kind).
        claude_bridge: Explicit Claude ACP bridge executable (``claude`` kind only).
        claude_bridge_args: Arguments for ``claude_bridge`` (``claude`` kind only).
        acknowledge_vendor_terms: Required ``True`` for ``gemini``, whose CLI OAuth reuse
            is governed by Google's account terms.

    Raises:
        AikitError: No executable found, terms not acknowledged, invalid options, or spawn
            failure (code in the message prefix).
    """
    ...


@final
class ExternalAgentSession:
    """One live vendor CLI session created by ``spawn_external_agent_session``.

    Every method except ``descriptor`` and ``events`` is awaitable. Call ``shutdown`` when
    finished; ``events()`` yields normalized runtime events and fails closed if the
    subscriber lags behind.
    """

    def __init__(self, _factory_only: NoReturn) -> None: ...
    def descriptor(self) -> Dict[str, Any]: ...
    def initialize(self) -> Awaitable[Dict[str, Any]]: ...
    def auth_status(self, refresh: bool = ...) -> Awaitable[Dict[str, Any]]: ...
    def list_models(self) -> Awaitable[Dict[str, Any]]: ...
    def new_session(self, request: Mapping[str, Any]) -> Awaitable[Dict[str, Any]]: ...
    def prompt(self, prompt: Mapping[str, Any]) -> Awaitable[None]: ...
    def cancel(self, session_id: str, turn_id: Optional[str] = ...) -> Awaitable[None]: ...
    def respond_approval(
        self, session_id: str, request_id: str, decision: Mapping[str, Any]
    ) -> Awaitable[None]: ...
    def respond_user_input(
        self, session_id: str, request_id: str, answers: Mapping[str, str]
    ) -> Awaitable[None]: ...
    def events(self) -> "ExternalAgentEventStream": ...
    def shutdown(self) -> Awaitable[None]: ...


@final
class ExternalAgentEventStream:
    """Async iterator of normalized runtime events; a lagged subscriber fails closed."""

    def __init__(self, _factory_only: NoReturn) -> None: ...
    def __aiter__(self) -> "ExternalAgentEventStream": ...
    def __anext__(self) -> Awaitable[Dict[str, Any]]: ...


@final
class ExternalUpdateState:
    """Typed managed-update state machine mirroring the Rust ``RuntimeUpdateState``.

    Every transition is fail-closed, every instant is injected by the caller, and a
    rejected transition leaves the state untouched. It performs no I/O itself.
    """

    def __init__(self, instance_id: str, profile: ExternalRuntimeProfile) -> None: ...
    @staticmethod
    def from_state(state: ExternalUpdateStateSnapshot) -> ExternalUpdateState: ...
    def snapshot(self) -> ExternalUpdateStateSnapshot: ...
    @property
    def phase(self) -> ExternalUpdatePhase: ...
    @property
    def current_version(self) -> Optional[str]: ...
    @property
    def available_version(self) -> Optional[str]: ...
    @property
    def quarantined_version(self) -> Optional[str]: ...
    @property
    def consecutive_failures(self) -> int: ...
    @property
    def retry_after_unix_ms(self) -> Optional[int]: ...
    @property
    def last_error(self) -> Optional[str]: ...
    def manifest_check_due(self, now_unix_ms: int) -> bool: ...
    def model_refresh_due(self, now_unix_ms: int) -> bool: ...
    def discovery_due(self, now_unix_ms: int) -> bool: ...
    def may_apply_binary_update(self) -> bool: ...
    def refresh_actions(self, now_unix_ms: int) -> List[ExternalRefreshAction]: ...
    def record_check_success(
        self, now_unix_ms: int, selection: Optional[ExternalUpdateSelection] = ...
    ) -> None: ...
    def record_failure(self, now_unix_ms: int, error: str) -> None: ...
    def record_discovery(self, now_unix_ms: int) -> None: ...
    def record_model_refresh(self, now_unix_ms: int) -> None: ...
    def begin_download(self) -> None: ...
    def record_staged(self) -> None: ...
    def begin_activation(self) -> None: ...
    def record_activated(self) -> None: ...
    def recover_activated(self, active_version: str) -> bool: ...
    def quarantine_available(self) -> None: ...
    def record_rollback(
        self, restored_version: str, rejected_version: Optional[str] = ...
    ) -> None: ...


__version__: str
