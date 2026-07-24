# aikit — "Take Everything" Roadmap (competitor-intelligence-driven)

*Synthesized 2026-07-16 from a GitHub scan of ~50 competitors across multi-provider SDKs, agent
frameworks, coding-agent harnesses, and governance/sandbox tooling; reconciled with the source tree
on 2026-07-24. This is what to **take** from the field, phase by phase — disciplined by what
actually defends aikit's moat.*

> **Status note (2026-07-24):** This is a historical strategy document, not the current completion
> dashboard. Durable runs, PostgreSQL/Temporal reference layers, eight provider adapters,
> multimodal contracts, governed protocol mappings, scoped governance, egress brokering, trace
> evals, and redacted telemetry have landed since the original scan. Use
> [`PARITY-MATRIX.md`](PARITY-MATRIX.md) for row-level current evidence and open gates.

> **2026-07-20 addendum:** three cross-cutting capabilities have also landed since the original
> roadmap: deterministic keyless evaluation gates inspired by Pydantic Evals/Mastra, async semantic
> structured-output validation, and exact MCP visibility filters plus bounded discovery/transport.
> All three have Rust, Python, and Node contract tests. Durable checkpoint/fork/rewind now uses an
> append-only event model and explicit reconciliation for ambiguous external effects; process-level
> chaos and real distributed-engine acceptance remain open gates.

> **2026-07-24 addendum:** the Sync in-process durable driver, bounded MCP server, and experimental
> A2A JSON-RPC/SSE listener have landed. A2A now includes artifact/direct-Message projection,
> protected cancellation ingress, and pinned official TCK evidence. Distributed workers,
> production A2A journal wiring, complete timestamps/history/artifact updates, and ACP wire
> integration remain open.

## What the research changed (read this first)

Three findings must reshape the plan and the pitch:

1. **The moat is thin on every single axis, but real as a *conjunction*.**
   - "Enforcing hooks that BLOCK" is **not** a differentiator — Semantic Kernel filters, MS Agent
     Framework middleware, LangChain v1 middleware, CrewAI, Google ADK, Agno, Mastra TripWire, Pydantic
     AI, Letta all block tool calls today. **Stop marketing "we enforce, they observe."**
   - "Multi-provider" is **not** a differentiator — Agno (20+ co-equal), Pydantic AI, Semantic Kernel,
     LangChain all do it. Our "4 co-equal incl. native DeepSeek" is only sharp vs the OpenAI-normalizers.
   - The governance *concept* is the **Claude Agent SDK / Claude-Code model** — we're porting a proven
     design, not inventing one. And the "nobody bundles governance" line is dead (MS Agent Governance
     Toolkit, Claude Agent SDK, Goose all bundle a version).

2. **The ONE genuinely unmatched pillar: a single Rust core with byte-identical, conformance-tested
   Python/TS/Rust bindings.** Every multi-language rival ships **separate reimplementations that drift**
   (LangChain Py/JS, OpenAI Py/JS, AutoGen Py/.NET, Semantic Kernel C#/Py/Java, ADK Py/Go/Java, MS
   toolkit per-language); Rig is Rust-only; Motia is a multi-runtime event bus; Letta is REST. **This is
   the pillar nobody can cheaply copy — so it is a gate on every item below, not a final phase.**

3. **Two credibility gaps keep "production governance" incomplete** vs Codex/Claude/OpenHands/Agno:
   **content-safety guardrails** (prompt-injection + PII) and a **real default OS sandbox** (Codex made
   default Seatbelt/Landlock table stakes; MS toolkit + Agno + Pydantic AI *concede* OS sandbox — exactly
   where we can win). Close these first or the conjunction has a hole.

### The positioning that survives scrutiny
> **"Claude-Code-grade governance, provider-neutral across 4 native providers plus isolated
> compatible endpoints, from one
> conformance-tested Rust core with Python / TypeScript / Rust bindings."**
> Not "another agent framework with hooks and multi-provider."

### Non-negotiable gates on every phase
- **Cross-language or it doesn't count.** Every new capability ships to Py **and** TS **and** Rust and
  enters the conformance suite in the same increment. (This is the moat — protect it obsessively.)
- **Stay a library.** Refuse to become a TUI, no-code builder, marketplace, hosted proxy, or daemon
  (see "What NOT to take"). Interop with those; don't reimplement them.
- **Governance depth ≥ Claude Agent SDK**, or the governance claim loses to the incumbent it apes.

---

## Phase 1 — Complete the governance conjunction (credibility-critical)

*Goal: make "Claude-Code-grade governance, provider-neutral" literally true and deep. Without these
two, a reviewer closes the tab.*

| # | Take | From | Why (gap / moat) | Status |
|---|------|------|------------------|--------|
| 1.1 | **Content-safety guardrail pipeline** — pluggable `Guardrail` stage with deterministic secret/PII redactors, regex blocklist, and fail-closed MCP safety-server interop. | Guardrails AI, NeMo, LlamaFirewall, Superagent, Invariant, Lakera, Bedrock Guardrails | **Biggest content-security gap.** Injection/PII is table stakes for "production." Interop keeps us a governance *runtime*, not an ML vendor. | **Mostly done** — deterministic + MCP path ship; deeper ML interop is optional host config |
| 1.2 | **Real default OS sandbox**: Bash **Required(Auto)** by default; Seatbelt / Linux ns+seccomp / Windows Job / digest-pinned Docker. Remaining: Landlock refinements and a network-egress allowlist proxy beyond Docker `--network=none`. | Codex (default Seatbelt/Landlock/seccomp), Claude Agent SDK (Seatbelt + bubblewrap + UDS network proxy), Goose | **Codex made default OS sandbox table stakes.** Network egress control kills the `echo $KEY \| curl` exfiltration path. | **Core done**; Landlock/egress-proxy polish remaining |
| 1.3 | **Sandbox capability report + honest threat model per backend**, surfaced in containment capabilities. | (our honesty constraint) | Never market a weak sandbox as containment; let callers pick their guarantee. | **Done** — `THREAT-MODEL.md` + capability report |

---

## Phase 2 — Claude-Code-grade governance UX (so depth ≥ the incumbent)

*Goal: match or beat the Claude Agent SDK's governance ergonomics, using our existing engine +
capability broker as the base.*

| # | Take | From | Why | Status |
|---|------|------|-----|--------|
| 2.1 | **Declarative policy config** — load allow/ask/deny rules from JSON, with **glob/arg patterns** (`Bash(rm *)`, `Write(./secrets/**)`, `Edit(**/*.env)`), deny-wins precedence. | Claude Agent SDK settings.json, opencode rulesets, Continue `permissions.yaml`, MS toolkit YAML | Declarative config is how every serious harness ships rules. | **Done** — `PolicySpec` + `examples/policy.rs` (YAML convenience remaining optional) |
| 2.2 | **Plan mode** — the agent proposes a step plan; a human approves / comments / edits / rejects **before** execution. Builds on our capability-request broker. | grok-build plan mode, Claude Agent SDK `plan` mode | The proven HITL pattern; extends our "agent requests → human decides" primitive to whole plans. | **Done** — `governance/plan.rs` + `examples/plan_mode.rs` |
| 2.3 | **Risk-scoring + SmartApprove** — annotate each tool call LOW/MED/HIGH; auto-approve low-risk, escalate the rest to a human. Optional LLM judge remains host-pluggable. | OpenHands security analyzer, Goose SmartApprove ("PermissionJudge") | Cuts approval fatigue without losing control. Composes with our `ToolApprover`. | **Done (heuristic)** — `HeuristicRiskScorer` + `SmartApprover`; built-in LLM judge deferred |
| 2.4 | **Reliability rules (declarative, distinct from security)** — tool-ordering / prerequisites / max-uses (`Forbidden`, `only_after`, caps) for *predictable* tool use. | BeeAI/IBM `ConditionalRequirement`, Letta Tool Rules, OpenAI Agents guardrail tripwires | Separates "is it *safe*" (permissions) from "is it *sensible*" (reliability) — reduces agent flailing. | **Done** — `ReliabilityPolicy` + `examples/reliability.rs` |
| 2.5 | **Off-prompt tool output** — option to keep large/sensitive tool results out of the model context (store + reference). | Griptape "off-prompt by default" | Data-privacy + context-budget win; complements compaction. | **Done** — `OffPromptExecutor` / `OffPromptStore` |

---

## Phase 3 — Depth, isolation, scale

| # | Take | From | Why | Effort |
|---|------|------|-----|--------|
| 3.1 | **microVM containment backend** — add **microsandbox** (Rust, libkrun, local-first, embeddable) as an isolation backend; keep Docker + Seatbelt/Landlock as tiers. | microsandbox (Rust, closest to our shape), E2B, Daytona (OSS retiring), Letta | VM-grade isolation for untrusted code, *local-first* (not cloud). microsandbox's Rust+embeddable shape fits our core. | L |
| 3.2 | **Durable / resumable runs** — checkpoint run state; resume/rewind after crash or interrupt. We have sessions + run recording; add durable checkpoints. | LangGraph `interrupt()`+checkpointer (best-in-class), Julep/Temporal, Inngest, Dapr, Cloudflare DO | Long-running production agents must survive crashes and pause/resume. LangGraph's is the bar. | L |
| 3.3 | **Model-summarizer compaction + memory-flush** — upgrade our extractive compaction to a cheap-model summary at ~85% context, flushing key facts to memory first. | grok-build compaction (`two_pass`, memory-flush) | Our current 0.2 compaction drops-with-a-note; summarizing preserves information. Memory already exists. | M |
| 3.4 | **Worktree-isolated parallel subagents** — git-worktree isolation for fan-out so parallel agents don't collide. We have orchestration/subagents; add worktree isolation. | grok-build worktree subagents | Safe parallel edits; matches how we already run multi-agent work. | M |
| 3.5 | **Broaden the compatible adapter** — OpenRouter, Groq, xAI, and Mistral already ship as isolated lower-fidelity endpoints; add configurable Ollama, Together, and llama.cpp support without weakening the four native adapters. | Rig (20+), LiteLLM (100+), Cline, opencode (75+) | Provider *breadth* without diluting native fidelity. Local models (Ollama/llama.cpp) strengthen the local-first story. | S–M |

### Cross-cutting work already landed after the original scan

| Capability | Borrowed pattern | Aikit-specific boundary |
|---|---|---|
| Deterministic eval datasets and gates | Pydantic Evals; Mastra gates/verdicts | No implicit LLM judge; current-invocation transcript boundary; redacted reports; explicit live budgets. |
| Semantic structured-output validation | PydanticAI output validators | Schema first, semantic callback second; shared bounded repair; fail-closed callback errors; three-language projection. |
| MCP tool visibility and transport limits | OpenAI Agents SDK per-server filtering; MCP protocol | Filter before cache and again before execution; deny wins; bounded pages/items/bytes/cursors/responses. |

---

## Phase 4 — Ecosystem & interop (reach without dilution)

| # | Take | From | Why | Effort |
|---|------|------|-----|--------|
| 4.1 | **Agent Client Protocol (ACP) surface** — expose aikit over ACP so editors (Zed, etc.) can drive it. | grok-build (ACP), Zed | Editor embedding = distribution, without us building a UI. | M |
| 4.2 | **A2A (Agent-to-Agent) protocol** — standard multi-agent interop + agent identity/trust for delegation. | Strands, BeeAI (LF), Google A2A, MS toolkit (identity) | **Partial in tree:** governed JSON-RPC/SSE and official TCK evidence exist; production journal/deployment gaps remain. | M–L |
| 4.3 | **Skills / plugins loader** — load skills/plugins/hooks from a directory (`AGENTS.md`-style), sandboxed + governed. | grok-build, Claude Agent SDK, opencode plugins | Extensibility without a marketplace-as-product; everything still governed. | M |
| 4.4 | **Cedar/OPA policy import** — let the permission engine ingest standard Cedar/OPA policies. | MS toolkit (Cedar/OPA), Permit.io | Enterprise policy interop; standards credibility. | M |
| 4.5 | **MCP: resources + prompts + Streamable-HTTP transport** (client already extended with these) → add an **MCP *server*** surface so aikit tools are consumable by other agents. | MCP ecosystem | **Landed locally:** external SDK/OAuth conformance remains. | M |
| 4.6 | **Richer trace/replay + OTel spans everywhere** — session replay of every request/tool/permission decision. We have an OTel bridge + audit; deepen it. | Langfuse, AgentOps, VoltAgent console, OpenLLMetry | Observability is table stakes for production adoption. | M |

---

## Phase 5 — The moat pillar (CONTINUOUS, not last)

**This is not a phase you do at the end — it is a gate on every item in Phases 1–4.**

| # | Take | From | Why | Effort |
|---|------|------|-----|--------|
| 5.1 | **Every new surface → Python + TypeScript + Rust, byte-identical, in the same increment.** | nobody (this is the moat) | The one pillar rivals can't cheaply copy. If bindings drift, aikit collapses into "another framework." | (ongoing) |
| 5.2 | **Grow the conformance suite** to cover every new capability (guardrails, plan mode, policy config, sandbox tier, MCP, compaction…) with byte-identical cross-language transcripts. | (our own parity harness) | Proves "no-drift shared core" — the differentiator — mechanically, every commit. | (ongoing) |
| 5.3 | **Keep reasoning-replay lossless + fidelity grades honest** as a standing correctness proof-point (rivals have documented replay bugs). | our reasoning module vs LiteLLM #27946 / Vercel #11602 / OpenAI-JS #770 | A rigor edge that's real and underserved; don't regress it. | (ongoing) |

---

## What NOT to take (protect the focus)

Taking "everything" means every **capability** relevant to a governed, provider-neutral, cross-language
**library** — not every product shape. Explicitly refuse (interop instead):

- **A TUI / desktop app** (grok-build, Goose Desktop, Aider, Gemini CLI) — aikit is embeddable; a TUI is a
  separate optional shell later, never the core.
- **A no-code / visual builder or marketplace-as-product** (AutoGPT, Rivet) — out of scope; we're a library.
- **A hosted proxy / gateway / SaaS** (LiteLLM proxy, Portkey, Helicone, OpenRouter, Lakera, Cloudflare) —
  we run in-process; interop, don't reimplement.
- **A daemon / client-server runtime** (Goose `goosed`, Letta REST, Plandex) — the in-process library *is*
  the differentiator vs these; don't add a socket boundary.
- **Building ML guard models from scratch** — interop with LlamaFirewall / Superagent over MCP instead.
- **Cloud-only microVM infra** (E2B/Daytona/Modal SaaS) — prefer the local-first embeddable microsandbox.

---

## Sequencing & rationale

```
P1  Complete the conjunction     ──►  guardrails + real OS sandbox   [largely landed]
P2  Governance depth ≥ Claude    ──►  policy, plan mode, SmartApprove, reliability, off-prompt  [landed in core]
P3  Depth / isolation / scale    ──►  durable core landed; distributed worker, microVM, model summaries remain
P4  Ecosystem / interop          ──►  MCP server landed; A2A partial; ACP wire and production proof remain
P5  Cross-language + conformance ──►  CONTINUOUS gate on P1–P4 (the actual moat) — project remaining Rust-only P2 helpers to Py/TS next
```

- **P1 core is in tree** (guardrails, required containment, honest capability report). Residual work is
  Landlock/egress-proxy polish, not greenfield.
- **P2 core is in tree** (policy config, plan mode, heuristic SmartApprove, reliability, off-prompt).
  Next obligation under P5: typed Python/TypeScript projections for each new primitive.
- **P3/P4 widen the wedge**; sequence by demand.
- **P5 is the moat and runs through all of it.** A feature that ships to Rust only is half-done.

**Highest-leverage direction:** close the remaining containment/egress gaps while keeping every new
public contract in the one-core/three-SDK conformance loop. The defensible intersection is deep
local governance × provider-native fidelity × deterministic proof, not any one checklist feature.
