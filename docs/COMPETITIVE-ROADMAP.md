# aikit — "Take Everything" Roadmap (competitor-intelligence-driven)

*Synthesized 2026-07-16 from a GitHub scan of ~50 competitors across multi-provider SDKs, agent
frameworks, coding-agent harnesses, and governance/sandbox tooling. This is what to **take** from
the field, phase by phase — disciplined by what actually defends aikit's moat.*

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
> **"Claude-Code-grade governance, provider-neutral across 4 co-equal providers, from one
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

| # | Take | From | Why (gap / moat) | Effort |
|---|------|------|------------------|--------|
| 1.1 | **Content-safety guardrail pipeline** — a pluggable `Guardrail` stage on pre-model / pre-tool / post-model, with (a) built-in deterministic redactors (regex PII: emails, keys, cards, secrets), (b) a `Guardrail` trait, (c) an **interop adapter that runs Superagent `safety-agent` + Meta LlamaFirewall over our new MCP client** (injection/jailbreak detection + PII, keyless, self-hostable — don't build ML from scratch). | Guardrails AI, NeMo, LlamaFirewall, Superagent, Invariant, Lakera, Bedrock Guardrails | **Biggest content-security gap.** Injection/PII is table stakes for "production." Interop keeps us a governance *runtime*, not an ML vendor. | M–L |
| 1.2 | **Real default OS sandbox**: make Bash sandbox **default-on**; add **Linux Landlock + seccomp** and a **network-egress allowlist proxy** (deny egress by default, allowlist domains). We already have Seatbelt/Docker containment + process hardening. | Codex (default Seatbelt/Landlock/seccomp), Claude Agent SDK (Seatbelt + bubblewrap + UDS network proxy), Goose | **Codex made default OS sandbox table stakes.** Network egress control kills the `echo $KEY | curl` exfiltration path. This is exactly where MS toolkit/Agno/Pydantic *concede* — our win. | L |
| 1.3 | **Sandbox capability report + honest threat model per backend** (none / path-jail+hardening / Seatbelt / Landlock+seccomp / Docker / microVM), surfaced in `capabilities()`. | (our honesty constraint) | Never market a weak sandbox as containment; let callers pick their guarantee. | S |

---

## Phase 2 — Claude-Code-grade governance UX (so depth ≥ the incumbent)

*Goal: match or beat the Claude Agent SDK's governance ergonomics, using our existing engine +
capability broker as the base.*

| # | Take | From | Why | Effort |
|---|------|------|-----|--------|
| 2.1 | **Declarative policy config** — load allow/ask/deny rules from `aikit.policy.yaml` / settings, with **glob/arg patterns** (`Bash(rm *)`, `Write(./secrets/**)`, `Edit(**/*.env)`), deny-wins precedence. We have the engine; add config + patterns. | Claude Agent SDK settings.json, opencode rulesets, Continue `permissions.yaml`, MS toolkit YAML | Declarative config is how every serious harness ships rules. Ours is code-only today. | M |
| 2.2 | **Plan mode** — the agent proposes a step plan; a human approves / comments / edits / rejects **before** execution. Builds on our capability-request broker. | grok-build plan mode, Claude Agent SDK `plan` mode | The proven HITL pattern; extends our "agent requests → human decides" primitive to whole plans. | M |
| 2.3 | **Risk-scoring + LLM SmartApprove** — annotate each tool call LOW/MED/HIGH/UNKNOWN; an optional `LlmApprover` auto-approves low-risk, escalates high-risk to a human. | OpenHands security analyzer, Goose SmartApprove ("PermissionJudge") | Cuts approval fatigue (Anthropic cites ~84% fewer prompts) without losing control. Composes with our `ToolApprover`. | M |
| 2.4 | **Reliability rules (declarative, distinct from security)** — tool-ordering / prerequisites / max-uses (`Forbidden`, `only_after`, `force_at_step`, caps) for *predictable* tool use. | BeeAI/IBM `ConditionalRequirement`, Letta Tool Rules, OpenAI Agents guardrail tripwires | Separates "is it *safe*" (permissions) from "is it *sensible*" (reliability) — reduces agent flailing. | M |
| 2.5 | **Off-prompt tool output** — option to keep large/sensitive tool results out of the model context (store + reference). | Griptape "off-prompt by default" | Data-privacy + context-budget win; complements compaction. | S |

---

## Phase 3 — Depth, isolation, scale

| # | Take | From | Why | Effort |
|---|------|------|-----|--------|
| 3.1 | **microVM containment backend** — add **microsandbox** (Rust, libkrun, local-first, embeddable) as an isolation backend; keep Docker + Seatbelt/Landlock as tiers. | microsandbox (Rust, closest to our shape), E2B, Daytona (OSS retiring), Letta | VM-grade isolation for untrusted code, *local-first* (not cloud). microsandbox's Rust+embeddable shape fits our core. | L |
| 3.2 | **Durable / resumable runs** — checkpoint run state; resume/rewind after crash or interrupt. We have sessions + run recording; add durable checkpoints. | LangGraph `interrupt()`+checkpointer (best-in-class), Julep/Temporal, Inngest, Dapr, Cloudflare DO | Long-running production agents must survive crashes and pause/resume. LangGraph's is the bar. | L |
| 3.3 | **Model-summarizer compaction + memory-flush** — upgrade our extractive compaction to a cheap-model summary at ~85% context, flushing key facts to memory first. | grok-build compaction (`two_pass`, memory-flush) | Our v1 compaction drops-with-a-note; summarizing preserves information. Memory already exists. | M |
| 3.4 | **Worktree-isolated parallel subagents** — git-worktree isolation for fan-out so parallel agents don't collide. We have orchestration/subagents; add worktree isolation. | grok-build worktree subagents | Safe parallel edits; matches how we already run multi-agent work. | M |
| 3.5 | **Generic OpenAI-compatible adapter** — one honest generic adapter for the long tail (Ollama, OpenRouter, Groq, xAI, Mistral, Together, llama.cpp) graded lower fidelity. Keep 4 native + 1 generic. | Rig (20+), LiteLLM (100+), Cline, opencode (75+) | Provider *breadth* without diluting native fidelity. Local models (Ollama) = local-first story. | M |

---

## Phase 4 — Ecosystem & interop (reach without dilution)

| # | Take | From | Why | Effort |
|---|------|------|-----|--------|
| 4.1 | **Agent Client Protocol (ACP) surface** — expose aikit over ACP so editors (Zed, etc.) can drive it. | grok-build (ACP), Zed | Editor embedding = distribution, without us building a UI. | M |
| 4.2 | **A2A (Agent-to-Agent) protocol** — standard multi-agent interop + agent identity/trust for delegation. | Strands, BeeAI (LF), Google A2A, MS toolkit (identity) | Interop with the broader agent ecosystem; identity underpins safe delegation. | M–L |
| 4.3 | **Skills / plugins loader** — load skills/plugins/hooks from a directory (`AGENTS.md`-style), sandboxed + governed. | grok-build, Claude Agent SDK, opencode plugins | Extensibility without a marketplace-as-product; everything still governed. | M |
| 4.4 | **Cedar/OPA policy import** — let the permission engine ingest standard Cedar/OPA policies. | MS toolkit (Cedar/OPA), Permit.io | Enterprise policy interop; standards credibility. | M |
| 4.5 | **MCP: resources + prompts + Streamable-HTTP transport** (client already extended with these) → add an **MCP *server*** surface so aikit tools are consumable by other agents. | MCP ecosystem | Two-way MCP = full ecosystem citizenship. | M |
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
P1  Complete the conjunction     ──►  guardrails (1.1) + real OS sandbox (1.2)   [credibility gate]
P2  Governance depth ≥ Claude    ──►  policy config, plan mode, SmartApprove, reliability rules
P3  Depth / isolation / scale    ──►  microVM, durable resume, model-compaction, worktrees, generic adapter
P4  Ecosystem / interop          ──►  ACP, A2A+identity, plugins, Cedar/OPA, MCP-server, observability
P5  Cross-language + conformance ──►  CONTINUOUS gate on P1–P4 (the actual moat)
```

- **Do P1 first.** It closes the two holes that make "production governance" ring hollow, and 1.2 lands
  squarely where the strongest governance rivals (MS toolkit, Agno, Pydantic AI) are weak.
- **P2 makes the governance deep enough to survive the Claude-Agent-SDK comparison** (the reference design).
- **P3/P4 widen the wedge**; sequence by demand.
- **P5 is the moat and runs through all of it.** A feature that ships to Rust only is half-done.

**Highest-leverage single move:** 1.1 + 1.2 together — they complete the *governance* pillar exactly where
incumbents are weak, and every bit ships across three languages (5.1), which is the thing none of them can
copy. That intersection — deep local governance × provider-neutral × one-core-three-bindings — is the only
ground aikit can hold.
