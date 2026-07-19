"use strict";

// Thin wrapper over the napi-built native addon. `cargo build -p aikit-node` produces the cdylib
// and `scripts/build-node.sh` copies it here as `aikit_node.node`; requiring it runs napi's module
// init, which registers `Agent`, `query`, and `QueryStream`.
//
// The only ergonomics we add on top of the raw addon: make the streaming `QueryStream` (which
// exposes an async `next()` returning the next delta or `null`) idiomatic to consume with
// `for await`. Generation, memory, routing, orchestration, and governance stay in Rust.

const fs = require("node:fs");
const path = require("node:path");

function nativePackageName() {
  const key = `${process.platform}-${process.arch}`;
  const packages = {
    "darwin-arm64": "aikit-runtime-darwin-arm64",
    "darwin-x64": "aikit-runtime-darwin-x64",
    "linux-arm64": "aikit-runtime-linux-arm64-gnu",
    "linux-x64": "aikit-runtime-linux-x64-gnu",
    "win32-x64": "aikit-runtime-win32-x64-msvc",
  };
  const selected = packages[key];
  if (selected == null) {
    throw new Error(
      `aikit-runtime does not publish a native addon for ${process.platform}/${process.arch}`,
    );
  }
  if (process.platform === "linux") {
    const header = process.report?.getReport?.().header;
    const glibc = header?.glibcVersionRuntime;
    if (glibc == null) {
      throw new Error(
        "aikit-runtime packaged Linux addons require glibc; musl is not yet supported",
      );
    }
    const match = /^(\d+)\.(\d+)/.exec(glibc);
    if (
      match == null ||
      Number(match[1]) < 2 ||
      (Number(match[1]) === 2 && Number(match[2]) < 28)
    ) {
      throw new Error(
        `aikit-runtime packaged Linux addons require glibc 2.28 or newer; found ${glibc}`,
      );
    }
  }
  return selected;
}

function loadNative() {
  // Local builds intentionally remain simple: scripts/build-node.sh stages the addon beside this
  // wrapper. Packaged installs omit that file and resolve the exact optional platform package.
  const local = path.join(__dirname, "aikit_node.node");
  if (fs.existsSync(local)) return require(local);

  const packageName = nativePackageName();
  try {
    return require(packageName);
  } catch (cause) {
    const error = new Error(
      `aikit-runtime could not load ${packageName}; optional dependencies may have been omitted ` +
        "during installation",
    );
    error.cause = cause;
    throw error;
  }
}

const native = loadNative();
const TYPED_ERROR_MARKER = "__AIKIT_TYPED_ERROR__";

function normalizeNativeError(error) {
  const message = typeof error?.message === "string" ? error.message : "";
  const marker = message.indexOf(TYPED_ERROR_MARKER);
  if (marker < 0) return error;
  try {
    const payload = JSON.parse(message.slice(marker + TYPED_ERROR_MARKER.length).trim());
    const normalized = new Error(payload.message ?? payload.info?.message ?? "aikit error");
    normalized.name = "AikitError";
    normalized.info = payload.info;
    normalized.code = payload.info?.code;
    normalized.cause = error;
    return normalized;
  } catch (_parseError) {
    return error;
  }
}

/** Make a native pull stream (`next()` → value | null) consumable via `for await`. */
function asyncIterable(stream, transform, signal) {
  const nativeNext = stream.next.bind(stream);
  const nativeCancel =
    typeof stream.cancel === "function" ? stream.cancel.bind(stream) : null;
  const nativeClose =
    typeof stream.close === "function" ? stream.close.bind(stream) : null;
  let inFlightNext;
  let closePromise;
  let abortListener;
  const removeAbortListener = () => {
    if (abortListener != null) signal?.removeEventListener("abort", abortListener);
    abortListener = undefined;
  };
  const close = () => {
    if (closePromise == null) {
      // Cancellation must happen before waiting for an outstanding pull: that pull may be blocked
      // inside a host hook and native close is otherwise the only operation that wakes it.
      nativeCancel?.();
      // Native QueryStream is deliberately single-consumer. If cancellation or an explicit
      // close races a pending next(), let that pull observe cancellation before taking the native
      // stream for finalization.
      const pending = inFlightNext;
      closePromise = Promise.resolve(pending)
        .catch(() => {})
        .then(() => nativeClose?.())
        .catch((error) => {
          throw normalizeNativeError(error);
        })
        .finally(removeAbortListener);
    }
    return closePromise;
  };
  stream.next = function () {
    if (closePromise != null) {
      return closePromise.then(() => null);
    }
    if (signal?.aborted) {
      return close().then(() => null);
    }
    const operation = (async () => {
      try {
        const value = await nativeNext();
        return value == null || typeof transform !== "function" ? value : transform(value);
      } catch (error) {
        throw normalizeNativeError(error);
      }
    })();
    inFlightNext = operation;
    return operation.finally(() => {
      if (inFlightNext === operation) inFlightNext = undefined;
    });
  };
  if (nativeClose != null) stream.close = close;
  if (signal != null) {
    if (typeof signal.addEventListener !== "function") {
      throw new TypeError("options.signal must be an AbortSignal");
    }
    abortListener = () => {
      void close().catch(() => {});
    };
    if (signal.aborted) abortListener();
    else signal.addEventListener("abort", abortListener, { once: true });
  }
  stream[Symbol.asyncIterator] = function () {
    return {
      next: async () => {
        const value = await stream.next();
        if (value != null) return { done: false, value };
        if (nativeClose != null) await close();
        else removeAbortListener();
        return { done: true, value: undefined };
      },
      // JavaScript calls `return()` on an async iterator when `for await` exits via `break`.
      // Waiting here makes early loop exit deterministically run Stop/audit/session finalizers.
      return: async () => {
        if (nativeClose != null) await close();
        else removeAbortListener();
        return { done: true, value: undefined };
      },
      throw: async (error) => {
        if (nativeClose != null) await close();
        else removeAbortListener();
        throw error;
      },
    };
  };
  return stream;
}

const RUN_OPTION_KEYS = new Set([
  "model",
  "fallbackModels",
  "maxTokens",
  "maxTurns",
  "providerOptions",
  "budget",
  "retry",
  "routing",
  "compaction",
  "signal",
]);
const QUERY_OPTION_KEYS = new Set([
  ...RUN_OPTION_KEYS,
  "permissions",
  "defaultMode",
]);
const PERMISSION_RULE_KEYS = new Set([
  "id",
  "effect",
  "tool",
  "pattern",
  "field",
]);

function checkedPermissionRules(rules) {
  if (rules == null) return rules;
  if (!Array.isArray(rules)) {
    throw new TypeError("permissions must be an array");
  }
  for (const [index, rule] of rules.entries()) {
    if (rule == null || typeof rule !== "object" || Array.isArray(rule)) {
      throw new TypeError(`permissions[${index}] must be an object`);
    }
    const unknown = Object.keys(rule).find((key) => !PERMISSION_RULE_KEYS.has(key));
    if (unknown != null) {
      throw new TypeError(`permissions[${index}] contains unknown field '${unknown}'`);
    }
    if (rule.field != null && rule.pattern == null) {
      throw new TypeError(`permissions[${index}].field requires pattern`);
    }
  }
  return rules;
}

function checkedOptionObject(options, context, allowedKeys) {
  if (options == null) return options;
  if (typeof options !== "object" || Array.isArray(options)) {
    throw new TypeError(`${context} must be an object`);
  }
  const unknown = Object.keys(options).find((key) => !allowedKeys.has(key));
  if (unknown != null) {
    throw new TypeError(`${context} contains unknown field '${unknown}'`);
  }
  return options;
}

function nativeOptions(options, allowedKeys = RUN_OPTION_KEYS) {
  checkedOptionObject(options, "RunOptions", allowedKeys);
  if (options == null) return [options, undefined];
  if ("permissions" in options) checkedPermissionRules(options.permissions);
  if (!("signal" in options)) return [options, undefined];
  const { signal, ...rest } = options;
  if (
    signal != null &&
    (typeof signal.addEventListener !== "function" ||
      typeof signal.removeEventListener !== "function")
  ) {
    throw new TypeError("RunOptions.signal must be an AbortSignal");
  }
  // Carry a pre-aborted signal into the Rust CancellationToken before the core driver is spawned.
  // Calling stream.cancel() only after nativeRun() returns races an ultra-fast mock/provider run.
  return [{ ...rest, cancelBeforeStart: signal?.aborted === true }, signal];
}

/** Pair one canonical tool definition with its host callback for convenient registration. */
function tool(name, description, inputSchema, callback) {
  if (typeof name !== "string" || name.length === 0) {
    throw new TypeError("tool name must be a non-empty string");
  }
  if (typeof description !== "string") {
    throw new TypeError("tool description must be a string");
  }
  if (inputSchema == null || typeof inputSchema !== "object") {
    throw new TypeError("tool inputSchema must be a JSON Schema object");
  }
  if (typeof callback !== "function") {
    throw new TypeError("tool callback must be a function");
  }
  return Object.freeze({ name, description, inputSchema, callback });
}

native.Agent.prototype.addToolDefinition = function (definition) {
  if (definition == null || typeof definition !== "object") {
    throw new TypeError("addToolDefinition expects a tool(...) definition");
  }
  return this.addTool(
    definition.name,
    definition.description,
    definition.inputSchema,
    definition.callback,
  );
};

const nativeSetPermissions = native.Agent.prototype.setPermissions;
native.Agent.prototype.setPermissions = function (rules, defaultMode) {
  return nativeSetPermissions.call(this, checkedPermissionRules(rules), defaultMode);
};

const DOCKER_OPTION_KEYS = new Set([
  "image",
  "executable",
  "pidsLimit",
  "memoryMiB",
  "cpus",
  "tmpfsMiB",
]);
const nativeEnableBash = native.Agent.prototype.enableBashWithRequiredContainment;
native.Agent.prototype.enableBashWithRequiredContainment = function (docker) {
  return nativeEnableBash.call(
    this,
    checkedOptionObject(docker, "DockerContainmentOptions", DOCKER_OPTION_KEYS),
  );
};

// napi represents Rust u64 values as BigInt. Session revisions are part of the public Node
// surface as ordinary numbers, so convert only while the value remains exactly representable.
const nativeRecoverExpiredSession = native.Agent.prototype.recoverExpiredSession;
native.Agent.prototype.recoverExpiredSession = function (
  sessionId,
  sideEffectsReconciled,
) {
  const revision = nativeRecoverExpiredSession.call(
    this,
    sessionId,
    sideEffectsReconciled,
  );
  if (
    typeof revision === "bigint" &&
    revision > BigInt(Number.MAX_SAFE_INTEGER)
  ) {
    throw new RangeError("session revision exceeds JavaScript's safe integer range");
  }
  return Number(revision);
};

// Thin Node ergonomics over the canonical SubagentSpec and existing fanOut implementation.
native.Agent.prototype.subtask = function (id, prompt, route, options = {}) {
  if (options == null || typeof options !== "object") {
    throw new TypeError("subtask options must be an object");
  }
  const allowed = new Set([
    "system",
    "allowedTools",
    "maxTurns",
    "maxTokens",
    "estimatedInputTokens",
  ]);
  const unknown = Object.keys(options).find((key) => !allowed.has(key));
  if (unknown != null) {
    throw new TypeError(`subtask options contain unknown field '${unknown}'`);
  }
  return {
    id,
    prompt,
    system: options.system ?? null,
    route,
    allowed_tools: [...(options.allowedTools ?? [])],
    max_turns: options.maxTurns ?? 16,
    max_tokens: options.maxTokens ?? 4096,
    estimated_input_tokens: options.estimatedInputTokens ?? 1024,
  };
};

const ORCHESTRATION_OPTION_KEYS = new Set(["maxParallelism", "budget"]);
const ORCHESTRATION_BUDGET_KEYS = new Set([
  "max_model_calls",
  "max_input_tokens",
  "max_output_tokens",
  "max_cost_micro_usd",
  "wall_time_ms",
]);

function checkedOrchestrationOptions(options) {
  if (options == null) return options;
  if (typeof options !== "object" || Array.isArray(options)) {
    throw new TypeError("OrchestrationOptions must be an object");
  }
  const unknown = Object.keys(options).find(
    (key) => !ORCHESTRATION_OPTION_KEYS.has(key),
  );
  if (unknown != null) {
    throw new TypeError(`OrchestrationOptions contains unknown field '${unknown}'`);
  }
  if (options.budget != null) {
    if (typeof options.budget !== "object" || Array.isArray(options.budget)) {
      throw new TypeError("OrchestrationOptions.budget must be an object");
    }
    const unknownBudget = Object.keys(options.budget).find(
      (key) => !ORCHESTRATION_BUDGET_KEYS.has(key),
    );
    if (unknownBudget != null) {
      throw new TypeError(
        `OrchestrationOptions.budget contains unknown field '${unknownBudget}'`,
      );
    }
  }
  return options;
}

for (const method of ["runSubagent", "fanOut", "council", "resumeSubagent"]) {
  const nativeMethod = native.Agent.prototype[method];
  native.Agent.prototype[method] = function (...args) {
    const optionsIndex = method === "council" ? 4 : method === "resumeSubagent" ? 3 : 2;
    if (args.length > optionsIndex) {
      args[optionsIndex] = checkedOrchestrationOptions(args[optionsIndex]);
    }
    return nativeMethod.apply(this, args);
  };
}

native.Agent.prototype.parallel = function (specs, profiles, options) {
  return this.fanOut(specs, profiles, options);
};

// Native methods return the same QueryStream used by the top-level `query` compatibility
// helper. Make Agent streaming equally idiomatic without adding another implementation layer.
const nativeGenerateText = native.Agent.prototype.generateText;
const GENERATE_TEXT_OPTION_KEYS = new Set(["model", "maxTokens"]);
native.Agent.prototype.generateText = async function (prompt, options) {
  try {
    return await nativeGenerateText.call(
      this,
      prompt,
      checkedOptionObject(options, "GenerateTextOptions", GENERATE_TEXT_OPTION_KEYS),
    );
  } catch (error) {
    throw normalizeNativeError(error);
  }
};

const nativeStreamText = native.Agent.prototype.streamText;
native.Agent.prototype.streamText = function (prompt, options) {
  return asyncIterable(
    nativeStreamText.call(
      this,
      prompt,
      checkedOptionObject(options, "GenerateTextOptions", GENERATE_TEXT_OPTION_KEYS),
    ),
  );
};

const nativeRun = native.Agent.prototype.run;
native.Agent.prototype.run = function (prompt, options) {
  const [runOptions, signal] = nativeOptions(options);
  return asyncIterable(nativeRun.call(this, prompt, runOptions), undefined, signal);
};

const nativeClientQuery = native.Client.prototype.query;
native.Client.prototype.query = function (prompt, options) {
  const [runOptions, signal] = nativeOptions(options);
  return asyncIterable(
    nativeClientQuery.call(this, prompt, runOptions),
    undefined,
    signal,
  );
};

function zodAdapter(schema) {
  const isZodSchema =
    schema != null &&
    typeof schema === "object" &&
    "_zod" in schema &&
    typeof schema.parse === "function";
  if (!isZodSchema) return null;

  let zod;
  try {
    zod = require("zod");
  } catch (error) {
    const dependencyError = new Error(
      "aikit received a Zod schema, but optional peer dependency 'zod' is not installed",
    );
    dependencyError.cause = error;
    throw dependencyError;
  }
  const toJSONSchema = zod.toJSONSchema ?? zod.z?.toJSONSchema;
  if (typeof toJSONSchema !== "function") {
    throw new TypeError("aikit requires Zod v4 (missing z.toJSONSchema)");
  }
  return {
    jsonSchema: toJSONSchema(schema),
    parse: (value) => schema.parse(value),
  };
}

// Accept Zod v4 schemas directly while keeping the native addon dependency-free. Zod stays an
// optional peer: raw JSON Schema callers never load it, while Zod callers get the same core JSON
// Schema validation plus a final `parse` that materializes their inferred runtime type.
const nativeGenerateObject = native.Agent.prototype.generateObject;
const GENERATE_OBJECT_OPTION_KEYS = new Set([
  "model",
  "maxRetries",
  "maxTokens",
  "name",
  "providerOptions",
  "validator",
]);
function structuredOptions(options) {
  const checked = checkedOptionObject(
    options,
    "GenerateObjectOptions",
    GENERATE_OBJECT_OPTION_KEYS,
  );
  if (checked == null) return [checked, undefined];
  const { validator, ...nativeOptions } = checked;
  if (validator != null && typeof validator !== "function") {
    throw new TypeError("GenerateObjectOptions.validator must be an async function");
  }
  return [nativeOptions, validator];
}
native.Agent.prototype.generateObject = async function (prompt, schema, options) {
  const adapter = zodAdapter(schema);
  const [checkedOptions, validator] = structuredOptions(options);
  try {
    if (adapter == null) {
      return await nativeGenerateObject.call(this, prompt, schema, checkedOptions, validator);
    }
    const result = await nativeGenerateObject.call(
      this,
      prompt,
      adapter.jsonSchema,
      checkedOptions,
      validator,
    );
    return { ...result, value: adapter.parse(result.value) };
  } catch (error) {
    throw normalizeNativeError(error);
  }
};

// Zod materializes only the final validated value. Attempt, delta, and validation/repair events
// stay byte-for-byte observable, so typed convenience never collapses a real stream into a
// one-shot promise.
const nativeStreamObject = native.Agent.prototype.streamObject;
native.Agent.prototype.streamObject = function (prompt, schema, options) {
  const adapter = zodAdapter(schema);
  const [checkedOptions, validator] = structuredOptions(options);
  const stream = nativeStreamObject.call(
    this,
    prompt,
    adapter?.jsonSchema ?? schema,
    checkedOptions,
    validator,
  );
  return asyncIterable(stream, (event) => {
    if (adapter == null || event?.type !== "completed") return event;
    return {
      ...event,
      object: {
        ...event.object,
        value: adapter.parse(event.object.value),
      },
    };
  });
};

module.exports = {
  Agent: native.Agent,
  Client: native.Client,
  McpServer: native.McpServer,
  evaluateOutcome: native.evaluateOutcome,
  tool,
  // query(prompt, tools?, options?) — `tools` maps a name to a JS `async (input) => string`.
  query: (prompt, tools, options) => {
    const [runOptions, signal] = nativeOptions(options, QUERY_OPTION_KEYS);
    return asyncIterable(native.query(prompt, tools, runOptions), undefined, signal);
  },
};
