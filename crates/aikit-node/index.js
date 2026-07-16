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
    if (header?.glibcVersionRuntime == null) {
      throw new Error(
        "aikit-runtime currently publishes glibc Linux addons only; musl is not yet supported",
      );
    }
  }
  return selected;
}

function loadNative() {
  // Local builds intentionally remain simple: scripts/build-node.sh stages the addon beside this
  // wrapper. Published installs omit that file and resolve the exact optional platform package.
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
  if (typeof transform === "function") {
    const nativeNext = stream.next.bind(stream);
    stream.next = async function () {
      try {
        const value = await nativeNext();
        return value == null ? null : transform(value);
      } catch (error) {
        throw normalizeNativeError(error);
      }
    };
  }
  const nativeClose =
    typeof stream.close === "function" ? stream.close.bind(stream) : null;
  let closePromise;
  let abortListener;
  const removeAbortListener = () => {
    if (abortListener != null) signal?.removeEventListener("abort", abortListener);
    abortListener = undefined;
  };
  const close = () => {
    if (closePromise == null) {
      closePromise = Promise.resolve(nativeClose?.()).finally(removeAbortListener);
    }
    return closePromise;
  };
  if (nativeClose != null) stream.close = close;
  if (signal != null) {
    if (typeof signal.addEventListener !== "function") {
      throw new TypeError("options.signal must be an AbortSignal");
    }
    abortListener = () => {
      stream.cancel?.();
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

function nativeOptions(options) {
  if (options == null || !("signal" in options)) return [options, undefined];
  const { signal, ...rest } = options;
  return [rest, signal];
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

// Thin Node ergonomics over the canonical SubagentSpec and existing fanOut implementation.
native.Agent.prototype.subtask = function (id, prompt, route, options = {}) {
  if (options == null || typeof options !== "object") {
    throw new TypeError("subtask options must be an object");
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

native.Agent.prototype.parallel = function (specs, profiles, options) {
  return this.fanOut(specs, profiles, options);
};

// Native methods return the same QueryStream used by the top-level `query` compatibility
// helper. Make Agent streaming equally idiomatic without adding another implementation layer.
const nativeGenerateText = native.Agent.prototype.generateText;
native.Agent.prototype.generateText = async function (...args) {
  try {
    return await nativeGenerateText.apply(this, args);
  } catch (error) {
    throw normalizeNativeError(error);
  }
};

const nativeStreamText = native.Agent.prototype.streamText;
native.Agent.prototype.streamText = function (...args) {
  return asyncIterable(nativeStreamText.apply(this, args));
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
native.Agent.prototype.generateObject = async function (prompt, schema, options) {
  const adapter = zodAdapter(schema);
  try {
    if (adapter == null) {
      return await nativeGenerateObject.call(this, prompt, schema, options);
    }
    const result = await nativeGenerateObject.call(
      this,
      prompt,
      adapter.jsonSchema,
      options,
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
  const stream = nativeStreamObject.call(
    this,
    prompt,
    adapter?.jsonSchema ?? schema,
    options,
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
  tool,
  // query(prompt, tools?, options?) — `tools` maps a name to a JS `async (input) => string`.
  query: (prompt, tools, options) => {
    const [runOptions, signal] = nativeOptions(options);
    return asyncIterable(native.query(prompt, tools, runOptions), undefined, signal);
  },
};
