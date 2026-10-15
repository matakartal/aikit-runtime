"use strict";

// aikit Node live-provider example — the quickstart's first steps against a real model.
//
// Unlike the other examples, this one CAN make a network request and spend tokens. It does so
// only when you have exported a provider key; without one it explains what to set and exits
// successfully without contacting anything.
//
// Prerequisite (once, from the repository root):  ./scripts/build-node.sh
// Run (any one supported key):
//   ANTHROPIC_API_KEY=... node examples/node/live_provider.cjs
//   OPENAI_API_KEY=...    node examples/node/live_provider.cjs
//   GEMINI_API_KEY=...    node examples/node/live_provider.cjs   (or GOOGLE_API_KEY)
// Also recognised: DEEPSEEK_API_KEY, OPENROUTER_API_KEY, GROQ_API_KEY, MISTRAL_API_KEY,
// XAI_API_KEY. Set AIKIT_MODEL=<model id> to pick the model yourself; otherwise the example
// picks the shipped-catalog model with the lowest listed output price (unpriced models last)
// among providers you have a key for. Output is capped at 128 tokens per request.

const path = require("node:path");

function loadBinding() {
  const entry = path.join(__dirname, "..", "..", "crates", "aikit-node", "index.js");
  try {
    return require(entry);
  } catch (error) {
    console.error(
      "Could not load the aikit Node binding from crates/aikit-node.\n" +
        "Build it first from the repository root:\n\n" +
        "    ./scripts/build-node.sh\n\n" +
        `Underlying error: ${error.message}`,
    );
    process.exit(1);
  }
}

const { Agent, shippedModelCatalog } = loadBinding();

// Hints for the stable error codes a first live run most often hits (see ErrorCode in index.d.ts).
const HINTS = {
  provider_auth: "the provider rejected the key, or no key is active for this model's provider",
  provider_invalid_request:
    "check AIKIT_MODEL — the id may be unknown or unavailable to your account",
  provider_rate_limit: "rate limited — wait and retry, or lower request volume",
  provider_timeout: "the provider did not answer in time — retry, or check connectivity",
  provider_transport: "network problem reaching the provider — check connectivity/proxy settings",
};

function pickModel(agent) {
  if (process.env.AIKIT_MODEL) return process.env.AIKIT_MODEL;
  const candidates = shippedModelCatalog().profiles.filter((profile) =>
    agent.hasProvider(profile.provider),
  );
  const price = (profile) => profile.pricing?.output_per_million_usd ?? Number.POSITIVE_INFINITY;
  candidates.sort((a, b) => price(a) - price(b));
  return candidates[0]?.model;
}

async function main() {
  // new Agent() discovers provider keys from process.env (key values are never printed).
  const agent = new Agent();
  const providers = agent.activeProviders();
  if (providers.length === 0) {
    console.log(
      "No provider key found, so nothing was sent anywhere.\n" +
        "Export one supported key (for example ANTHROPIC_API_KEY, OPENAI_API_KEY, or\n" +
        "GEMINI_API_KEY) and run this example again. For a keyless tour, run:\n\n" +
        "    node examples/node/quickstart.cjs",
    );
    return;
  }
  console.log("active providers:", providers);

  const model = pickModel(agent);
  if (!model) {
    console.log("No shipped-catalog model matches your providers; set AIKIT_MODEL=<model id>.");
    return;
  }
  console.log("model:", model);

  try {
    const answer = await agent.generateText("In one short sentence, what is an agent runtime?", {
      model,
      maxTokens: 128,
    });
    console.log("text:", answer.text);
    console.log("usage:", answer.usage.input_tokens, "in /", answer.usage.output_tokens, "out");

    process.stdout.write("stream: ");
    for await (const delta of agent.streamText("Count from 1 to 5.", { model, maxTokens: 128 })) {
      if (delta.type === "text_delta") process.stdout.write(delta.text);
    }
    process.stdout.write("\n");
  } catch (error) {
    // Generation failures carry a stable, redacted `code` (and `info`) that is safe to branch on.
    console.error(`live request failed [${error.code ?? "unknown"}]: ${error.message}`);
    if (HINTS[error.code]) console.error(`hint: ${HINTS[error.code]}`);
    process.exitCode = 1;
  }
}

main().catch((error) => {
  console.error(error);
  process.exitCode = 1;
});
