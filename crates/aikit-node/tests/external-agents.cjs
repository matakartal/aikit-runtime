"use strict";

// Keyless external-agent projection coverage: discovery, signed manifests, update state.
const assert = require("node:assert/strict");
const fs = require("node:fs");
const os = require("node:os");
const path = require("node:path");

const {
  ExternalUpdateState,
  discoverExternalRuntimeCandidates,
  externalUpdateRefreshActions,
  selectExternalRuntimeUpdate,
  validateExternalUpdateConfig,
  verifyExternalUpdateManifest,
} = require("..");

// Deterministic fixture: Ed25519 seed [7; 32], detached signature over the exact bytes below.
const MANIFEST_JSON =
  '{"schema_version":1,"generated_unix_ms":1000,"expires_unix_ms":10000,"releases":' +
  '[{"kind":"gemini","channel":"stable","version":"1.2.3","protocol":"acp",' +
  '"contract_version":1,"artifacts":[{"target_os":"macos","target_arch":"aarch64",' +
  '"url":"https://updates.example.test/gemini",' +
  '"sha256":"d92c6a81b2ff50096bcda80885427d1f59a25b5f483f7055523504925d16ab23",' +
  '"size_bytes":7,"executable_name":"gemini"}]}]}';
const SIGNATURE_B64 =
  "PLnOFo824GQR9bmoOdChdF+xvlgh3C34yQnNY7ZMTcndBEPqPrrLpVoGUSbr4c0G87/KkkopnitV8QpkbW6LAA==";
const PUBLIC_KEY_B64 = "6kpsY+KcUgq+9VB7Ey7F+ZVHdq6+vnuSQh7qaRRG0iw=";
const KEY_ID = "stable-2026";
const NOW_MS = 2_000;

function expectError(callable, ...args) {
  try {
    callable(...args);
  } catch (error) {
    return String(error.message ?? error);
  }
  throw new Error("expected the call to fail closed");
}

const manifestBytes = Buffer.from(MANIFEST_JSON);
const trustedKeys = { [KEY_ID]: PUBLIC_KEY_B64 };

// Signature is verified before parsing; failures name the exact rejection.
const manifest = verifyExternalUpdateManifest(
  manifestBytes,
  SIGNATURE_B64,
  KEY_ID,
  trustedKeys,
  NOW_MS,
);
assert.equal(manifest.schema_version, 1);
assert.equal(manifest.releases[0].kind, "gemini");
assert.equal(manifest.releases[0].version, "1.2.3");
assert.match(
  expectError(
    verifyExternalUpdateManifest,
    Buffer.from(MANIFEST_JSON + " "),
    SIGNATURE_B64,
    KEY_ID,
    trustedKeys,
    NOW_MS,
  ),
  /signature verification failed/,
);
assert.match(
  expectError(
    verifyExternalUpdateManifest,
    manifestBytes,
    SIGNATURE_B64,
    "other-key",
    trustedKeys,
    NOW_MS,
  ),
  /untrusted key id/,
);
assert.match(
  expectError(
    verifyExternalUpdateManifest,
    manifestBytes,
    SIGNATURE_B64,
    KEY_ID,
    trustedKeys,
    20_000,
  ),
  /expired/,
);

// Selection respects kind, channel, target, and monotone version upgrades.
const selection = selectExternalRuntimeUpdate(
  manifest,
  "gemini",
  "stable",
  "0.3.0",
  "macos",
  "aarch64",
);
assert.ok(selection);
assert.equal(selection.version, "1.2.3");
assert.equal(selection.artifact.executable_name, "gemini");
assert.equal(
  selectExternalRuntimeUpdate(manifest, "gemini", "stable", "0.3.0", "macos", "aarch64", "1.2.3"),
  null,
);
assert.equal(
  selectExternalRuntimeUpdate(manifest, "gemini", "preview", "0.3.0", "macos", "aarch64"),
  null,
);
assert.equal(
  selectExternalRuntimeUpdate(manifest, "codex", "stable", "0.3.0", "macos", "aarch64"),
  null,
);

// The managed-update state machine is fail-closed and clock-injected.
const state = new ExternalUpdateState("gemini-default", "managed");
assert.equal(state.snapshot().policy, "aikit_managed");
assert.equal(state.snapshot().phase, "idle");
assert.ok(state.manifestCheckDue(NOW_MS));
assert.match(expectError(() => state.beginDownload()), /cannot download/);
assert.equal(state.snapshot().phase, "idle");

state.recordCheckSuccess(NOW_MS, selection);
assert.equal(state.snapshot().phase, "available");
assert.equal(state.availableVersion, "1.2.3");
assert.ok(!state.manifestCheckDue(NOW_MS + 1));
assert.ok(state.mayApplyBinaryUpdate());
assert.ok(state.refreshActions(NOW_MS + 1).includes("stage_managed_update"));

state.beginDownload();
state.recordStaged();
state.beginActivation();
state.recordActivated();
assert.equal(state.snapshot().phase, "current");
assert.equal(state.currentVersion, "1.2.3");
assert.equal(state.availableVersion, null);

state.recordRollback("1.0.0", "1.2.3");
assert.equal(state.snapshot().phase, "rolled_back");
assert.equal(state.currentVersion, "1.0.0");
assert.equal(state.quarantinedVersion, "1.2.3");

// Failure backoff escalates 1h -> 6h -> 24h from the injected instant.
state.recordFailure(NOW_MS, "manifest fetch failed");
assert.equal(state.consecutiveFailures, 1);
assert.equal(state.retryAfterUnixMs, NOW_MS + 60 * 60 * 1_000);
assert.ok(!state.manifestCheckDue(NOW_MS + 1));
state.recordFailure(NOW_MS, "manifest fetch failed");
assert.equal(state.retryAfterUnixMs, NOW_MS + 6 * 60 * 60 * 1_000);

// External profiles never auto-stage binary updates.
const external = new ExternalUpdateState("gemini-path", "external");
external.recordCheckSuccess(NOW_MS, selection);
assert.ok(!external.mayApplyBinaryUpdate());
const actions = external.refreshActions(NOW_MS + 1);
assert.ok(actions.includes("report_external_update"));
assert.ok(!actions.includes("stage_managed_update"));
assert.deepEqual(externalUpdateRefreshActions(external.snapshot(), NOW_MS + 1), actions);

// Snapshot round-trip across the JS number bridge, including a large wall-clock instant.
const staged = new ExternalUpdateState("gemini-default", "managed");
staged.recordCheckSuccess(1_785_916_419_448, selection);
staged.beginDownload();
staged.recordStaged();
const restored = ExternalUpdateState.fromState(staged.snapshot());
assert.equal(restored.snapshot().last_manifest_check_unix_ms, 1_785_916_419_448);
assert.ok(restored.recoverActivated("1.2.3"));
assert.equal(restored.currentVersion, "1.2.3");
assert.ok(!restored.recoverActivated("9.9.9"));

// Update configuration validation fails closed on policy and credential-bearing URLs.
const config = {
  schema_version: 1,
  instance_id: "gemini-default",
  kind: "gemini",
  profile: "managed",
  policy: "aikit_managed",
  channel: "stable",
  source: {
    manifest_url: "https://updates.example.test/manifest.json",
    signature_url: "https://updates.example.test/manifest.sig",
    key_id: KEY_ID,
    public_key_base64: PUBLIC_KEY_B64,
  },
};
validateExternalUpdateConfig(config);
assert.match(
  expectError(validateExternalUpdateConfig, { ...config, profile: "external" }),
  /cannot use the AIKit-managed update policy/,
);
assert.match(
  expectError(validateExternalUpdateConfig, {
    ...config,
    source: { ...config.source, manifest_url: "https://updates.example.test/m.json?token=x" },
  }),
  /query credentials/,
);

// Discovery inspects executables without ever executing them.
const root = fs.mkdtempSync(path.join(os.tmpdir(), "aikit-external-"));
try {
  const executable = path.join(root, "gemini");
  fs.writeFileSync(executable, "#!/bin/sh\nexit 42\n", { mode: 0o755 });
  const candidates = discoverExternalRuntimeCandidates("gemini", executable);
  const explicit = candidates.filter((candidate) => candidate.executable === executable);
  assert.equal(explicit.length, 1);
  assert.equal(explicit[0].kind, "gemini");
  assert.ok(explicit[0].discovered_by.includes("explicit"));
  assert.match(
    expectError(discoverExternalRuntimeCandidates, "gemini", path.join(root, "gone")),
    /./,
  );
} finally {
  fs.rmSync(root, { recursive: true, force: true });
}

console.log("external agents Node contract: ok");
