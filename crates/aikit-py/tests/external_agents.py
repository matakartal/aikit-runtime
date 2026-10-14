"""Keyless external-agent projection coverage: discovery, signed manifests, update state."""

import base64
import os
import stat
import tempfile

from typing import TYPE_CHECKING, Any, Dict, List, Optional, cast

from aikit import (
    ExternalUpdateState,
    discover_external_runtime_candidates,
    external_update_refresh_actions,
    select_external_runtime_update,
    validate_external_update_config,
    verify_external_update_manifest,
)

if TYPE_CHECKING:
    from aikit import (
        ExternalRuntimeCandidate,
        ExternalUpdateConfig,
        ExternalUpdateManifest,
    )

# Deterministic fixture: Ed25519 seed [7; 32], detached signature over the exact bytes below.
MANIFEST_JSON = (
    '{"schema_version":1,"generated_unix_ms":1000,"expires_unix_ms":10000,"releases":'
    '[{"kind":"gemini","channel":"stable","version":"1.2.3","protocol":"acp",'
    '"contract_version":1,"artifacts":[{"target_os":"macos","target_arch":"aarch64",'
    '"url":"https://updates.example.test/gemini",'
    '"sha256":"d92c6a81b2ff50096bcda80885427d1f59a25b5f483f7055523504925d16ab23",'
    '"size_bytes":7,"executable_name":"gemini"}]}]}'
)
SIGNATURE_B64 = (
    "PLnOFo824GQR9bmoOdChdF+xvlgh3C34yQnNY7ZMTcndBEPqPrrLpVoGUSbr4c0G87/KkkopnitV8QpkbW6LAA=="
)
PUBLIC_KEY_B64 = "6kpsY+KcUgq+9VB7Ey7F+ZVHdq6+vnuSQh7qaRRG0iw="
KEY_ID = "stable-2026"
NOW_MS = 2_000


def expect_error(callable_: Any, *args: Any, **kwargs: Any) -> str:
    try:
        callable_(*args, **kwargs)
    except Exception as error:  # noqa: BLE001 - the message content is the assertion
        return str(error)
    raise AssertionError("expected the call to fail closed")


def verified_manifest() -> "ExternalUpdateManifest":
    return verify_external_update_manifest(
        MANIFEST_JSON.encode(),
        SIGNATURE_B64,
        KEY_ID,
        {KEY_ID: PUBLIC_KEY_B64},
        NOW_MS,
    )


def test_signature_is_verified_before_parse() -> None:
    manifest = verified_manifest()
    assert manifest["schema_version"] == 1
    assert manifest["releases"][0]["kind"] == "gemini"
    assert manifest["releases"][0]["version"] == "1.2.3"

    tampered = expect_error(
        verify_external_update_manifest,
        (MANIFEST_JSON + " ").encode(),
        SIGNATURE_B64,
        KEY_ID,
        {KEY_ID: PUBLIC_KEY_B64},
        NOW_MS,
    )
    assert "signature verification failed" in tampered

    untrusted = expect_error(
        verify_external_update_manifest,
        MANIFEST_JSON.encode(),
        SIGNATURE_B64,
        "other-key",
        {KEY_ID: PUBLIC_KEY_B64},
        NOW_MS,
    )
    assert "untrusted key id" in untrusted

    expired = expect_error(
        verify_external_update_manifest,
        MANIFEST_JSON.encode(),
        SIGNATURE_B64,
        KEY_ID,
        {KEY_ID: PUBLIC_KEY_B64},
        20_000,
    )
    assert "expired" in expired

    bad_key = expect_error(
        verify_external_update_manifest,
        MANIFEST_JSON.encode(),
        SIGNATURE_B64,
        KEY_ID,
        {KEY_ID: base64.b64encode(b"short").decode()},
        NOW_MS,
    )
    assert "32 bytes" in bad_key


def test_selection_respects_channel_target_and_current_version() -> None:
    manifest = verified_manifest()
    selection = select_external_runtime_update(
        manifest, "gemini", "stable", "0.3.0", "macos", "aarch64"
    )
    assert selection is not None
    assert selection["version"] == "1.2.3"
    assert selection["artifact"]["executable_name"] == "gemini"

    assert (
        select_external_runtime_update(
            manifest, "gemini", "stable", "0.3.0", "macos", "aarch64", "1.2.3"
        )
        is None
    )
    assert (
        select_external_runtime_update(
            manifest, "gemini", "preview", "0.3.0", "macos", "aarch64"
        )
        is None
    )
    assert (
        select_external_runtime_update(
            manifest, "codex", "stable", "0.3.0", "macos", "aarch64"
        )
        is None
    )
    assert (
        select_external_runtime_update(
            manifest, "gemini", "stable", "0.3.0", "linux", "x86_64"
        )
        is None
    )
    return None


def test_update_state_machine_is_fail_closed() -> None:
    manifest = verified_manifest()
    selection = select_external_runtime_update(
        manifest, "gemini", "stable", "0.3.0", "macos", "aarch64"
    )
    assert selection is not None

    state = ExternalUpdateState("gemini-default", "managed")
    snapshot = cast(Dict[str, Any], state.snapshot())
    assert snapshot["policy"] == "aikit_managed"
    assert snapshot["phase"] == "idle"
    assert state.manifest_check_due(NOW_MS)

    # Downloading before an available selection fails closed and mutates nothing.
    error = expect_error(state.begin_download)
    assert "cannot download" in error
    assert cast(Dict[str, Any], state.snapshot())["phase"] == "idle"

    state.record_check_success(NOW_MS, selection)
    assert cast(Dict[str, Any], state.snapshot())["phase"] == "available"
    assert state.available_version == "1.2.3"
    assert not state.manifest_check_due(NOW_MS + 1)
    assert state.may_apply_binary_update()
    assert "stage_managed_update" in cast(
        List[str], state.refresh_actions(NOW_MS + 1)
    )

    state.begin_download()
    state.record_staged()
    state.begin_activation()
    state.record_activated()
    assert cast(Dict[str, Any], state.snapshot())["phase"] == "current"
    assert state.current_version == "1.2.3"
    assert state.available_version is None

    state.record_rollback("1.0.0", "1.2.3")
    rolled = cast(Dict[str, Any], state.snapshot())
    assert rolled["phase"] == "rolled_back"
    assert state.current_version == "1.0.0"
    assert state.quarantined_version == "1.2.3"

    # Failure backoff escalates 1h -> 6h -> 24h from the injected instant.
    state.record_failure(NOW_MS, "manifest fetch failed")
    assert state.consecutive_failures == 1
    assert state.retry_after_unix_ms == NOW_MS + 60 * 60 * 1_000
    assert not state.manifest_check_due(NOW_MS + 1)
    state.record_failure(NOW_MS, "manifest fetch failed")
    assert state.retry_after_unix_ms == NOW_MS + 6 * 60 * 60 * 1_000

    # External profiles never auto-stage binary updates.
    external = ExternalUpdateState("gemini-path", "external")
    external.record_check_success(NOW_MS, selection)
    assert not external.may_apply_binary_update()
    actions = cast(List[str], external.refresh_actions(NOW_MS + 1))
    assert "report_external_update" in actions
    assert "stage_managed_update" not in actions
    assert cast(List[str], external_update_refresh_actions(
        cast(Dict[str, Any], external.snapshot()), NOW_MS + 1
    )) == actions

    # Snapshot round-trip and recovery of an interrupted activation.
    staged = ExternalUpdateState("gemini-default", "managed")
    staged.record_check_success(NOW_MS, selection)
    staged.begin_download()
    staged.record_staged()
    restored = ExternalUpdateState.from_state(staged.snapshot())
    assert restored.recover_activated("1.2.3")
    assert restored.current_version == "1.2.3"
    assert not restored.recover_activated("9.9.9")


def test_update_config_validation_fails_closed() -> None:
    config: "ExternalUpdateConfig" = {
        "schema_version": 1,
        "instance_id": "gemini-default",
        "kind": "gemini",
        "profile": "managed",
        "policy": "aikit_managed",
        "channel": "stable",
        "source": {
            "manifest_url": "https://updates.example.test/manifest.json",
            "signature_url": "https://updates.example.test/manifest.sig",
            "key_id": KEY_ID,
            "public_key_base64": PUBLIC_KEY_B64,
        },
    }
    validate_external_update_config(config)

    external = cast(Dict[str, Any], dict(config))
    external["profile"] = "external"
    error = expect_error(validate_external_update_config, external)
    assert "cannot use the AIKit-managed update policy" in error

    credentials = cast(Dict[str, Any], dict(config))
    credentials["source"] = dict(cast(Dict[str, Any], dict(config["source"])))
    credentials["source"]["manifest_url"] = "https://updates.example.test/m.json?token=x"
    error = expect_error(validate_external_update_config, credentials)
    assert "query credentials" in error


def test_discovery_inspects_but_never_executes() -> None:
    with tempfile.TemporaryDirectory() as root:
        executable = os.path.join(root, "gemini")
        with open(executable, "w", encoding="utf-8") as handle:
            handle.write("#!/bin/sh\nexit 42\n")
        os.chmod(executable, stat.S_IRWXU)
        candidates: List["ExternalRuntimeCandidate"] = (
            discover_external_runtime_candidates("gemini", executable)
        )
        explicit = [
            candidate
            for candidate in candidates
            if candidate["executable"] == executable
        ]
        assert len(explicit) == 1
        assert explicit[0]["kind"] == "gemini"
        assert "explicit" in explicit[0]["discovered_by"]

    missing: Optional[str] = None
    try:
        discover_external_runtime_candidates("gemini", os.path.join(root, "gone"))
    except Exception as error:  # noqa: BLE001
        missing = str(error)
    assert missing is not None


def main() -> None:
    test_signature_is_verified_before_parse()
    test_selection_respects_channel_target_and_current_version()
    test_update_state_machine_is_fail_closed()
    test_update_config_validation_fails_closed()
    test_discovery_inspects_but_never_executes()
    print("external agents Python contract: ok")


if __name__ == "__main__":
    main()
