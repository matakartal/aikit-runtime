"""Keyless A2A mapper coverage for tenant scope, cursor paging, and restore."""

import json

from typing import TYPE_CHECKING, Any, cast

from aikit import A2aMapper

EXPECTED_A2A_MAPPER_SCHEMA_VERSION = 4

if TYPE_CHECKING:
    from aikit import (
        A2aAction,
        A2aCorrelationIdentity,
        A2aDispatchMessageAction,
        A2aGovernedAction,
        A2aGovernedActionAllowed,
        A2aListTasksAction,
        A2aMessage,
        A2aProtocolPrincipal,
        A2aRunMapping,
        A2aTaskState,
    )


def correlation(sequence: int) -> "A2aCorrelationIdentity":
    return {
        "correlation_id": f"correlation-{sequence}",
        "request_id": f"request-{sequence}",
    }


def principal(subject: str, tenant: str) -> "A2aProtocolPrincipal":
    return {
        "subject": subject,
        "tenant_id": tenant,
        "scopes": ["a2a:message:send", "a2a:tasks:read", "a2a:tasks:cancel"],
    }


def message(message_id: str, context_id: str) -> "A2aMessage":
    return {
        "message_id": message_id,
        "context_id": context_id,
        "role": "ROLE_USER",
        "parts": [{"kind": "text", "text": message_id}],
    }


def targeted_message(message_id: str, mapping: "A2aRunMapping") -> "A2aMessage":
    return {
        **message(message_id, mapping["context_id"]),
        "task_id": mapping["task_id"],
    }


def serialized_state(mapper: A2aMapper) -> bytes:
    return json.dumps(
        mapper.snapshot(), sort_keys=True, separators=(",", ":")
    ).encode("utf-8")


def allowed_action(result: "A2aGovernedAction") -> "A2aAction":
    assert result["envelope"]["authorization"] == {"status": "allowed"}
    assert "action" in result
    return cast("A2aGovernedActionAllowed", result)["action"]


def send(
    mapper: A2aMapper,
    message_id: str,
    context_id: str,
    sequence: int,
    actor: "A2aProtocolPrincipal",
) -> str:
    result = mapper.send_message(
        message(message_id, context_id), correlation(sequence), actor
    )
    action = cast("A2aDispatchMessageAction", allowed_action(result))
    assert action["kind"] == "dispatch_message"
    return action["mapping"]["task_id"]


def main() -> None:
    mapper = A2aMapper()
    tenant_a = principal("owner-a", "tenant-a")
    tenant_b = principal("owner-b", "tenant-b")

    first_task = send(mapper, "message-a-1", "context-a-1", 1, tenant_a)
    second_task = send(mapper, "message-a-2", "context-a-2", 2, tenant_a)
    hidden_task = send(mapper, "message-b-1", "context-b-1", 3, tenant_b)

    first_page = mapper.list_tasks(
        {"tenant": "tenant-a", "pageSize": 1}, correlation(4), tenant_a
    )
    page = cast("A2aListTasksAction", allowed_action(first_page))["page"]
    assert page["totalSize"] == 2
    assert page["pageSize"] == 1
    assert len(page["tasks"]) == 1
    assert page["tasks"][0]["mapping"]["task_id"] == second_task
    assert page["tasks"][0]["owner_tenant_id"] == "tenant-a"
    assert page["tasks"][0]["mapping"]["task_id"] != hidden_task
    assert page["nextPageToken"]

    snapshot = mapper.snapshot()
    assert snapshot["schema_version"] == EXPECTED_A2A_MAPPER_SCHEMA_VERSION
    restored = A2aMapper.from_state(snapshot)
    assert restored.snapshot() == snapshot

    second_page = restored.list_tasks(
        {
            "tenant": "tenant-a",
            "pageSize": 1,
            "pageToken": page["nextPageToken"],
        },
        correlation(5),
        tenant_a,
    )
    restored_page = cast("A2aListTasksAction", allowed_action(second_page))["page"]
    assert restored_page["totalSize"] == 2
    assert restored_page["nextPageToken"] == ""
    assert [task["mapping"]["task_id"] for task in restored_page["tasks"]] == [
        first_task
    ]

    denied = restored.list_tasks(
        {"tenant": "tenant-b"}, correlation(6), tenant_a
    )
    assert denied["envelope"]["authorization"]["status"] == "denied"
    assert denied["envelope"]["authorization"]["code"] == "principal_mismatch"
    assert "action" not in denied

    transitioned = restored.transition_task(
        first_task, "TASK_STATE_COMPLETED", "finished"
    )
    assert transitioned["schema_version"] == EXPECTED_A2A_MAPPER_SCHEMA_VERSION
    assert transitioned["tasks"][first_task]["state"] == "TASK_STATE_COMPLETED"
    assert transitioned == restored.snapshot()
    try:
        restored.transition_task(first_task, "TASK_STATE_WORKING")
    except ValueError as error:
        assert "invalid_transition" in str(error), error
    else:
        raise AssertionError("invalid terminal A2A task transition was accepted")

    try:
        A2aMapper.from_state(cast(Any, {**snapshot, "unexpected": True}))
    except ValueError as error:
        assert "unknown field" in str(error), error
    else:
        raise AssertionError("unknown A2A mapper state field was accepted")

    invalid_mapper = A2aMapper()
    valid_message = message("governed-invalid-message", "governed-context")
    empty_principal = invalid_mapper.send_message(
        valid_message,
        correlation(7),
        cast(Any, {**tenant_a, "subject": ""}),
    )
    assert empty_principal["envelope"]["authorization"]["status"] == "denied"
    assert empty_principal["envelope"]["authorization"]["code"] == "invalid_request"
    assert "action" not in empty_principal

    empty_correlation = invalid_mapper.send_message(
        valid_message,
        cast(Any, {"correlation_id": "", "request_id": "empty-request"}),
        tenant_a,
    )
    assert empty_correlation["envelope"]["authorization"]["status"] == "denied"
    assert empty_correlation["envelope"]["authorization"]["code"] == "invalid_request"
    assert "action" not in empty_correlation

    empty_parts = invalid_mapper.send_message(
        cast(Any, {**valid_message, "message_id": "empty-parts", "parts": []}),
        correlation(8),
        tenant_a,
    )
    assert empty_parts["envelope"]["authorization"]["status"] == "denied"
    assert empty_parts["envelope"]["authorization"]["code"] == "invalid_request"
    assert "action" not in empty_parts

    try:
        invalid_mapper.send_message(
            cast(
                Any,
                {
                    **valid_message,
                    "message_id": "unknown-part-field",
                    "parts": [
                        {"kind": "text", "text": "hello", "unexpected": True}
                    ],
                },
            ),
            correlation(9),
            tenant_a,
        )
    except ValueError as error:
        assert "unknown field" in str(error) and "unexpected" in str(error), error
    else:
        raise AssertionError("unknown A2A message-part field was accepted")

    scoped_mapper = A2aMapper()
    shared_message = message("shared-message-id", "shared-id-context")
    scoped_tenant_a = principal("shared-owner", "tenant-a")
    scoped_tenant_b = principal("shared-owner", "tenant-b")
    tenant_a_receipt = scoped_mapper.send_message(
        shared_message, correlation(10), scoped_tenant_a
    )
    tenant_b_receipt = scoped_mapper.send_message(
        shared_message, correlation(11), scoped_tenant_b
    )
    tenant_a_action = cast(
        "A2aDispatchMessageAction", allowed_action(tenant_a_receipt)
    )
    tenant_b_action = cast(
        "A2aDispatchMessageAction", allowed_action(tenant_b_receipt)
    )
    assert tenant_a_action["kind"] == "dispatch_message"
    assert tenant_b_action["kind"] == "dispatch_message"
    assert tenant_a_action["mapping"]["task_id"] != tenant_b_action["mapping"]["task_id"]

    tenant_a_duplicate = scoped_mapper.send_message(
        shared_message, correlation(12), scoped_tenant_a
    )
    tenant_a_duplicate_action = allowed_action(tenant_a_duplicate)
    assert tenant_a_duplicate_action["kind"] == "duplicate_message"
    assert (
        tenant_a_duplicate_action["receipt"]["mapping"]["task_id"]
        == tenant_a_action["mapping"]["task_id"]
    )

    owner_task_id = tenant_a_action["mapping"]["task_id"]
    owner_get = scoped_mapper.get_task(
        owner_task_id, correlation(13), scoped_tenant_a
    )
    assert allowed_action(owner_get)["kind"] == "get_task"

    foreign_get = scoped_mapper.get_task(
        owner_task_id, correlation(14), scoped_tenant_b
    )
    unknown_get = scoped_mapper.get_task(
        "a2a-task-9999999999999999", correlation(15), scoped_tenant_b
    )
    for denied_task in [foreign_get, unknown_get]:
        assert denied_task["envelope"]["authorization"]["status"] == "denied"
        assert denied_task["envelope"]["authorization"]["code"] == "unknown_target"
        assert "session_id" not in denied_task["envelope"]["correlation"]
        assert "run_id" not in denied_task["envelope"]["correlation"]
        assert "action" not in denied_task

    foreign_cancel = scoped_mapper.cancel_task(
        owner_task_id, correlation(16), scoped_tenant_b
    )
    foreign_cancel_auth = foreign_cancel["envelope"]["authorization"]
    assert foreign_cancel_auth["status"] == "denied"
    assert foreign_cancel_auth["code"] == "unknown_target"
    assert "action" not in foreign_cancel

    read_only_owner = cast(
        "A2aProtocolPrincipal",
        {
            "subject": scoped_tenant_a["subject"],
            "tenant_id": scoped_tenant_a["tenant_id"],
            "scopes": ["a2a:message:send"],
        },
    )
    missing_scope = scoped_mapper.get_task(
        owner_task_id, correlation(17), read_only_owner
    )
    missing_scope_auth = missing_scope["envelope"]["authorization"]
    assert missing_scope_auth["status"] == "denied"
    assert missing_scope_auth["code"] == "missing_scope"
    assert "action" not in missing_scope

    owner_cancel = scoped_mapper.cancel_task(
        owner_task_id, correlation(18), scoped_tenant_a
    )
    owner_cancel_action = allowed_action(owner_cancel)
    assert owner_cancel_action["kind"] == "cancel_task"
    assert owner_cancel_action["task"]["state"] == "TASK_STATE_WORKING"
    assert owner_cancel_action["task"]["status_message"] == "cancellation requested"
    repeated_cancel = scoped_mapper.cancel_task(
        owner_task_id, correlation(19), scoped_tenant_a
    )
    assert allowed_action(repeated_cancel) == owner_cancel_action

    waiting_states = cast(
        "list[A2aTaskState]",
        [
            "TASK_STATE_WORKING",
            "TASK_STATE_INPUT_REQUIRED",
            "TASK_STATE_AUTH_REQUIRED",
        ],
    )
    for index, waiting_state in enumerate(waiting_states):
        fence_mapper = A2aMapper()
        fence_message = message(
            f"cancel-fence-initial-{index}", f"cancel-fence-context-{index}"
        )
        initial = fence_mapper.send_message(
            fence_message, correlation(20 + index * 10), tenant_a
        )
        initial_action = cast("A2aDispatchMessageAction", allowed_action(initial))
        if waiting_state != "TASK_STATE_WORKING":
            fence_mapper.transition_task(
                initial_action["mapping"]["task_id"],
                waiting_state,
                f"waiting-{index}",
            )
        assert (
            allowed_action(
                fence_mapper.cancel_task(
                    initial_action["mapping"]["task_id"],
                    correlation(22 + index * 10),
                    tenant_a,
                )
            )["kind"]
            == "cancel_task"
        )

        before_snapshot = cast(Any, fence_mapper.snapshot())
        before_bytes = serialized_state(fence_mapper)
        exact_retry = fence_mapper.send_message(
            fence_message, correlation(23 + index * 10), tenant_a
        )
        assert allowed_action(exact_retry)["kind"] == "duplicate_message"
        assert fence_mapper.snapshot() == before_snapshot
        assert serialized_state(fence_mapper) == before_bytes

        blocked = fence_mapper.send_message(
            targeted_message(
                f"cancel-fence-blocked-{index}", initial_action["mapping"]
            ),
            correlation(24 + index * 10),
            tenant_a,
        )
        blocked_auth = blocked["envelope"]["authorization"]
        assert blocked_auth["status"] == "denied"
        assert blocked_auth["code"] == "state_conflict"
        assert (
            blocked_auth["reason"]
            == "A2A task has an unsettled cancellation and cannot accept another message"
        )
        assert "action" not in blocked
        after_snapshot = cast(Any, fence_mapper.snapshot())
        assert after_snapshot == before_snapshot
        assert serialized_state(fence_mapper) == before_bytes
        assert len(after_snapshot["receipts"]) == len(before_snapshot["receipts"])
        assert len(after_snapshot["dispatch_outbox"]) == len(
            before_snapshot["dispatch_outbox"]
        )
        assert len(after_snapshot["pending_events"]) == len(
            before_snapshot["pending_events"]
        )
        assert after_snapshot["revision"] == before_snapshot["revision"]


main()
