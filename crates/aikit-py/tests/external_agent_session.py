"""Keyless live-session coverage: spawn a fake ACP agent and drive it through the binding."""

import asyncio
import os
import stat
import tempfile

from typing import TYPE_CHECKING, Any, Dict, List

from aikit import spawn_external_agent_session

FAKE_ACP_AGENT = r"""#!/bin/sh
while IFS= read -r line; do
  id=$(printf '%s' "$line" | sed -n 's/.*"id":"\([^"]*\)".*/\1/p')
  case "$line" in
    *'"method":"initialize"'*)
      printf '{"jsonrpc":"2.0","id":"%s","result":{"protocolVersion":1,"agentCapabilities":{},"authMethods":[],"agentInfo":{"name":"fake-acp","version":"9.9.9"}}}\n' "$id"
      ;;
    *'"method":"session/new"'*)
      printf '{"jsonrpc":"2.0","id":"%s","result":{"sessionId":"session-1"}}\n' "$id"
      ;;
    *'"method":"session/prompt"'*)
      printf '{"jsonrpc":"2.0","method":"session/update","params":{"sessionId":"session-1","update":{"sessionUpdate":"agent_message_chunk","content":{"type":"text","text":"hello from fake"}}}}\n'
      printf '{"jsonrpc":"2.0","id":"%s","result":{"stopReason":"end_turn"}}\n' "$id"
      ;;
  esac
done
"""


def expect_error(awaitable_factory: Any) -> str:
    async def run() -> str:
        try:
            await awaitable_factory()
        except Exception as error:  # noqa: BLE001 - message content is the assertion
            return str(error)
        raise AssertionError("expected the call to fail closed")

    return asyncio.run(run())


def main() -> None:
    with tempfile.TemporaryDirectory() as root:
        executable = os.path.join(root, "gemini")
        with open(executable, "w", encoding="utf-8") as handle:
            handle.write(FAKE_ACP_AGENT)
        os.chmod(executable, stat.S_IRWXU)

        # The Gemini vendor-terms gate fails closed before any process is spawned.
        gate = expect_error(
            lambda: spawn_external_agent_session("gemini", root, executable)
        )
        assert "account terms" in gate, gate

        async def drive() -> None:
            async def bounded(awaitable: Any) -> Any:
                return await asyncio.wait_for(awaitable, timeout=15)

            session = await bounded(
                spawn_external_agent_session(
                    "gemini",
                    root,
                    executable,
                    acknowledge_vendor_terms=True,
                )
            )
            events = session.events()
            descriptor: Dict[str, Any] = await bounded(session.initialize())
            assert descriptor["kind"] == "gemini"
            assert descriptor["version"] == "9.9.9"

            runtime_session: Dict[str, Any] = await bounded(
                session.new_session({"cwd": root, "additional_directories": []})
            )
            session_id = runtime_session["session_id"]
            assert session_id == "session-1"

            await bounded(
                session.prompt(
                    {
                        "session_id": session_id,
                        "prompt_id": "prompt-1",
                        "blocks": [{"type": "text", "text": "hi"}],
                    }
                )
            )

            seen: List[str] = []

            async def collect() -> None:
                async for event in events:
                    kind = event["event"]["type"]
                    seen.append(kind)
                    if kind == "completed":
                        return

            await asyncio.wait_for(collect(), timeout=15)
            assert "content_delta" in seen, seen
            await bounded(session.shutdown())

        asyncio.run(drive())
    print("external agent session Python contract: ok")


if TYPE_CHECKING:
    _ = List

if __name__ == "__main__":
    main()
