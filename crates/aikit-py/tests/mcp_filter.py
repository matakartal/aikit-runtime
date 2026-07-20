"""Offline FFI checks for fail-closed MCP tool-filter configuration."""

import asyncio

import aikit
from aikit import connect_mcp_http, connect_mcp_stdio, legacy


async def expect_value_error(awaitable_factory, expected: str) -> None:
    try:
        await awaitable_factory()
    except ValueError as error:
        assert expected in str(error), error
    else:
        raise AssertionError("invalid MCP tool filter was accepted")


async def main() -> None:
    assert not hasattr(aikit, "McpServer")
    assert hasattr(legacy.McpServer, "list_resources")
    try:
        aikit.McpConnection()
    except TypeError:
        pass
    else:
        raise AssertionError("MCP connection unexpectedly exposed a public constructor")
    await expect_value_error(
        lambda: connect_mcp_http(
            "http://127.0.0.1:1/mcp",
            "bad",
            tool_filter={"unexpected": []},
        ),
        "MCP tool filter contains an unknown field",
    )
    await expect_value_error(
        lambda: connect_mcp_stdio(
            "missing-mcp-server",
            [],
            "bad",
            tool_filter={"deny": ["hidden", "hidden"]},
        ),
        "duplicate name",
    )
    await expect_value_error(
        lambda: connect_mcp_http(
            "http://127.0.0.1:1/mcp",
            "bad",
            tool_filter={"allow": None},
        ),
        "must be an array of strings",
    )


asyncio.run(main())
