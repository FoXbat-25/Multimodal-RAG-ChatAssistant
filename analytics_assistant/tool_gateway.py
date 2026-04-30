from __future__ import annotations

from collections.abc import Callable
from typing import Any

from analytics_assistant.audit import JsonlAuditLogger
from analytics_assistant.config import settings
from analytics_assistant.models import ToolResponse


ToolCallable = Callable[..., ToolResponse]


class ToolGateway:
    """Single entry point for assistant-visible tools.

    The LLM should call only this gateway or an MCP wrapper around it. Data tools
    stay behind this boundary so validation, logging, and provenance remain
    consistent.
    """

    def __init__(self, audit_logger: JsonlAuditLogger | None = None) -> None:
        self._tools: dict[str, ToolCallable] = {}
        self.audit_logger = audit_logger or JsonlAuditLogger(settings.audit_log_path)

    def register(self, name: str, tool: ToolCallable) -> None:
        if name in self._tools:
            raise ValueError(f"Tool already registered: {name}")
        self._tools[name] = tool

    def call(self, name: str, **kwargs: Any) -> dict[str, Any]:
        if name not in self._tools:
            raise ValueError(f"Unknown tool: {name}")

        try:
            response = self._tools[name](**kwargs)
        except Exception:
            self.audit_logger.record(
                tool_name=name,
                action="call",
                request=kwargs,
                sources=[],
                status="error",
            )
            raise

        payload = response.as_dict()
        self.audit_logger.record(
            tool_name=name,
            action="call",
            request=kwargs,
            sources=payload["sources"],
            status="ok",
        )
        return payload

