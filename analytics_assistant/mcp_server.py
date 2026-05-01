from __future__ import annotations

import json
import sys
import traceback
from typing import Any

from analytics_assistant.orchestrator import AnalyticsOrchestrator
from analytics_assistant.registry import build_gateway


PROTOCOL_VERSION = "2024-11-05"


def _tool_definitions() -> list[dict[str, Any]]:
    return [
        {
            "name": "ask_analytics_assistant",
            "description": "Ask a natural-language analytics question. Returns an answer with source citations.",
            "inputSchema": {
                "type": "object",
                "properties": {
                    "question": {"type": "string"},
                    "top_k": {"type": "integer", "default": 5},
                    "use_llm": {"type": "boolean", "default": True},
                },
                "required": ["question"],
            },
        },
        {
            "name": "build_document_index",
            "description": "Index PDFs, Markdown, and text files from the configured document directory.",
            "inputSchema": {
                "type": "object",
                "properties": {
                    "document_dir": {"type": "string"},
                    "index_path": {"type": "string"},
                    "manifest_path": {"type": "string"},
                    "enable_ocr": {"type": "boolean", "default": True},
                    "enable_embeddings": {"type": "boolean", "default": True},
                    "embedding_model": {"type": "string"},
                    "max_chunks": {"type": "integer"},
                },
            },
        },
        {
            "name": "retrieve_documents",
            "description": "Retrieve relevant chunks from the document index.",
            "inputSchema": {
                "type": "object",
                "properties": {
                    "query": {"type": "string"},
                    "top_k": {"type": "integer", "default": 5},
                    "index_path": {"type": "string"},
                    "retrieval_mode": {
                        "type": "string",
                        "enum": ["auto", "semantic", "keyword"],
                        "default": "auto",
                    },
                },
                "required": ["query"],
            },
        },
        {
            "name": "list_spreadsheets",
            "description": "List CSV/XLSX files available to the spreadsheet tool.",
            "inputSchema": {"type": "object", "properties": {}},
        },
        {
            "name": "analyze_spreadsheet",
            "description": "Describe, search, aggregate, profile, or rank spreadsheet rows.",
            "inputSchema": {
                "type": "object",
                "properties": {
                    "file_name": {"type": "string"},
                    "operation": {
                        "type": "string",
                        "enum": ["describe", "auto_profile", "group_by", "search", "filter_and_rank"],
                    },
                    "sheet": {"type": "string"},
                    "group_by": {"type": "string"},
                    "metric": {"type": "string"},
                    "aggregation": {"type": "string", "default": "sum"},
                    "query": {"type": "string"},
                    "max_rows": {"type": "integer", "default": 10},
                    "rank_by": {"type": "string"},
                    "sort_order": {"type": "string", "enum": ["auto", "asc", "desc"], "default": "auto"},
                    "min_vote_count": {"type": "number"},
                },
                "required": ["file_name"],
            },
        },
        {
            "name": "secure_sql_query",
            "description": "Run a validated read-only SQL query against the configured SQLite database.",
            "inputSchema": {
                "type": "object",
                "properties": {
                    "sql": {"type": "string"},
                    "database_path": {"type": "string"},
                    "row_limit": {"type": "integer"},
                },
                "required": ["sql"],
            },
        },
    ]


class McpServer:
    def __init__(self) -> None:
        self.gateway = build_gateway()
        self.orchestrator = AnalyticsOrchestrator(gateway=self.gateway)

    def handle(self, request: dict[str, Any]) -> dict[str, Any] | None:
        method = request.get("method")
        request_id = request.get("id")

        if method == "notifications/initialized":
            return None

        try:
            if method == "initialize":
                result = {
                    "protocolVersion": PROTOCOL_VERSION,
                    "capabilities": {"tools": {}},
                    "serverInfo": {
                        "name": "internal-analytics-assistant",
                        "version": "0.1.0",
                    },
                }
            elif method == "tools/list":
                result = {"tools": _tool_definitions()}
            elif method == "tools/call":
                result = self._call_tool(request.get("params", {}))
            else:
                return _error_response(request_id, -32601, f"Method not found: {method}")
        except Exception as exc:
            return _error_response(
                request_id,
                -32000,
                f"{type(exc).__name__}: {exc}",
                {"traceback": traceback.format_exc()},
            )

        return {"jsonrpc": "2.0", "id": request_id, "result": result}

    def _call_tool(self, params: dict[str, Any]) -> dict[str, Any]:
        name = params.get("name")
        arguments = params.get("arguments") or {}
        if name == "ask_analytics_assistant":
            payload = self.orchestrator.answer(
                arguments["question"],
                top_k=arguments.get("top_k", 5),
                use_llm=arguments.get("use_llm", True),
            )
        else:
            payload = self.gateway.call(name, **arguments)

        return {
            "content": [
                {
                    "type": "text",
                    "text": json.dumps(payload, indent=2, ensure_ascii=False),
                }
            ],
            "structuredContent": payload,
        }


def _error_response(
    request_id: Any,
    code: int,
    message: str,
    data: dict[str, Any] | None = None,
) -> dict[str, Any]:
    error: dict[str, Any] = {"code": code, "message": message}
    if data is not None:
        error["data"] = data
    return {"jsonrpc": "2.0", "id": request_id, "error": error}


def main() -> None:
    if hasattr(sys.stdout, "reconfigure"):
        sys.stdout.reconfigure(encoding="utf-8", errors="replace")
    if hasattr(sys.stdin, "reconfigure"):
        sys.stdin.reconfigure(encoding="utf-8", errors="replace")

    server = McpServer()
    for line in sys.stdin:
        if not line.strip():
            continue
        request = json.loads(line)
        response = server.handle(request)
        if response is not None:
            sys.stdout.write(json.dumps(response, ensure_ascii=False) + "\n")
            sys.stdout.flush()


if __name__ == "__main__":
    main()
