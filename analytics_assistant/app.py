from __future__ import annotations

from typing import Any

try:
    from fastapi import FastAPI
except ModuleNotFoundError as exc:  # pragma: no cover
    raise RuntimeError(
        "FastAPI is not installed. Install dependencies with `pip install -r requirements.txt`."
    ) from exc

from analytics_assistant.registry import build_gateway


app = FastAPI(title="Internal Analytics Assistant Tool API")
gateway = build_gateway()


@app.get("/health")
def health() -> dict[str, str]:
    return {"status": "ok"}


@app.post("/tools/{tool_name}")
def call_tool(tool_name: str, payload: dict[str, Any]) -> dict[str, Any]:
    return gateway.call(tool_name, **payload)

