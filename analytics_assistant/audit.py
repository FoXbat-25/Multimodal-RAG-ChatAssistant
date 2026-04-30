from __future__ import annotations

import json
from dataclasses import asdict, dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Mapping


@dataclass(frozen=True)
class AuditEvent:
    timestamp: str
    tool_name: str
    action: str
    request: Mapping[str, Any]
    sources: list[Mapping[str, Any]]
    status: str


class JsonlAuditLogger:
    def __init__(self, path: Path) -> None:
        self.path = path
        self.path.parent.mkdir(parents=True, exist_ok=True)

    def record(
        self,
        *,
        tool_name: str,
        action: str,
        request: Mapping[str, Any],
        sources: list[Mapping[str, Any]],
        status: str,
    ) -> None:
        event = AuditEvent(
            timestamp=datetime.now(timezone.utc).isoformat(),
            tool_name=tool_name,
            action=action,
            request=request,
            sources=sources,
            status=status,
        )
        with self.path.open("a", encoding="utf-8") as audit_file:
            audit_file.write(json.dumps(asdict(event), ensure_ascii=False) + "\n")

