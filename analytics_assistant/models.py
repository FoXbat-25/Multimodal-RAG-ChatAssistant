from __future__ import annotations

from dataclasses import asdict, dataclass, field
from typing import Any, Mapping


@dataclass(frozen=True)
class Source:
    type: str
    name: str
    details: Mapping[str, Any] = field(default_factory=dict)

    def as_dict(self) -> dict[str, Any]:
        return asdict(self)


@dataclass(frozen=True)
class ToolResponse:
    data: Any
    sources: list[Source]
    explainability: Mapping[str, Any] = field(default_factory=dict)

    def as_dict(self) -> dict[str, Any]:
        return {
            "data": self.data,
            "sources": [source.as_dict() for source in self.sources],
            "explainability": dict(self.explainability),
        }

