from __future__ import annotations

import json
import os
import urllib.error
import urllib.request
from dataclasses import dataclass
from typing import Protocol
from typing import Any

from analytics_assistant.config import settings


SYSTEM_PROMPT = (
    "You are an internal analytics assistant. Answer the user's business question "
    "using only the supplied evidence. Do not describe JSON, schemas, columns, "
    "or the data structure unless the user explicitly asks. Do not infer from "
    "outside knowledge. Cite source IDs like [D1] or [S2] for every factual claim. "
    "If the supplied evidence is insufficient, say exactly what is missing."
)


@dataclass(frozen=True)
class LlmSummary:
    text: str
    provider: str
    model: str | None
    used: bool
    warning: str | None = None


class LlmSummarizer(Protocol):
    def summarize(self, question: str, evidence: dict[str, Any], sources: list[dict[str, Any]]) -> LlmSummary:
        ...


def build_llm_summarizer(provider: str | None = None) -> LlmSummarizer:
    selected = (provider or settings.llm_provider).lower()
    if selected == "ollama":
        return OllamaLlmSummarizer()
    if selected == "openai":
        return OpenAILlmSummarizer()
    if selected in {"none", "disabled"}:
        return DisabledLlmSummarizer()
    return DisabledLlmSummarizer(warning=f"Unknown LLM_PROVIDER '{selected}'")


class DisabledLlmSummarizer:
    def __init__(self, warning: str = "LLM summarization disabled") -> None:
        self.warning = warning

    def summarize(self, question: str, evidence: dict[str, Any], sources: list[dict[str, Any]]) -> LlmSummary:
        return LlmSummary(
            text="",
            provider="disabled",
            model=None,
            used=False,
            warning=self.warning,
        )


class OpenAILlmSummarizer:
    def __init__(self, model: str | None = None) -> None:
        self.model = model or settings.openai_model

    def summarize(self, question: str, evidence: dict[str, Any], sources: list[dict[str, Any]]) -> LlmSummary:
        api_key = os.getenv("OPENAI_API_KEY")
        if not api_key:
            return LlmSummary(
                text="",
                provider="openai",
                model=self.model,
                used=False,
                warning="OPENAI_API_KEY is not set",
            )

        try:
            from openai import OpenAI  # type: ignore
        except ModuleNotFoundError:
            return LlmSummary(
                text="",
                provider="openai",
                model=self.model,
                used=False,
                warning="openai package is not installed",
            )

        client = OpenAI(api_key=api_key)
        prompt = _build_prompt(question, evidence, sources)
        try:
            response = client.responses.create(
                model=self.model,
                input=[
                    {
                        "role": "system",
                        "content": SYSTEM_PROMPT,
                    },
                    {"role": "user", "content": prompt},
                ],
                max_output_tokens=settings.llm_max_output_tokens,
            )
        except Exception as exc:
            return LlmSummary(
                text="",
                provider="openai",
                model=self.model,
                used=False,
                warning=f"OpenAI request failed: {type(exc).__name__}: {exc}",
            )

        return LlmSummary(
            text=response.output_text.strip(),
            provider="openai",
            model=self.model,
            used=True,
        )


class OllamaLlmSummarizer:
    def __init__(self, model: str | None = None, base_url: str | None = None) -> None:
        self.model = model or settings.ollama_model
        self.base_url = (base_url or settings.ollama_url).rstrip("/")

    def summarize(self, question: str, evidence: dict[str, Any], sources: list[dict[str, Any]]) -> LlmSummary:
        prompt = _build_prompt(question, evidence, sources)
        payload = {
            "model": self.model,
            "stream": False,
            "messages": [
                {
                    "role": "system",
                    "content": SYSTEM_PROMPT,
                },
                {"role": "user", "content": prompt},
            ],
            "options": {
                "temperature": 0.2,
                "num_predict": settings.llm_max_output_tokens,
            },
        }

        request = urllib.request.Request(
            f"{self.base_url}/api/chat",
            data=json.dumps(payload).encode("utf-8"),
            headers={"Content-Type": "application/json"},
            method="POST",
        )
        try:
            with urllib.request.urlopen(request, timeout=180) as response:
                response_payload = json.loads(response.read().decode("utf-8"))
        except urllib.error.HTTPError as exc:
            body = exc.read().decode("utf-8", errors="replace")
            return LlmSummary(
                text="",
                provider="ollama",
                model=self.model,
                used=False,
                warning=f"Ollama request failed: HTTP {exc.code}: {body}",
            )
        except Exception as exc:
            return LlmSummary(
                text="",
                provider="ollama",
                model=self.model,
                used=False,
                warning=f"Ollama request failed: {type(exc).__name__}: {exc}",
            )

        message = response_payload.get("message") or {}
        text = str(message.get("content") or "").strip()
        if not text:
            return LlmSummary(
                text="",
                provider="ollama",
                model=self.model,
                used=False,
                warning=f"Ollama returned no message content: {response_payload}",
            )

        return LlmSummary(
            text=text,
            provider="ollama",
            model=self.model,
            used=True,
        )


def _build_prompt(question: str, evidence: dict[str, Any], sources: list[dict[str, Any]]) -> str:
    lines = [
        "TASK",
        "Answer the question below from the evidence. Do not summarize the format of the evidence.",
        "",
        f"QUESTION: {question}",
        "",
        "DOCUMENT EVIDENCE",
    ]

    document_chunks = evidence.get("documents", [])
    if document_chunks:
        for index, chunk in enumerate(document_chunks, start=1):
            page = f", page {chunk.get('page')}" if chunk.get("page") else ""
            lines.extend(
                [
                    f"[D{index}] {chunk.get('document')}{page}",
                    _shorten(chunk.get("text", ""), 700),
                    "",
                ]
            )
    else:
        lines.extend(["No document evidence was found.", ""])

    lines.append("SPREADSHEET EVIDENCE")
    spreadsheet_rows = _compact_spreadsheet_evidence(evidence)
    if spreadsheet_rows:
        for row in spreadsheet_rows:
            lines.append(f"[{row['source_id']}] {row['file']}: {_format_compact_row(row['row'])}")
    else:
        lines.append("No spreadsheet evidence was found.")

    analysis_rows = _compact_spreadsheet_analysis(evidence)
    if analysis_rows:
        lines.extend(["", "SPREADSHEET ANALYSIS"])
        for row in analysis_rows:
            lines.append(
                f"[{row['source_id']}] {row['file']} ranked by {row.get('rank_by')}: "
                f"{row.get('rank_value')} votes={row.get('vote_count')} -> "
                f"{_format_compact_row(row['row'])}"
            )

    if evidence.get("warnings"):
        lines.extend(["", "WARNINGS"])
        lines.extend(f"- {warning}" for warning in evidence["warnings"])

    lines.extend(
        [
            "",
            "RESPONSE FORMAT",
            "Answer: 2-5 sentences that directly answer the question.",
            "Evidence: 2-4 bullets with citations.",
            "Sources used: cite the document/spreadsheet IDs used.",
            "Caveats: one sentence on missing or weak evidence.",
        ]
    )
    return "\n".join(lines)


def _compact_spreadsheet_evidence(evidence: dict[str, Any]) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    source_index = 1
    for sheet in evidence.get("spreadsheets", []):
        for match in sheet.get("matched_rows", [])[:5]:
            rows.append(
                {
                    "source_id": f"S{source_index}",
                    "file": sheet["file"],
                    "score": match["score"],
                    "row": _compact_row(match["row"]),
                }
            )
            source_index += 1
    return rows[:12]


def _compact_spreadsheet_analysis(evidence: dict[str, Any]) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    source_index = 1
    for analysis in evidence.get("spreadsheet_analysis", []):
        ranked = analysis.get("filter_and_rank", {})
        for match in ranked.get("ranked_rows", [])[:4]:
            rows.append(
                {
                    "source_id": f"A{source_index}",
                    "file": analysis["file"],
                    "rank_by": match.get("rank_by"),
                    "rank_value": match.get("rank_value"),
                    "vote_count": match.get("vote_count"),
                    "row": match.get("row", {}),
                }
            )
            source_index += 1
    return rows[:10]


def _format_compact_row(row: dict[str, Any]) -> str:
    cells = [f"{key}={value}" for key, value in row.items()]
    return _shorten("; ".join(cells), 420)


def _compact_row(row: dict[str, Any]) -> dict[str, Any]:
    preferred_terms = (
        "title",
        "release",
        "genre",
        "rating",
        "vote",
        "revenue",
        "worldwide",
        "domestic",
        "foreign",
        "budget",
        "gross",
        "description",
        "outcome",
        "year",
        "rank",
    )
    compact: dict[str, Any] = {}
    for key, value in row.items():
        if value in (None, ""):
            continue
        key_lower = key.lower()
        if any(term in key_lower for term in preferred_terms):
            compact[key] = _shorten(str(value), 240)
    if compact:
        return compact

    for key, value in list(row.items())[:8]:
        if value not in (None, ""):
            compact[key] = _shorten(str(value), 160)
    return compact


def _shorten(text: str, limit: int) -> str:
    normalized = " ".join(str(text).split())
    if len(normalized) <= limit:
        return normalized
    return normalized[: limit - 3].rstrip() + "..."
