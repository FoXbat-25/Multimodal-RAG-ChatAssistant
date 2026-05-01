from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
import re
from typing import Any

from analytics_assistant.config import settings
from analytics_assistant.llm_summarizer import LlmSummarizer, build_llm_summarizer
from analytics_assistant.registry import build_gateway
from analytics_assistant.tool_gateway import ToolGateway


EXPLANATION_TERMS = {
    "cause",
    "caused",
    "explain",
    "failed",
    "fails",
    "failure",
    "reason",
    "reasons",
    "risk",
    "why",
}


@dataclass(frozen=True)
class RouteDecision:
    use_documents: bool
    use_spreadsheets: bool
    use_sql: bool
    reasons: list[str]


class AnalyticsOrchestrator:
    """Coordinates tool calls and produces an evidence-first answer.

    This is intentionally deterministic for the MVP. The LLM can be added later
    as the final summarizer, while routing and data access remain behind tools.
    """

    def __init__(
        self,
        gateway: ToolGateway | None = None,
        *,
        document_index_path: str | None = None,
        llm_summarizer: LlmSummarizer | None = None,
    ) -> None:
        self.gateway = gateway or build_gateway()
        self.document_index_path = Path(document_index_path) if document_index_path else settings.document_index_path
        self.llm_summarizer = llm_summarizer or build_llm_summarizer()

    def answer(self, question: str, *, top_k: int = 5, use_llm: bool = True) -> dict[str, Any]:
        if not question.strip():
            raise ValueError("Question is empty.")

        route = self._route(question)
        evidence: dict[str, Any] = {
            "documents": [],
            "spreadsheets": [],
            "spreadsheet_analysis": [],
            "sql": [],
            "warnings": [],
        }

        if route.use_documents:
            self._collect_document_evidence(question, top_k, evidence)

        if route.use_spreadsheets:
            self._collect_spreadsheet_evidence(question, evidence)

        sources = self._collect_sources(evidence)
        fallback_answer = self._compose_answer(question, route, evidence)
        llm_metadata: dict[str, Any] = {"used": False}
        answer_text = fallback_answer

        if use_llm:
            llm_summary = self.llm_summarizer.summarize(question, evidence, sources)
            llm_metadata = {
                "used": llm_summary.used,
                "provider": llm_summary.provider,
                "model": llm_summary.model,
                "warning": llm_summary.warning,
            }
            if llm_summary.used and llm_summary.text:
                answer_text = llm_summary.text
            elif llm_summary.warning:
                evidence["warnings"].append(f"llm_summary_unavailable:{llm_summary.warning}")
                answer_text = self._compose_answer(question, route, evidence)

        grounding = _validate_citations(answer_text, sources)
        if grounding["invalid_citations"]:
            evidence["warnings"].append(
                "invalid_llm_citations:" + ",".join(grounding["invalid_citations"])
            )
            answer_text = f"{answer_text}\n\nCitation warning: some cited source IDs were not found."

        answer_text = _append_source_legend(answer_text, sources)

        return {
            "question": question,
            "answer": answer_text,
            "fallback_answer": fallback_answer,
            "llm": llm_metadata,
            "grounding": grounding,
            "route": {
                "use_documents": route.use_documents,
                "use_spreadsheets": route.use_spreadsheets,
                "use_sql": route.use_sql,
                "reasons": route.reasons,
            },
            "evidence": evidence,
            "sources": sources,
        }

    def _route(self, question: str) -> RouteDecision:
        lowered = question.lower()
        reasons: list[str] = []
        use_documents = True
        use_spreadsheets = True
        use_sql = any(term in lowered for term in ["sql", "database", "table"])

        if any(term in lowered for term in EXPLANATION_TERMS):
            reasons.append("explanation_question_document_evidence_needed")
        else:
            reasons.append("general_question_document_search_default")

        reasons.append("spreadsheet_search_checks_structured_files_for_matching_rows")

        if use_sql:
            reasons.append("sql_terms_detected_but_auto_sql_generation_not_enabled")

        return RouteDecision(
            use_documents=use_documents,
            use_spreadsheets=use_spreadsheets,
            use_sql=False,
            reasons=reasons,
        )

    def _collect_document_evidence(
        self,
        question: str,
        top_k: int,
        evidence: dict[str, Any],
    ) -> None:
        try:
            response = self.gateway.call(
                "retrieve_documents",
                query=question,
                top_k=top_k,
                index_path=str(self.document_index_path),
                retrieval_mode="auto",
            )
        except FileNotFoundError:
            evidence["warnings"].append(
                f"document_index_missing_run_build_doc_index:{self.document_index_path}"
            )
            return

        evidence["documents"] = response["data"].get("chunks", [])

    def _collect_spreadsheet_evidence(self, question: str, evidence: dict[str, Any]) -> None:
        files_response = self.gateway.call("list_spreadsheets")
        files = files_response["data"].get("files", [])
        if not files:
            evidence["warnings"].append("no_spreadsheets_found")
            return

        spreadsheet_hits: list[dict[str, Any]] = []
        spreadsheet_analysis: list[dict[str, Any]] = []
        for file_name in files:
            try:
                search_response = self.gateway.call(
                    "analyze_spreadsheet",
                    file_name=file_name,
                    operation="search",
                    query=question,
                    max_rows=5,
                )
            except ValueError as exc:
                evidence["warnings"].append(f"spreadsheet_search_skipped:{file_name}:{exc}")
                continue

            matched_rows = search_response["data"].get("matched_rows", [])
            spreadsheet_hits.append(
                {
                    "file": file_name,
                    "matched_rows": matched_rows,
                    "searched_columns": search_response["data"].get("searched_columns", []),
                }
            )

            try:
                profile_response = self.gateway.call(
                    "analyze_spreadsheet",
                    file_name=file_name,
                    operation="auto_profile",
                )
                ranked_response = self.gateway.call(
                    "analyze_spreadsheet",
                    file_name=file_name,
                    operation="filter_and_rank",
                    query=question,
                    max_rows=5,
                )
            except ValueError as exc:
                evidence["warnings"].append(f"spreadsheet_analysis_skipped:{file_name}:{exc}")
                continue

            spreadsheet_analysis.append(
                {
                    "file": file_name,
                    "profile": {
                        "row_count": profile_response["data"].get("row_count"),
                        "inferred_roles": profile_response["data"].get("inferred_roles", {}),
                        "numeric_columns": profile_response["data"].get("numeric_columns", {}),
                    },
                    "filter_and_rank": ranked_response["data"],
                }
            )

        evidence["spreadsheets"] = spreadsheet_hits
        evidence["spreadsheet_analysis"] = spreadsheet_analysis

    def _collect_sources(self, evidence: dict[str, Any]) -> list[dict[str, Any]]:
        sources: list[dict[str, Any]] = []
        for index, chunk in enumerate(evidence["documents"], start=1):
            sources.append(
                {
                    "id": f"D{index}",
                    "type": "document",
                    "name": chunk["document"],
                    "page": chunk["page"],
                    "chunk_id": chunk["chunk_id"],
                    "score": chunk["score"],
                    "extraction_method": chunk.get("extraction_method"),
                }
            )

        source_index = 1
        for sheet in evidence["spreadsheets"]:
            for row in sheet["matched_rows"]:
                sources.append(
                    {
                        "id": f"S{source_index}",
                        "type": "spreadsheet",
                        "name": sheet["file"],
                        "score": row["score"],
                        "columns": sheet["searched_columns"],
                        "row": _compact_source_row(row.get("row", {})),
                    }
                )
                source_index += 1

        analysis_source_index = 1
        for analysis in evidence.get("spreadsheet_analysis", []):
            for row in analysis.get("filter_and_rank", {}).get("ranked_rows", []):
                sources.append(
                    {
                        "id": f"A{analysis_source_index}",
                        "type": "spreadsheet_analysis",
                        "name": analysis["file"],
                        "rank_by": row.get("rank_by"),
                        "rank_value": row.get("rank_value"),
                        "vote_count": row.get("vote_count"),
                        "match_score": row.get("match_score"),
                        "row": _compact_source_row(row.get("row", {})),
                    }
                )
                analysis_source_index += 1

        return sources

    def _compose_answer(
        self,
        question: str,
        route: RouteDecision,
        evidence: dict[str, Any],
    ) -> str:
        lines = [
            f"Question: {question}",
            "",
            "Evidence summary:",
        ]

        if not evidence["documents"] and not any(
            sheet["matched_rows"] for sheet in evidence["spreadsheets"]
        ):
            lines.extend(
                [
                    "I could not find enough indexed evidence to answer this from the local sources.",
                    "Add relevant PDFs/CSVs, run build-doc-index again, and retry the question.",
                ]
            )
        else:
            if evidence["documents"]:
                lines.append("Document evidence:")
                for index, chunk in enumerate(evidence["documents"], start=1):
                    snippet = _shorten(chunk["text"])
                    page = f", page {chunk['page']}" if chunk.get("page") else ""
                    lines.append(f"[D{index}] {chunk['document']}{page}: {snippet}")

            spreadsheet_source_index = 1
            spreadsheet_lines: list[str] = []
            for sheet in evidence["spreadsheets"]:
                for row in sheet["matched_rows"]:
                    spreadsheet_lines.append(
                        f"[S{spreadsheet_source_index}] {sheet['file']}: {_format_row(row['row'])}"
                    )
                    spreadsheet_source_index += 1
            if spreadsheet_lines:
                lines.append("Spreadsheet evidence:")
                lines.extend(spreadsheet_lines[:8])
                if len(spreadsheet_lines) > 8:
                    lines.append(f"...and {len(spreadsheet_lines) - 8} more spreadsheet matches.")

            analysis_lines = []
            for analysis in evidence.get("spreadsheet_analysis", []):
                ranked = analysis.get("filter_and_rank", {})
                for row in ranked.get("ranked_rows", [])[:3]:
                    analysis_lines.append(
                        f"{analysis['file']} ranked by {row.get('rank_by')}: "
                        f"{row.get('rank_value')} -> {_format_row(row.get('row', {}))}"
                    )
            if analysis_lines:
                lines.append("Spreadsheet analysis:")
                lines.extend(analysis_lines[:6])

        if evidence["warnings"]:
            lines.append("Warnings:")
            lines.extend(f"- {warning}" for warning in evidence["warnings"])

        lines.append("Routing:")
        lines.extend(f"- {reason}" for reason in route.reasons)
        return "\n".join(lines)


def _shorten(text: str, limit: int = 320) -> str:
    normalized = " ".join(text.split())
    if len(normalized) <= limit:
        return normalized
    return normalized[: limit - 3].rstrip() + "..."


def _format_row(row: dict[str, Any], limit: int = 320) -> str:
    cells = [
        f"{key}={value}"
        for key, value in row.items()
        if value not in (None, "")
    ]
    return _shorten("; ".join(cells), limit=limit)


def _compact_source_row(row: dict[str, Any]) -> dict[str, Any]:
    preferred_terms = (
        "title",
        "release",
        "genre",
        "rating",
        "vote",
        "budget",
        "gross",
        "worldwide",
        "domestic",
        "foreign",
        "revenue",
        "outcome",
        "description",
        "year",
        "rank",
    )
    compact: dict[str, Any] = {}
    for key, value in row.items():
        if value in (None, ""):
            continue
        if any(term in key.lower() for term in preferred_terms):
            compact[key] = value
        if len(compact) >= 10:
            break
    return compact or {key: value for key, value in list(row.items())[:6] if value not in (None, "")}


def _citation_ids(text: str) -> list[str]:
    seen: set[str] = set()
    citations: list[str] = []
    for match in re.finditer(r"\[([DSA]\d+)\]", text):
        citation = match.group(1)
        if citation not in seen:
            seen.add(citation)
            citations.append(citation)
    return citations


def _validate_citations(answer_text: str, sources: list[dict[str, Any]]) -> dict[str, Any]:
    cited_ids = _citation_ids(answer_text)
    valid_ids = {source["id"] for source in sources}
    invalid = [citation for citation in cited_ids if citation not in valid_ids]
    return {
        "cited_source_ids": cited_ids,
        "invalid_citations": invalid,
        "valid_source_ids": sorted(valid_ids),
        "has_citations": bool(cited_ids),
    }


def _append_source_legend(answer_text: str, sources: list[dict[str, Any]]) -> str:
    if not sources:
        return answer_text

    source_by_id = {source["id"]: source for source in sources}
    cited_ids = _citation_ids(answer_text)
    legend_ids = [source_id for source_id in cited_ids if source_id in source_by_id]
    if not legend_ids:
        legend_ids = [source["id"] for source in sources[:10]]

    lines = [answer_text.rstrip(), "", "Source legend:"]
    for source_id in legend_ids[:12]:
        lines.append(_format_source_legend(source_by_id[source_id]))
    if len(legend_ids) > 12:
        lines.append(f"...and {len(legend_ids) - 12} more cited sources.")
    return "\n".join(lines)


def _format_source_legend(source: dict[str, Any]) -> str:
    source_type = source.get("type")
    if source_type == "document":
        page = f", page {source.get('page')}" if source.get("page") else ""
        return f"[{source['id']}] document: {source.get('name')}{page}"

    row = source.get("row") or {}
    row_summary = _format_row(row, limit=180) if row else ""
    if source_type == "spreadsheet_analysis":
        rank = source.get("rank_by")
        value = source.get("rank_value")
        votes = source.get("vote_count")
        suffix = f" | {row_summary}" if row_summary else ""
        return (
            f"[{source['id']}] analysis: {source.get('name')} | "
            f"ranked by {rank}={value} | votes={votes}{suffix}"
        )

    if source_type == "spreadsheet":
        suffix = f" | {row_summary}" if row_summary else ""
        return f"[{source['id']}] spreadsheet row: {source.get('name')}{suffix}"

    return f"[{source['id']}] {source_type}: {source.get('name')}"
