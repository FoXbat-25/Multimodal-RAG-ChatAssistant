from __future__ import annotations

import argparse
import json
import sys
import urllib.request

from analytics_assistant.registry import build_gateway
from analytics_assistant.config import settings


def main() -> None:
    if hasattr(sys.stdout, "reconfigure"):
        sys.stdout.reconfigure(encoding="utf-8", errors="replace")

    parser = argparse.ArgumentParser(description="Call analytics assistant backend tools.")
    subparsers = parser.add_subparsers(dest="command", required=True)

    subparsers.add_parser("llm-health")

    doc_index_parser = subparsers.add_parser("build-doc-index")
    doc_index_parser.add_argument("--document-dir")
    doc_index_parser.add_argument("--index-path")
    doc_index_parser.add_argument("--manifest-path")
    doc_index_parser.add_argument("--disable-ocr", action="store_true")
    doc_index_parser.add_argument("--disable-embeddings", action="store_true")
    doc_index_parser.add_argument("--embedding-model")
    doc_index_parser.add_argument("--max-chunks", type=int)

    retrieve_parser = subparsers.add_parser("retrieve-docs")
    retrieve_parser.add_argument("query")
    retrieve_parser.add_argument("--top-k", type=int, default=5)
    retrieve_parser.add_argument("--retrieval-mode", default="auto", choices=["auto", "semantic", "keyword"])

    sql_parser = subparsers.add_parser("sql")
    sql_parser.add_argument("sql")
    sql_parser.add_argument("--database-path")
    sql_parser.add_argument("--row-limit", type=int)

    subparsers.add_parser("list-sheets")

    sheet_parser = subparsers.add_parser("sheet")
    sheet_parser.add_argument("file_name")
    sheet_parser.add_argument("--operation", default="describe")
    sheet_parser.add_argument("--sheet")
    sheet_parser.add_argument("--group-by")
    sheet_parser.add_argument("--metric")
    sheet_parser.add_argument("--aggregation", default="sum")
    sheet_parser.add_argument("--query")
    sheet_parser.add_argument("--max-rows", type=int, default=10)
    sheet_parser.add_argument("--rank-by")
    sheet_parser.add_argument("--sort-order", default="auto")
    sheet_parser.add_argument("--min-vote-count", type=float)

    args = parser.parse_args()
    gateway = build_gateway()

    if args.command == "build-doc-index":
        payload = gateway.call(
            "build_document_index",
            document_dir=args.document_dir,
            index_path=args.index_path,
            manifest_path=args.manifest_path,
            enable_ocr=not args.disable_ocr,
            enable_embeddings=not args.disable_embeddings,
            embedding_model=args.embedding_model,
            max_chunks=args.max_chunks,
        )
    elif args.command == "retrieve-docs":
        payload = gateway.call(
            "retrieve_documents",
            query=args.query,
            top_k=args.top_k,
            retrieval_mode=args.retrieval_mode,
        )
    elif args.command == "sql":
        payload = gateway.call(
            "secure_sql_query",
            sql=args.sql,
            database_path=args.database_path,
            row_limit=args.row_limit,
        )
    elif args.command == "list-sheets":
        payload = gateway.call("list_spreadsheets")
    elif args.command == "sheet":
        payload = gateway.call(
            "analyze_spreadsheet",
            file_name=args.file_name,
            operation=args.operation,
            sheet=args.sheet,
            group_by=args.group_by,
            metric=args.metric,
            aggregation=args.aggregation,
            query=args.query,
            max_rows=args.max_rows,
            rank_by=args.rank_by,
            sort_order=args.sort_order,
            min_vote_count=args.min_vote_count,
        )
    elif args.command == "llm-health":
        payload = _llm_health()
    else:
        raise ValueError(f"Unhandled command: {args.command}")

    print(json.dumps(payload, indent=2, ensure_ascii=False))


def _llm_health() -> dict[str, object]:
    if settings.llm_provider.lower() == "ollama":
        try:
            with urllib.request.urlopen(f"{settings.ollama_url.rstrip('/')}/api/tags", timeout=5) as response:
                ollama_payload = json.loads(response.read().decode("utf-8"))
        except Exception as exc:
            return {
                "provider": "ollama",
                "ok": False,
                "url": settings.ollama_url,
                "model": settings.ollama_model,
                "error": f"{type(exc).__name__}: {exc}",
            }
        return {
            "provider": "ollama",
            "ok": True,
            "url": settings.ollama_url,
            "model": settings.ollama_model,
            "available_models": [item.get("name") for item in ollama_payload.get("models", [])],
        }

    return {
        "provider": settings.llm_provider,
        "ok": bool(settings.llm_provider.lower() == "openai"),
        "model": settings.openai_model,
        "needs_openai_api_key": settings.llm_provider.lower() == "openai",
    }


if __name__ == "__main__":
    main()
