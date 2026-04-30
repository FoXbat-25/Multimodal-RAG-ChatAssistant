from __future__ import annotations

import argparse
import json

from analytics_assistant.registry import build_gateway


def main() -> None:
    parser = argparse.ArgumentParser(description="Call analytics assistant backend tools.")
    subparsers = parser.add_subparsers(dest="command", required=True)

    doc_index_parser = subparsers.add_parser("build-doc-index")
    doc_index_parser.add_argument("--document-dir")
    doc_index_parser.add_argument("--index-path")
    doc_index_parser.add_argument("--manifest-path")
    doc_index_parser.add_argument("--disable-ocr", action="store_true")

    retrieve_parser = subparsers.add_parser("retrieve-docs")
    retrieve_parser.add_argument("query")
    retrieve_parser.add_argument("--top-k", type=int, default=5)

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

    args = parser.parse_args()
    gateway = build_gateway()

    if args.command == "build-doc-index":
        payload = gateway.call(
            "build_document_index",
            document_dir=args.document_dir,
            index_path=args.index_path,
            manifest_path=args.manifest_path,
            enable_ocr=not args.disable_ocr,
        )
    elif args.command == "retrieve-docs":
        payload = gateway.call("retrieve_documents", query=args.query, top_k=args.top_k)
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
        )
    else:
        raise ValueError(f"Unhandled command: {args.command}")

    print(json.dumps(payload, indent=2, ensure_ascii=False))


if __name__ == "__main__":
    main()
