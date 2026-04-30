from __future__ import annotations

from analytics_assistant.tool_gateway import ToolGateway
from analytics_assistant.tools.document_tool import build_document_index, retrieve_documents
from analytics_assistant.tools.spreadsheet_tool import analyze_spreadsheet, list_spreadsheets
from analytics_assistant.tools.sql_tool import secure_sql_query


def build_gateway() -> ToolGateway:
    gateway = ToolGateway()
    gateway.register("build_document_index", build_document_index)
    gateway.register("retrieve_documents", retrieve_documents)
    gateway.register("list_spreadsheets", list_spreadsheets)
    gateway.register("analyze_spreadsheet", analyze_spreadsheet)
    gateway.register("secure_sql_query", secure_sql_query)
    return gateway

