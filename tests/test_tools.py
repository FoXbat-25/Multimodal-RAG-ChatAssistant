from __future__ import annotations

import sqlite3
import unittest
from pathlib import Path

from analytics_assistant.tools import document_tool
from analytics_assistant.tools.document_tool import build_document_index, retrieve_documents
from analytics_assistant.tools.spreadsheet_tool import analyze_spreadsheet
from analytics_assistant.tools.sql_tool import secure_sql_query
from analytics_assistant.orchestrator import AnalyticsOrchestrator
from analytics_assistant.llm_summarizer import LlmSummary, _build_prompt
from analytics_assistant.mcp_server import McpServer


PROJECT_ROOT = Path(__file__).resolve().parents[1]
TEST_DB = PROJECT_ROOT / "data" / "test_analytics.db"
TEST_DOC_DIR = PROJECT_ROOT / "tests" / "fixtures" / "documents"
TEST_INDEX = PROJECT_ROOT / "storage" / "test_document_index.json"
TEST_MANIFEST = PROJECT_ROOT / "storage" / "test_document_manifest.json"
TEST_SHEET_DIR = PROJECT_ROOT / "tests" / "fixtures" / "spreadsheets"


class ToolTests(unittest.TestCase):
    def test_sql_tool_allows_select_and_blocks_write(self) -> None:
        with sqlite3.connect(TEST_DB) as connection:
            connection.execute("drop table if exists revenue")
            connection.execute("create table revenue(region text, amount real)")
            connection.execute("insert into revenue values ('East', 100)")
            connection.commit()

        response = secure_sql_query("select region, amount from revenue", str(TEST_DB))
        self.assertEqual(response.data, [{"region": "East", "amount": 100.0}])
        self.assertEqual(response.sources[0].name, "revenue")

        with self.assertRaises(ValueError):
            secure_sql_query("delete from revenue", str(TEST_DB))

    def test_document_index_and_retrieval(self) -> None:
        index_response = build_document_index(
            str(TEST_DOC_DIR),
            str(TEST_INDEX),
            manifest_path=str(TEST_MANIFEST),
            enable_embeddings=False,
        )
        response = retrieve_documents("revenue renewals", index_path=str(TEST_INDEX))

        manifest_entry = next(
            item for item in index_response.data["manifest"] if item["document"] == "fixture_board.md"
        )
        self.assertEqual(manifest_entry["extraction_methods"], ["plain_text"])
        self.assertTrue(TEST_MANIFEST.exists())
        self.assertEqual(response.sources[0].name, "fixture_board.md")
        self.assertIn("delayed renewals", response.data["chunks"][0]["text"])
        self.assertEqual(response.data["chunks"][0]["extraction_method"], "plain_text")

    def test_csv_describe_and_group_by(self) -> None:
        from analytics_assistant.tools import spreadsheet_tool

        old_dir = spreadsheet_tool.settings.spreadsheet_dir
        object.__setattr__(spreadsheet_tool.settings, "spreadsheet_dir", TEST_SHEET_DIR)
        try:
            describe = analyze_spreadsheet("fixture_sales.csv")
            self.assertEqual(describe.data["row_count"], 2)

            grouped = analyze_spreadsheet(
                "fixture_sales.csv",
                operation="group_by",
                group_by="region",
                metric="revenue",
            )
            self.assertEqual(grouped.data, [{"region": "East", "revenue_sum": 150.0}])

            searched = analyze_spreadsheet(
                "fixture_sales.csv",
                operation="search",
                query="why East revenue failed",
            )
            self.assertEqual(searched.data["matched_rows"][0]["row"]["region"], "East")

            profile = analyze_spreadsheet("fixture_sales.csv", operation="auto_profile")
            self.assertIn("revenue", profile.data["numeric_columns"])
            self.assertEqual(profile.data["inferred_roles"]["revenue"], ["revenue"])

            ranked = analyze_spreadsheet(
                "fixture_sales.csv",
                operation="filter_and_rank",
                query="failed comedy",
                rank_by="revenue",
                sort_order="asc",
            )
            self.assertEqual(ranked.data["ranked_rows"][0]["rank_value"], 100.0)
            self.assertEqual(ranked.data["ranked_rows"][0]["row"]["title"], "Laugh Track")

            rating_ranked = analyze_spreadsheet(
                "fixture_sales.csv",
                operation="filter_and_rank",
                query="failed comedy low rating",
                rank_by="revenue",
                sort_order="asc",
            )
            self.assertIsNone(rating_ranked.data["ranked_rows"][0]["vote_count"])
        finally:
            object.__setattr__(spreadsheet_tool.settings, "spreadsheet_dir", old_dir)

    def test_orchestrator_collects_documents_and_spreadsheets(self) -> None:
        from analytics_assistant.tools import spreadsheet_tool

        build_document_index(
            str(TEST_DOC_DIR),
            str(TEST_INDEX),
            manifest_path=str(TEST_MANIFEST),
            enable_embeddings=False,
        )

        old_dir = spreadsheet_tool.settings.spreadsheet_dir
        object.__setattr__(spreadsheet_tool.settings, "spreadsheet_dir", TEST_SHEET_DIR)
        try:
            orchestrator = AnalyticsOrchestrator(document_index_path=str(TEST_INDEX))
            response = orchestrator.answer("why did the comedy movie fail", use_llm=False)

            self.assertIn("Document evidence", response["answer"])
            self.assertIn("Spreadsheet evidence", response["answer"])
            self.assertIn("Spreadsheet analysis", response["answer"])
            self.assertIn("Source legend:", response["answer"])
            self.assertTrue(any(source["type"] == "document" for source in response["sources"]))
            self.assertTrue(any(source["type"] == "spreadsheet" for source in response["sources"]))
            self.assertTrue(any(source["type"] == "spreadsheet_analysis" for source in response["sources"]))
        finally:
            object.__setattr__(spreadsheet_tool.settings, "spreadsheet_dir", old_dir)

    def test_orchestrator_can_use_llm_summarizer(self) -> None:
        class FakeSummarizer:
            def summarize(self, question, evidence, sources):  # type: ignore[no-untyped-def]
                return LlmSummary(
                    text="Answer\nComedy underperformed based on cited evidence [D1].",
                    provider="fake",
                    model="fake-model",
                    used=True,
                )

        build_document_index(
            str(TEST_DOC_DIR),
            str(TEST_INDEX),
            manifest_path=str(TEST_MANIFEST),
            enable_embeddings=False,
        )

        orchestrator = AnalyticsOrchestrator(
            document_index_path=str(TEST_INDEX),
            llm_summarizer=FakeSummarizer(),  # type: ignore[arg-type]
        )
        response = orchestrator.answer("why did the comedy movie fail")

        self.assertTrue(response["llm"]["used"])
        self.assertEqual(response["llm"]["provider"], "fake")
        self.assertIn("Comedy underperformed", response["answer"])
        self.assertIn("Source legend:", response["answer"])
        self.assertEqual(response["grounding"]["invalid_citations"], [])

    def test_orchestrator_flags_invalid_llm_citations(self) -> None:
        class BadCitationSummarizer:
            def summarize(self, question, evidence, sources):  # type: ignore[no-untyped-def]
                return LlmSummary(
                    text="Answer\nThis claim cites a nonexistent source [A99].",
                    provider="fake",
                    model="fake-model",
                    used=True,
                )

        build_document_index(
            str(TEST_DOC_DIR),
            str(TEST_INDEX),
            manifest_path=str(TEST_MANIFEST),
            enable_embeddings=False,
        )

        orchestrator = AnalyticsOrchestrator(
            document_index_path=str(TEST_INDEX),
            llm_summarizer=BadCitationSummarizer(),  # type: ignore[arg-type]
        )
        response = orchestrator.answer("why did the comedy movie fail")

        self.assertEqual(response["grounding"]["invalid_citations"], ["A99"])
        self.assertIn("Citation warning", response["answer"])

    def test_llm_prompt_asks_for_answer_not_schema_description(self) -> None:
        prompt = _build_prompt(
            "why did the comedy movie fail",
            {
                "documents": [
                    {
                        "document": "movie_report.pdf",
                        "page": 1,
                        "text": "Audience reviews said the humor was stale.",
                    }
                ],
                "spreadsheets": [
                    {
                        "file": "movies.csv",
                        "matched_rows": [
                            {
                                "score": 2,
                                "row": {
                                    "title": "Laugh Track",
                                    "genre": "Comedy",
                                    "rating": "4.8",
                                    "outcome": "Failed after weak reviews",
                                },
                            }
                        ],
                    }
                ],
                "spreadsheet_analysis": [
                    {
                        "file": "movies.csv",
                        "filter_and_rank": {
                            "ranked_rows": [
                                {
                                    "rank_by": "rating",
                                    "rank_value": 4.8,
                                    "row": {"title": "Laugh Track", "rating": "4.8"},
                                }
                            ]
                        },
                    }
                ],
                "warnings": [],
            },
            [],
        )

        self.assertIn("Do not summarize the format", prompt)
        self.assertIn("Audience reviews said the humor was stale.", prompt)
        self.assertIn("[S1] movies.csv", prompt)
        self.assertIn("[A1] movies.csv ranked by rating", prompt)
        self.assertNotIn('"sources"', prompt)

    def test_semantic_document_retrieval_uses_embeddings_when_available(self) -> None:
        original_try_embedding = document_tool._try_embedding

        def fake_embedding(text: str, model: str):  # type: ignore[no-untyped-def]
            lowered = text.lower()
            if "audience" in lowered or "humor" in lowered:
                return [1.0, 0.0], None
            if "renewal" in lowered or "revenue" in lowered:
                return [0.0, 1.0], None
            return [0.5, 0.5], None

        document_tool._try_embedding = fake_embedding  # type: ignore[assignment]
        try:
            index_response = build_document_index(
                str(TEST_DOC_DIR),
                str(TEST_INDEX),
                manifest_path=str(TEST_MANIFEST),
                enable_embeddings=True,
                embedding_model="fake-embed",
            )
            response = retrieve_documents(
                "audience disliked humor",
                index_path=str(TEST_INDEX),
                retrieval_mode="semantic",
            )
        finally:
            document_tool._try_embedding = original_try_embedding  # type: ignore[assignment]

        self.assertEqual(index_response.data["embedding_model"], "fake-embed")
        self.assertIn("ollama_embeddings:fake-embed", response.explainability["retrieval_method"])
        self.assertIn("humor stale", response.data["chunks"][0]["text"])

    def test_mcp_lists_and_calls_assistant_tool(self) -> None:
        server = McpServer()
        tools_response = server.handle({"jsonrpc": "2.0", "id": 1, "method": "tools/list"})
        self.assertIsNotNone(tools_response)
        tool_names = [tool["name"] for tool in tools_response["result"]["tools"]]  # type: ignore[index]
        self.assertIn("ask_analytics_assistant", tool_names)

        call_response = server.handle(
            {
                "jsonrpc": "2.0",
                "id": 2,
                "method": "tools/call",
                "params": {
                    "name": "ask_analytics_assistant",
                    "arguments": {
                        "question": "why did the comedy movie fail",
                        "use_llm": False,
                    },
                },
            }
        )
        self.assertIsNotNone(call_response)
        result = call_response["result"]  # type: ignore[index]
        self.assertIn("structuredContent", result)
        self.assertIn("answer", result["structuredContent"])

    def test_app_ask_endpoint(self) -> None:
        try:
            from fastapi.testclient import TestClient
        except ModuleNotFoundError:
            self.skipTest("fastapi test client is unavailable")

        from analytics_assistant.app import app

        client = TestClient(app)
        response = client.post(
            "/ask",
            json={
                "question": "why did the comedy movie fail",
                "use_llm": False,
                "top_k": 2,
            },
        )
        self.assertEqual(response.status_code, 200)
        payload = response.json()
        self.assertIn("answer", payload)
        self.assertIn("sources", payload)


if __name__ == "__main__":
    unittest.main()
