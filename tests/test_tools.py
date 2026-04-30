from __future__ import annotations

import csv
import sqlite3
import unittest
from pathlib import Path

from analytics_assistant.tools.document_tool import build_document_index, retrieve_documents
from analytics_assistant.tools.spreadsheet_tool import analyze_spreadsheet
from analytics_assistant.tools.sql_tool import secure_sql_query


PROJECT_ROOT = Path(__file__).resolve().parents[1]
TEST_DB = PROJECT_ROOT / "storage" / "test_analytics.db"
TEST_DOC_DIR = PROJECT_ROOT / "tests" / "fixtures" / "documents"
TEST_DOC = TEST_DOC_DIR / "test_board.md"
TEST_INDEX = PROJECT_ROOT / "storage" / "test_document_index.json"
TEST_MANIFEST = PROJECT_ROOT / "storage" / "test_document_manifest.json"
TEST_SHEET_DIR = PROJECT_ROOT / "tests" / "fixtures" / "spreadsheets"
TEST_SHEET = TEST_SHEET_DIR / "test_sales.csv"


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
        TEST_DOC.write_text("Q4 revenue risk came from delayed renewals.", encoding="utf-8")

        index_response = build_document_index(
            str(TEST_DOC_DIR),
            str(TEST_INDEX),
            manifest_path=str(TEST_MANIFEST),
        )
        response = retrieve_documents("revenue renewals", index_path=str(TEST_INDEX))

        manifest_entry = next(
            item for item in index_response.data["manifest"] if item["document"] == "test_board.md"
        )
        self.assertEqual(manifest_entry["extraction_methods"], ["plain_text"])
        self.assertTrue(TEST_MANIFEST.exists())
        self.assertEqual(response.sources[0].name, "test_board.md")
        self.assertIn("delayed renewals", response.data["chunks"][0]["text"])
        self.assertEqual(response.data["chunks"][0]["extraction_method"], "plain_text")

    def test_csv_describe_and_group_by(self) -> None:
        from analytics_assistant.tools import spreadsheet_tool

        old_dir = spreadsheet_tool.settings.spreadsheet_dir
        object.__setattr__(spreadsheet_tool.settings, "spreadsheet_dir", TEST_SHEET_DIR)
        try:
            with TEST_SHEET.open("w", newline="", encoding="utf-8") as csv_file:
                writer = csv.DictWriter(csv_file, fieldnames=["region", "revenue"])
                writer.writeheader()
                writer.writerow({"region": "East", "revenue": "100"})
                writer.writerow({"region": "East", "revenue": "50"})

            describe = analyze_spreadsheet("test_sales.csv")
            self.assertEqual(describe.data["row_count"], 2)

            grouped = analyze_spreadsheet(
                "test_sales.csv",
                operation="group_by",
                group_by="region",
                metric="revenue",
            )
            self.assertEqual(grouped.data, [{"region": "East", "revenue_sum": 150.0}])
        finally:
            object.__setattr__(spreadsheet_tool.settings, "spreadsheet_dir", old_dir)


if __name__ == "__main__":
    unittest.main()
