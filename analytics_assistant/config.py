from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path


PROJECT_ROOT = Path(__file__).resolve().parents[1]


@dataclass(frozen=True)
class Settings:
    data_dir: Path = PROJECT_ROOT / "data"
    document_dir: Path = PROJECT_ROOT / "data" / "documents"
    spreadsheet_dir: Path = PROJECT_ROOT / "data" / "spreadsheets"
    storage_dir: Path = PROJECT_ROOT / "storage"
    document_index_path: Path = PROJECT_ROOT / "storage" / "document_index.json"
    document_manifest_path: Path = PROJECT_ROOT / "storage" / "document_manifest.json"
    audit_log_path: Path = PROJECT_ROOT / "storage" / "audit.jsonl"
    default_sqlite_path: Path = PROJECT_ROOT / "data" / "analytics.db"
    max_sql_rows: int = 500
    max_document_chunks: int = 8
    min_pdf_text_chars_per_page: int = 40


settings = Settings()
