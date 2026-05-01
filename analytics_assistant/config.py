from __future__ import annotations

from dataclasses import dataclass
import os
from pathlib import Path


PROJECT_ROOT = Path(__file__).resolve().parents[1]


def _load_project_env() -> None:
    env_path = PROJECT_ROOT / ".env"
    try:
        from dotenv import load_dotenv  # type: ignore
    except ModuleNotFoundError:
        if not env_path.exists():
            return
        for line in env_path.read_text(encoding="utf-8").splitlines():
            stripped = line.strip()
            if not stripped or stripped.startswith("#") or "=" not in stripped:
                continue
            key, value = stripped.split("=", 1)
            os.environ.setdefault(key.strip(), value.strip().strip('"').strip("'"))
    else:
        load_dotenv(env_path, override=False)


def _env_path(name: str, default: Path) -> Path:
    value = os.getenv(name)
    if not value:
        return default
    path = Path(value)
    return path if path.is_absolute() else PROJECT_ROOT / path


def _env_int(name: str, default: int) -> int:
    value = os.getenv(name)
    if not value:
        return default
    return int(value)


_load_project_env()


@dataclass(frozen=True)
class Settings:
    data_dir: Path = _env_path("DATA_DIR", PROJECT_ROOT / "data")
    document_dir: Path = _env_path("DOCUMENT_DIR", data_dir / "documents")
    spreadsheet_dir: Path = _env_path("SPREADSHEET_DIR", data_dir / "spreadsheets")
    storage_dir: Path = _env_path("STORAGE_DIR", PROJECT_ROOT / "storage")
    document_index_path: Path = _env_path("DOCUMENT_INDEX_PATH", storage_dir / "document_index.json")
    document_manifest_path: Path = _env_path(
        "DOCUMENT_MANIFEST_PATH",
        storage_dir / "document_manifest.json",
    )
    audit_log_path: Path = _env_path("AUDIT_LOG_PATH", storage_dir / "audit.jsonl")
    default_sqlite_path: Path = _env_path("DEFAULT_SQLITE_PATH", data_dir / "test_analytics.db")
    max_sql_rows: int = _env_int("MAX_SQL_ROWS", 500)
    max_document_chunks: int = _env_int("MAX_DOCUMENT_CHUNKS", 8)
    min_pdf_text_chars_per_page: int = _env_int("MIN_PDF_TEXT_CHARS_PER_PAGE", 40)
    llm_provider: str = os.getenv("LLM_PROVIDER", "openai")
    openai_model: str = os.getenv("OPENAI_MODEL", "gpt-5.4-mini")
    ollama_model: str = os.getenv("OLLAMA_MODEL", "llama3.1:8b")
    ollama_embedding_model: str = os.getenv("OLLAMA_EMBED_MODEL", "nomic-embed-text")
    ollama_url: str = os.getenv("OLLAMA_URL", "http://127.0.0.1:11434")
    llm_max_output_tokens: int = _env_int("LLM_MAX_OUTPUT_TOKENS", 900)


settings = Settings()
