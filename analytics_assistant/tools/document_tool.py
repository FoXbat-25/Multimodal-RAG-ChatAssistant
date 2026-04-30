from __future__ import annotations

import json
import math
import re
from collections import Counter
from dataclasses import asdict, dataclass
from hashlib import sha256
from pathlib import Path
from typing import Iterable

from analytics_assistant.config import settings
from analytics_assistant.models import Source, ToolResponse


@dataclass(frozen=True)
class DocumentChunk:
    chunk_id: str
    document: str
    page: int | None
    text: str
    term_counts: dict[str, int]
    extraction_method: str = "unknown"


@dataclass(frozen=True)
class PageExtraction:
    page: int | None
    text: str
    method: str
    warnings: list[str]


@dataclass(frozen=True)
class DocumentManifestEntry:
    document: str
    path: str
    sha256: str
    pages_seen: int
    chunks_indexed: int
    characters_extracted: int
    extraction_methods: list[str]
    warnings: list[str]


def _tokens(text: str) -> list[str]:
    return [token.lower() for token in re.findall(r"[A-Za-z0-9][A-Za-z0-9_-]*", text)]


def _chunks(text: str, *, size: int = 900, overlap: int = 150) -> Iterable[str]:
    normalized = re.sub(r"\s+", " ", text).strip()
    if not normalized:
        return

    start = 0
    while start < len(normalized):
        yield normalized[start : start + size]
        start += size - overlap


def _file_hash(path: Path) -> str:
    digest = sha256()
    with path.open("rb") as document:
        for block in iter(lambda: document.read(1024 * 1024), b""):
            digest.update(block)
    return digest.hexdigest()


def _ocr_dependencies_available() -> tuple[bool, str | None]:
    try:
        import pypdfium2  # noqa: F401
        import pytesseract  # noqa: F401
    except ModuleNotFoundError as exc:
        return False, exc.name
    return True, None


def _ocr_pdf_page(path: Path, page_index: int) -> str:
    try:
        import pypdfium2 as pdfium  # type: ignore
        import pytesseract  # type: ignore
    except ModuleNotFoundError as exc:
        raise RuntimeError(
            "OCR requires pypdfium2 and pytesseract. Install Python dependencies and the Tesseract OCR binary."
        ) from exc

    pdf = pdfium.PdfDocument(str(path))
    page = pdf[page_index]
    image = page.render(scale=2).to_pil()
    return pytesseract.image_to_string(image) or ""


def _read_pdf(path: Path, *, enable_ocr: bool = True) -> list[PageExtraction]:
    try:
        from pypdf import PdfReader  # type: ignore
    except ModuleNotFoundError as exc:
        raise RuntimeError(
            "PDF extraction requires pypdf. Install dependencies with `pip install -r requirements.txt`."
        ) from exc

    reader = PdfReader(str(path))
    pages: list[PageExtraction] = []
    ocr_available, missing_ocr_module = _ocr_dependencies_available()
    for page_number, page in enumerate(reader.pages, start=1):
        text = page.extract_text() or ""
        cleaned_text = re.sub(r"\s+", " ", text).strip()
        warnings: list[str] = []
        method = "pypdf_text"

        if len(cleaned_text) < settings.min_pdf_text_chars_per_page:
            warnings.append("low_text_extraction")
            if enable_ocr and ocr_available:
                try:
                    ocr_text = _ocr_pdf_page(path, page_number - 1)
                except Exception as exc:  # pragma: no cover - depends on host OCR binary
                    warnings.append(f"ocr_fallback_failed_{type(exc).__name__}")
                else:
                    if len(ocr_text.strip()) > len(cleaned_text):
                        text = ocr_text
                        method = "ocr"
                        warnings.append("ocr_fallback_used")
            elif enable_ocr:
                warnings.append(
                    f"ocr_fallback_unavailable_missing_{missing_ocr_module or 'dependency'}"
                )

        pages.append(PageExtraction(page=page_number, text=text, method=method, warnings=warnings))
    return pages


def _read_document(path: Path, *, enable_ocr: bool = True) -> list[PageExtraction]:
    suffix = path.suffix.lower()
    if suffix == ".pdf":
        return _read_pdf(path, enable_ocr=enable_ocr)
    if suffix in {".txt", ".md"}:
        return [
            PageExtraction(
                page=None,
                text=path.read_text(encoding="utf-8"),
                method="plain_text",
                warnings=[],
            )
        ]
    return []


def build_document_index(
    document_dir: str | None = None,
    index_path: str | None = None,
    manifest_path: str | None = None,
    enable_ocr: bool = True,
) -> ToolResponse:
    doc_dir = Path(document_dir) if document_dir else settings.document_dir
    output_path = Path(index_path) if index_path else settings.document_index_path
    manifest_output_path = Path(manifest_path) if manifest_path else settings.document_manifest_path
    output_path.parent.mkdir(parents=True, exist_ok=True)
    manifest_output_path.parent.mkdir(parents=True, exist_ok=True)
    doc_dir.mkdir(parents=True, exist_ok=True)

    indexed_chunks: list[DocumentChunk] = []
    manifest: list[DocumentManifestEntry] = []
    for path in sorted(doc_dir.iterdir()):
        if not path.is_file():
            continue
        if path.suffix.lower() not in {".pdf", ".txt", ".md"}:
            continue

        entries_before = len(indexed_chunks)
        extractions = _read_document(path, enable_ocr=enable_ocr)
        for extraction in extractions:
            for chunk_number, chunk_text in enumerate(_chunks(extraction.text), start=1):
                chunk_id = f"{path.stem}_p{extraction.page or 0}_c{chunk_number}"
                indexed_chunks.append(
                    DocumentChunk(
                        chunk_id=chunk_id,
                        document=path.name,
                        page=extraction.page,
                        text=chunk_text,
                        term_counts=dict(Counter(_tokens(chunk_text))),
                        extraction_method=extraction.method,
                    )
                )

        warnings = sorted({warning for extraction in extractions for warning in extraction.warnings})
        chunks_indexed = len(indexed_chunks) - entries_before
        if not chunks_indexed:
            warnings.append("no_chunks_indexed")

        manifest.append(
            DocumentManifestEntry(
                document=path.name,
                path=str(path),
                sha256=_file_hash(path),
                pages_seen=len(extractions),
                chunks_indexed=chunks_indexed,
                characters_extracted=sum(len(extraction.text.strip()) for extraction in extractions),
                extraction_methods=sorted({extraction.method for extraction in extractions}),
                warnings=sorted(set(warnings)),
            )
        )

    payload = {"chunks": [asdict(chunk) for chunk in indexed_chunks]}
    output_path.write_text(json.dumps(payload, indent=2), encoding="utf-8")
    manifest_payload = {"documents": [asdict(entry) for entry in manifest]}
    manifest_output_path.write_text(json.dumps(manifest_payload, indent=2), encoding="utf-8")

    return ToolResponse(
        data={
            "indexed_chunks": len(indexed_chunks),
            "indexed_documents": len(manifest),
            "index_path": str(output_path),
            "manifest_path": str(manifest_output_path),
            "manifest": [asdict(entry) for entry in manifest],
        },
        sources=[
            Source(type="document", name=path.name, details={"path": str(path)})
            for path in sorted(doc_dir.iterdir())
            if path.is_file() and path.suffix.lower() in {".pdf", ".txt", ".md"}
        ],
        explainability={
            "index_type": "local_keyword_vector_index",
            "ocr_enabled": enable_ocr,
            "ocr_dependencies_available": _ocr_dependencies_available()[0],
        },
    )


def _load_index(index_path: Path) -> list[DocumentChunk]:
    if not index_path.exists():
        raise FileNotFoundError(
            f"Document index not found: {index_path}. Run build_document_index first."
        )
    payload = json.loads(index_path.read_text(encoding="utf-8"))
    return [DocumentChunk(**chunk) for chunk in payload.get("chunks", [])]


def _cosine_score(query_counts: Counter[str], chunk_counts: dict[str, int]) -> float:
    shared = set(query_counts) & set(chunk_counts)
    numerator = sum(query_counts[token] * chunk_counts[token] for token in shared)
    query_norm = math.sqrt(sum(value * value for value in query_counts.values()))
    chunk_norm = math.sqrt(sum(value * value for value in chunk_counts.values()))
    if not query_norm or not chunk_norm:
        return 0.0
    return numerator / (query_norm * chunk_norm)


def retrieve_documents(
    query: str,
    top_k: int = 5,
    index_path: str | None = None,
) -> ToolResponse:
    query_counts = Counter(_tokens(query))
    if not query_counts:
        raise ValueError("Document query is empty.")

    path = Path(index_path) if index_path else settings.document_index_path
    chunks = _load_index(path)
    scored = [
        (_cosine_score(query_counts, chunk.term_counts), chunk)
        for chunk in chunks
    ]
    scored = sorted(scored, key=lambda item: item[0], reverse=True)
    selected = [
        {
            "score": round(score, 4),
            "chunk_id": chunk.chunk_id,
            "document": chunk.document,
            "page": chunk.page,
            "text": chunk.text,
            "extraction_method": chunk.extraction_method,
        }
        for score, chunk in scored[: min(top_k, settings.max_document_chunks)]
        if score > 0
    ]

    return ToolResponse(
        data={"query": query, "chunks": selected},
        sources=[
            Source(
                type="document",
                name=item["document"],
                details={
                    "page": item["page"],
                    "chunk_id": item["chunk_id"],
                    "score": item["score"],
                    "extraction_method": item["extraction_method"],
                },
            )
            for item in selected
        ],
        explainability={
            "retrieval_method": "cosine_similarity_over_local_term_vectors",
            "index_path": str(path),
        },
    )
