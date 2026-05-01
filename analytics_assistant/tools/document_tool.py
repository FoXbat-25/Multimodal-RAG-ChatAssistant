from __future__ import annotations

import json
import math
import re
import urllib.error
import urllib.request
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
    embedding: list[float] | None = None
    embedding_model: str | None = None


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
    embedding_model: str | None
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


def _ollama_embedding(text: str, model: str) -> list[float]:
    payload = {"model": model, "prompt": text}
    request = urllib.request.Request(
        f"{settings.ollama_url.rstrip('/')}/api/embeddings",
        data=json.dumps(payload).encode("utf-8"),
        headers={"Content-Type": "application/json"},
        method="POST",
    )
    with urllib.request.urlopen(request, timeout=120) as response:
        response_payload = json.loads(response.read().decode("utf-8"))
    embedding = response_payload.get("embedding")
    if not isinstance(embedding, list) or not embedding:
        raise RuntimeError(f"Ollama returned no embedding for model {model}.")
    return [float(value) for value in embedding]


def _try_embedding(text: str, model: str) -> tuple[list[float] | None, str | None]:
    try:
        return _ollama_embedding(text, model), None
    except urllib.error.HTTPError as exc:
        body = exc.read().decode("utf-8", errors="replace")
        return None, f"embedding_unavailable_http_{exc.code}:{body}"
    except Exception as exc:
        return None, f"embedding_unavailable_{type(exc).__name__}:{exc}"


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


def _iter_pdf_pages(path: Path, *, enable_ocr: bool = True) -> Iterable[PageExtraction]:
    try:
        from pypdf import PdfReader  # type: ignore
    except ModuleNotFoundError as exc:
        raise RuntimeError(
            "PDF extraction requires pypdf. Install dependencies with `pip install -r requirements.txt`."
        ) from exc

    reader = PdfReader(str(path))
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

        yield PageExtraction(page=page_number, text=text, method=method, warnings=warnings)


def _read_pdf(path: Path, *, enable_ocr: bool = True) -> list[PageExtraction]:
    return list(_iter_pdf_pages(path, enable_ocr=enable_ocr))


def _iter_document_pages(path: Path, *, enable_ocr: bool = True) -> Iterable[PageExtraction]:
    suffix = path.suffix.lower()
    if suffix == ".pdf":
        yield from _iter_pdf_pages(path, enable_ocr=enable_ocr)
    elif suffix in {".txt", ".md"}:
        yield PageExtraction(
            page=None,
            text=path.read_text(encoding="utf-8"),
            method="plain_text",
            warnings=[],
        )


def _read_document(path: Path, *, enable_ocr: bool = True) -> list[PageExtraction]:
    return list(_iter_document_pages(path, enable_ocr=enable_ocr))


def build_document_index(
    document_dir: str | None = None,
    index_path: str | None = None,
    manifest_path: str | None = None,
    enable_ocr: bool = True,
    enable_embeddings: bool = True,
    embedding_model: str | None = None,
    max_chunks: int | None = None,
) -> ToolResponse:
    doc_dir = Path(document_dir) if document_dir else settings.document_dir
    output_path = Path(index_path) if index_path else settings.document_index_path
    manifest_output_path = Path(manifest_path) if manifest_path else settings.document_manifest_path
    output_path.parent.mkdir(parents=True, exist_ok=True)
    manifest_output_path.parent.mkdir(parents=True, exist_ok=True)
    doc_dir.mkdir(parents=True, exist_ok=True)

    indexed_chunks: list[DocumentChunk] = []
    manifest: list[DocumentManifestEntry] = []
    selected_embedding_model = embedding_model or settings.ollama_embedding_model
    embedding_warning: str | None = None
    for path in sorted(doc_dir.iterdir()):
        if max_chunks is not None and len(indexed_chunks) >= max_chunks:
            break
        if not path.is_file():
            continue
        if path.suffix.lower() not in {".pdf", ".txt", ".md"}:
            continue

        entries_before = len(indexed_chunks)
        pages_seen = 0
        characters_extracted = 0
        extraction_methods: set[str] = set()
        warnings: list[str] = []
        for extraction in _iter_document_pages(path, enable_ocr=enable_ocr):
            pages_seen += 1
            characters_extracted += len(extraction.text.strip())
            extraction_methods.add(extraction.method)
            warnings.extend(extraction.warnings)
            for chunk_number, chunk_text in enumerate(_chunks(extraction.text), start=1):
                if max_chunks is not None and len(indexed_chunks) >= max_chunks:
                    break
                chunk_id = f"{path.stem}_p{extraction.page or 0}_c{chunk_number}"
                embedding = None
                chunk_embedding_model = None
                if enable_embeddings and not embedding_warning:
                    embedding, embedding_warning = _try_embedding(chunk_text, selected_embedding_model)
                    if embedding is not None:
                        chunk_embedding_model = selected_embedding_model
                indexed_chunks.append(
                    DocumentChunk(
                        chunk_id=chunk_id,
                        document=path.name,
                        page=extraction.page,
                        text=chunk_text,
                        term_counts=dict(Counter(_tokens(chunk_text))),
                        extraction_method=extraction.method,
                        embedding=embedding,
                        embedding_model=chunk_embedding_model,
                    )
                )
            if max_chunks is not None and len(indexed_chunks) >= max_chunks:
                break

        chunks_indexed = len(indexed_chunks) - entries_before
        if not chunks_indexed:
            warnings.append("no_chunks_indexed")
        if enable_embeddings and embedding_warning:
            warnings.append(embedding_warning)

        manifest.append(
            DocumentManifestEntry(
                document=path.name,
                path=str(path),
                sha256=_file_hash(path),
                pages_seen=pages_seen,
                chunks_indexed=chunks_indexed,
                characters_extracted=characters_extracted,
                extraction_methods=sorted(extraction_methods),
                embedding_model=selected_embedding_model if enable_embeddings and not embedding_warning else None,
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
            "embedding_model": selected_embedding_model if enable_embeddings and not embedding_warning else None,
            "embedding_warning": embedding_warning,
            "manifest": [asdict(entry) for entry in manifest],
        },
        sources=[
            Source(type="document", name=entry.document, details={"path": entry.path})
            for entry in manifest
        ],
        explainability={
            "index_type": "local_keyword_vector_index",
            "embeddings_enabled": enable_embeddings,
            "embedding_model": selected_embedding_model if enable_embeddings and not embedding_warning else None,
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


def _cosine_embedding_score(query_embedding: list[float], chunk_embedding: list[float]) -> float:
    numerator = sum(left * right for left, right in zip(query_embedding, chunk_embedding))
    query_norm = math.sqrt(sum(value * value for value in query_embedding))
    chunk_norm = math.sqrt(sum(value * value for value in chunk_embedding))
    if not query_norm or not chunk_norm:
        return 0.0
    return numerator / (query_norm * chunk_norm)


def retrieve_documents(
    query: str,
    top_k: int = 5,
    index_path: str | None = None,
    retrieval_mode: str = "auto",
) -> ToolResponse:
    query_counts = Counter(_tokens(query))
    if not query_counts:
        raise ValueError("Document query is empty.")

    path = Path(index_path) if index_path else settings.document_index_path
    chunks = _load_index(path)
    embedding_chunks = [
        chunk for chunk in chunks if chunk.embedding and chunk.embedding_model
    ]
    retrieval_method = "cosine_similarity_over_local_term_vectors"
    query_embedding_warning = None

    if retrieval_mode in {"auto", "semantic"} and embedding_chunks:
        model = embedding_chunks[0].embedding_model or settings.ollama_embedding_model
        query_embedding, query_embedding_warning = _try_embedding(query, model)
        if query_embedding is not None:
            retrieval_method = f"cosine_similarity_over_ollama_embeddings:{model}"
            scored = [
                (_cosine_embedding_score(query_embedding, chunk.embedding or []), chunk)
                for chunk in embedding_chunks
            ]
        elif retrieval_mode == "semantic":
            raise RuntimeError(query_embedding_warning or "semantic retrieval failed")
        else:
            scored = [
                (_cosine_score(query_counts, chunk.term_counts), chunk)
                for chunk in chunks
            ]
    elif retrieval_mode == "semantic":
        raise RuntimeError("semantic retrieval requested but no chunk embeddings are available")
    else:
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
            "embedding_model": chunk.embedding_model,
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
                    "embedding_model": item["embedding_model"],
                },
            )
            for item in selected
        ],
        explainability={
            "retrieval_method": retrieval_method,
            "index_path": str(path),
            "query_embedding_warning": query_embedding_warning,
        },
    )
