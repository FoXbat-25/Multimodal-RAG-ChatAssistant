# Internal Analytics Assistant Tool Layer

This repo contains the first secure boundary for an internal AI analytics assistant. The LLM should call these tools through the gateway, never connect directly to databases, files, PDFs, or spreadsheets.

## What is included

- `secure_sql_query`: read-only SQL execution against SQLite for the MVP.
- `build_document_index`: extracts `.txt`, `.md`, and `.pdf` files into a local searchable index.
- `retrieve_documents`: retrieves relevant document chunks with source metadata.
- Document ingestion manifest with file hashes, page counts, chunk counts, extraction methods, and warnings.
- `list_spreadsheets`: lists approved spreadsheet files.
- `analyze_spreadsheet`: describes or aggregates `.csv`, `.xlsx`, and `.xlsm` files.
- `ToolGateway`: the single tool entry point with audit logging and provenance.

## Data folders

Place files here:

- Documents: `data/documents/`
- Spreadsheets: `data/spreadsheets/`
- SQLite database: `data/analytics.db`

Generated files:

- Document index: `storage/document_index.json`
- Document manifest: `storage/document_manifest.json`
- Audit log: `storage/audit.jsonl`

## Run from CLI

```powershell
python -m analytics_assistant.cli build-doc-index
python -m analytics_assistant.cli retrieve-docs "quarterly revenue risk"
python -m analytics_assistant.cli list-sheets
python -m analytics_assistant.cli sheet sales.csv --operation describe
python -m analytics_assistant.cli sql "select * from revenue limit 10"
```

`build-doc-index` writes a manifest so you can see whether each PDF produced useful text:

```powershell
python -m analytics_assistant.cli build-doc-index
Get-Content storage/document_manifest.json
```

If a scanned/image PDF has little extractable text, the manifest will include `low_text_extraction`. OCR fallback is supported when `pypdfium2`, `pytesseract`, and the Tesseract OCR binary are installed. If they are missing, the manifest records `ocr_fallback_unavailable_missing_*` so you know the file needs OCR setup.

## Run as API

```powershell
pip install -r requirements.txt
uvicorn analytics_assistant.app:app --reload --port 8000
```

Example:

```powershell
Invoke-RestMethod -Method Post -Uri http://localhost:8000/tools/retrieve_documents -Body '{"query":"revenue risk","top_k":3}' -ContentType 'application/json'
```

## Architecture note

This is MCP-ready: each registered gateway tool can be exposed as an MCP tool later. The important part is that every response already returns `sources` and `explainability`, so the orchestrator can cite tables, documents, spreadsheets, filters, limits, and operations used.
