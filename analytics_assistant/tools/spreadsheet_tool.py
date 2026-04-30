from __future__ import annotations

import csv
from collections import defaultdict
from pathlib import Path
from statistics import mean
from typing import Any

from analytics_assistant.config import settings
from analytics_assistant.models import Source, ToolResponse


def _safe_file_path(file_name: str) -> Path:
    base = settings.spreadsheet_dir.resolve()
    path = (base / file_name).resolve()
    if base not in path.parents and path != base:
        raise ValueError("Spreadsheet path must stay inside the configured spreadsheet directory.")
    if not path.exists():
        raise FileNotFoundError(f"Spreadsheet not found: {path}")
    return path


def _read_csv(path: Path) -> list[dict[str, Any]]:
    with path.open(newline="", encoding="utf-8-sig") as csv_file:
        return list(csv.DictReader(csv_file))


def _read_xlsx(path: Path, sheet: str | None) -> list[dict[str, Any]]:
    try:
        from openpyxl import load_workbook  # type: ignore
    except ModuleNotFoundError as exc:
        raise RuntimeError(
            "XLSX analysis requires openpyxl. Install dependencies with `pip install -r requirements.txt`."
        ) from exc

    workbook = load_workbook(path, data_only=True, read_only=True)
    worksheet = workbook[sheet] if sheet else workbook.active
    rows = list(worksheet.iter_rows(values_only=True))
    if not rows:
        return []
    headers = [str(value) if value is not None else f"column_{index}" for index, value in enumerate(rows[0], start=1)]
    return [
        {headers[index]: value for index, value in enumerate(row)}
        for row in rows[1:]
    ]


def _read_table(path: Path, sheet: str | None) -> list[dict[str, Any]]:
    if path.suffix.lower() == ".csv":
        return _read_csv(path)
    if path.suffix.lower() in {".xlsx", ".xlsm"}:
        return _read_xlsx(path, sheet)
    raise ValueError("Supported spreadsheet formats are .csv, .xlsx, and .xlsm.")


def _to_number(value: Any) -> float | None:
    if value is None or value == "":
        return None
    try:
        return float(str(value).replace(",", ""))
    except ValueError:
        return None


def list_spreadsheets() -> ToolResponse:
    settings.spreadsheet_dir.mkdir(parents=True, exist_ok=True)
    files = [
        path.name
        for path in sorted(settings.spreadsheet_dir.iterdir())
        if path.suffix.lower() in {".csv", ".xlsx", ".xlsm"}
    ]
    return ToolResponse(
        data={"files": files},
        sources=[],
        explainability={"directory": str(settings.spreadsheet_dir)},
    )


def analyze_spreadsheet(
    file_name: str,
    operation: str = "describe",
    sheet: str | None = None,
    group_by: str | None = None,
    metric: str | None = None,
    aggregation: str = "sum",
) -> ToolResponse:
    path = _safe_file_path(file_name)
    rows = _read_table(path, sheet)
    columns = list(rows[0].keys()) if rows else []

    if operation == "describe":
        numeric_columns: dict[str, dict[str, float | int]] = {}
        for column in columns:
            values = [number for row in rows if (number := _to_number(row.get(column))) is not None]
            if values:
                numeric_columns[column] = {
                    "count": len(values),
                    "min": min(values),
                    "max": max(values),
                    "mean": mean(values),
                }
        data: Any = {"row_count": len(rows), "columns": columns, "numeric_columns": numeric_columns}
    elif operation == "group_by":
        if not group_by or not metric:
            raise ValueError("group_by operation requires group_by and metric.")
        grouped: dict[str, list[float]] = defaultdict(list)
        for row in rows:
            number = _to_number(row.get(metric))
            if number is not None:
                grouped[str(row.get(group_by))].append(number)
        if aggregation == "sum":
            data = [{group_by: key, f"{metric}_sum": sum(values)} for key, values in grouped.items()]
        elif aggregation == "avg":
            data = [{group_by: key, f"{metric}_avg": mean(values)} for key, values in grouped.items()]
        elif aggregation == "count":
            data = [{group_by: key, f"{metric}_count": len(values)} for key, values in grouped.items()]
        else:
            raise ValueError("Supported aggregations are sum, avg, and count.")
    else:
        raise ValueError("Supported operations are describe and group_by.")

    return ToolResponse(
        data=data,
        sources=[
            Source(
                type="spreadsheet",
                name=file_name,
                details={
                    "sheet": sheet,
                    "operation": operation,
                    "columns_used": [column for column in [group_by, metric] if column] or columns,
                },
            )
        ],
        explainability={"file_path": str(path), "rows_read": len(rows)},
    )

