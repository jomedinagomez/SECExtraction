"""Hybrid layout-based SEC financial-table extractor.

Strategy:
  1. Call `prebuilt-layout` once per PDF (~15s vs ~25min for the custom
     analyzer) to get raw OCR cells for every table.
  2. For each table, read `caption` or nearest preceding paragraph to derive
     tableTitle, companyName, unit, and statementType (enum bucket).
  3. Build periodHeaders / periodGroupHeaders from `kind=columnHeader` cells
     using a two-row split where present.
  4. Walk data rows and infer `isSectionHeader`, `isSubtotal`, `level`,
     `parentLineItem` from label patterns (ALLCAPS headers, 'Total X' /
     'Net cash X' / 'Balance, DATE' subtotals).
  5. Emit JSON in the same shape the custom analyzer produced, so
     `export_to_excel.py` works unchanged.

Usage:
    python scripts/extract_via_layout.py
    python scripts/extract_via_layout.py --concurrency 3 --no-excel
"""

from __future__ import annotations

import argparse
import json
import os
import re
import sys
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path
from typing import Any

from azure.ai.contentunderstanding import ContentUnderstandingClient
from azure.identity import ClientSecretCredential
from dotenv import load_dotenv

ROOT = Path(__file__).resolve().parent.parent
DEFAULT_PDF_DIR = ROOT / "email" / "attachements"
DEFAULT_OUT_DIR = ROOT / "output"

sys.path.insert(0, str(Path(__file__).resolve().parent))
from export_to_excel import export_document  # noqa: E402


# --------------------------------------------------------------------------- #
# Classification heuristics
# --------------------------------------------------------------------------- #

_TITLE_TO_TYPE: list[tuple[re.Pattern[str], str]] = [
    (re.compile(r"cash\s*flows?", re.I), "CashFlow"),
    (re.compile(r"balance\s*sheets?|statement\s+of\s+financial\s+position", re.I), "BalanceSheet"),
    (re.compile(r"comprehensive\s+(income|loss)", re.I), "ComprehensiveIncome"),
    (re.compile(r"(stockholders?|shareholders?|members?|partners?)['’]?\s*equity|changes\s+in\s+equity", re.I), "Equity"),
    (re.compile(r"statements?\s+of\s+(operations|income|earnings|loss)", re.I), "IncomeStatement"),
]


def classify(title: str) -> str:
    if not title:
        return "Other"
    for pat, bucket in _TITLE_TO_TYPE:
        if pat.search(title):
            # Reject MD&A / FFO / NOI variants that can match 'operations' loosely.
            if re.search(r"comparison of|reconciliation|ffo|noi|adjusted", title, re.I):
                return "Other"
            return bucket
    return "Other"


_UNIT_RE = re.compile(
    r"\(in\s+((?:\w+(?:\s+of\s+\w+)?)"
    r"(?:\s*[-,]\s*except[^)]*)?)\)",
    re.I,
)


def parse_unit(caption: str) -> str:
    if not caption:
        return ""
    m = _UNIT_RE.search(caption)
    return m.group(1).strip() if m else ""


_COMPANY_SUFFIX_RE = re.compile(
    r"^(.*?(?:Inc\.|Corporation|Corp\.|Company|LLC|L\.P\.|Trust|N\.V\.|PLC|LLP|Co\.))",
    re.I,
)


def parse_company(caption: str) -> str:
    """First sentence-fragment ending in a corporate suffix."""
    if not caption:
        return ""
    m = _COMPANY_SUFFIX_RE.match(caption.strip())
    return m.group(1).strip() if m else ""


def parse_title(caption: str) -> str:
    """Everything after the company name, up to the first '(' (which starts unit)."""
    if not caption:
        return ""
    rest = caption
    m = _COMPANY_SUFFIX_RE.match(caption.strip())
    if m:
        rest = caption[m.end():].strip()
    rest = rest.split("(")[0].strip()
    return rest


_SUBTOTAL_RE = re.compile(
    r"^\s*("
    r"total\b|"
    r"net\s+(cash|income|loss|revenues|sales|assets|change|increase|decrease|gain|loss)\b|"
    r"balance[, ]+|"
    r"cash\s+and\s+cash\s+equivalents(?:,?\s+end)|"
    r"cash,?\s+cash\s+equivalents\s+and\s+restricted\s+cash"
    r")",
    re.I,
)


def is_subtotal(label: str) -> bool:
    return bool(_SUBTOTAL_RE.search(label or ""))


def is_section_header(label: str, values: list[str]) -> bool:
    """ALLCAPS (or Title Case ending with ':') label with no values."""
    if not label:
        return False
    if any(v.strip() for v in values):
        return False
    if label.endswith(":"):
        return True
    letters = [c for c in label if c.isalpha()]
    if letters and all(c.isupper() for c in letters):
        return True
    return False


# --------------------------------------------------------------------------- #
# Layout result -> normalized table
# --------------------------------------------------------------------------- #

def _cells_to_grid(cells: list[dict[str, Any]]) -> list[list[str]]:
    if not cells:
        return []
    max_r = max(c.get("rowIndex", 0) + c.get("rowSpan", 1) for c in cells)
    max_c = max(c.get("columnIndex", 0) + c.get("columnSpan", 1) for c in cells)
    grid = [[""] * max_c for _ in range(max_r)]
    for c in cells:
        r = c.get("rowIndex", 0)
        ci = c.get("columnIndex", 0)
        rs = c.get("rowSpan", 1) or 1
        cs = c.get("columnSpan", 1) or 1
        content = (c.get("content") or "").replace("\n", " ").strip()
        for dr in range(rs):
            for dc in range(cs):
                # Only set if empty; first cell wins for spans.
                if not grid[r + dr][ci + dc]:
                    grid[r + dr][ci + dc] = content
    return grid


def _header_rows(cells: list[dict[str, Any]]) -> int:
    """Number of leading rows that are columnHeader cells."""
    header_row_idxs = {
        c.get("rowIndex", 0)
        for c in cells
        if c.get("kind") == "columnHeader"
    }
    if not header_row_idxs:
        return 0
    # Must be contiguous starting at 0.
    n = 0
    while n in header_row_idxs:
        n += 1
    return n


def _build_column_headers(
    grid: list[list[str]], n_header_rows: int, n_value_cols: int
) -> tuple[list[str], list[str]]:
    """Return (periodHeaders, periodGroupHeaders), both length n_value_cols."""
    if n_header_rows == 0 or not grid:
        return [""] * n_value_cols, []

    # Value columns are columns 1..n (col 0 is line-item).
    def col(r: int, c: int) -> str:
        return grid[r][c] if r < len(grid) and c < len(grid[r]) else ""

    if n_header_rows == 1:
        headers = [col(0, 1 + i) for i in range(n_value_cols)]
        return headers, []

    # n_header_rows >= 2: last row = leaf, row above = group (propagated via rowspan fill).
    leaf_row = n_header_rows - 1
    group_row = n_header_rows - 2
    headers = [col(leaf_row, 1 + i) for i in range(n_value_cols)]
    groups = [col(group_row, 1 + i) for i in range(n_value_cols)]
    # If group row is entirely identical to leaf row, collapse.
    if groups == headers:
        return headers, []
    # If all groups empty, collapse.
    if not any(groups):
        return headers, []
    return headers, groups


def _normalize_table(
    table: dict[str, Any], paragraphs: list[dict[str, Any]]
) -> dict[str, Any]:
    cells = table.get("cells") or []
    grid = _cells_to_grid(cells)
    if not grid:
        return {}

    n_cols = max(len(r) for r in grid)
    n_header_rows = _header_rows(cells)
    n_value_cols = max(n_cols - 1, 0)

    # Caption fallback: nearest preceding paragraph (role != pageHeader/pageFooter/pageNumber).
    caption_obj = table.get("caption") or {}
    caption = (caption_obj.get("content") or "").strip()
    if not caption:
        tbl_offset = (table.get("span") or {}).get("offset", 0)
        prev = [
            p for p in paragraphs
            if (p.get("span") or {}).get("offset", 0) < tbl_offset
            and p.get("role") not in ("pageHeader", "pageFooter", "pageNumber")
            and (p.get("content") or "").strip()
        ]
        if prev:
            caption = (prev[-1].get("content") or "").strip()

    company = parse_company(caption)
    title = parse_title(caption) or caption[:120]
    unit = parse_unit(caption)
    statement_type = classify(title)

    period_headers, period_groups = _build_column_headers(grid, n_header_rows, n_value_cols)

    # Build data rows
    rows_out: list[dict[str, Any]] = []
    last_section: str = ""
    section_level: int = 0
    for r in range(n_header_rows, len(grid)):
        line_item = grid[r][0].strip()
        values = [
            grid[r][1 + i].strip() if 1 + i < len(grid[r]) else ""
            for i in range(n_value_cols)
        ]
        if not line_item and not any(values):
            continue

        section = is_section_header(line_item, values)
        subtotal = False if section else is_subtotal(line_item)

        if section:
            last_section = line_item
            section_level = 0
            level = 0
            parent = ""
        elif subtotal:
            level = section_level
            parent = last_section
        else:
            level = section_level + 1 if last_section else 0
            parent = last_section

        rows_out.append(
            {
                "lineItem": line_item,
                "level": level,
                "parentLineItem": parent,
                "isSectionHeader": section,
                "isSubtotal": subtotal,
                "values": values,
            }
        )

    return {
        "tableTitle": title,
        "companyName": company,
        "statementType": statement_type,
        "unit": unit,
        "periodHeaders": period_headers,
        "periodGroupHeaders": period_groups,
        "rows": rows_out,
    }


# --------------------------------------------------------------------------- #
# Envelope to match the custom-analyzer output shape
# --------------------------------------------------------------------------- #

def _wrap_scalar(s: str) -> dict[str, str]:
    return {"valueString": s}


def _wrap_int(n: int) -> dict[str, int]:
    return {"valueInteger": int(n)}


def _wrap_bool(b: bool) -> dict[str, bool]:
    return {"valueBoolean": bool(b)}


def _wrap_str_array(xs: list[str]) -> dict[str, Any]:
    return {"valueArray": [_wrap_scalar(x) for x in xs]}


def _wrap_row(row: dict[str, Any]) -> dict[str, Any]:
    return {
        "valueObject": {
            "lineItem": _wrap_scalar(row["lineItem"]),
            "level": _wrap_int(row["level"]),
            "parentLineItem": _wrap_scalar(row["parentLineItem"]),
            "isSectionHeader": _wrap_bool(row["isSectionHeader"]),
            "isSubtotal": _wrap_bool(row["isSubtotal"]),
            "values": _wrap_str_array(row["values"]),
        }
    }


def _wrap_table(t: dict[str, Any]) -> dict[str, Any]:
    return {
        "valueObject": {
            "tableTitle": _wrap_scalar(t["tableTitle"]),
            "companyName": _wrap_scalar(t["companyName"]),
            "statementType": _wrap_scalar(t["statementType"]),
            "unit": _wrap_scalar(t["unit"]),
            "periodHeaders": _wrap_str_array(t["periodHeaders"]),
            "periodGroupHeaders": _wrap_str_array(t["periodGroupHeaders"]),
            "rows": {"valueArray": [_wrap_row(r) for r in t["rows"]]},
        }
    }


def _wrap_result(tables: list[dict[str, Any]]) -> dict[str, Any]:
    return {
        "contents": [
            {
                "fields": {
                    "financialTables": {
                        "valueArray": [_wrap_table(t) for t in tables]
                    }
                }
            }
        ]
    }


# --------------------------------------------------------------------------- #
# Pipeline
# --------------------------------------------------------------------------- #

def _make_client() -> ContentUnderstandingClient:
    load_dotenv(ROOT / ".env", override=True)
    ep = os.environ["FOUNDRY_ENDPOINT"].split("/api/projects/")[0].rstrip("/") + "/"
    cred = ClientSecretCredential(
        tenant_id=os.environ["AZURE_TENANT_ID"],
        client_id=os.environ["AZURE_CLIENT_ID"],
        client_secret=os.environ["AZURE_CLIENT_SECRET"],
    )
    return ContentUnderstandingClient(endpoint=ep, credential=cred)


def extract_pdf(
    client: ContentUnderstandingClient,
    pdf_path: Path,
    out_dir: Path,
    excel_dir: Path | None,
    keep_other: bool,
) -> Path:
    t0 = time.time()
    data = pdf_path.read_bytes()
    poller = client.begin_analyze_binary(
        analyzer_id="prebuilt-layout",
        binary_input=data,
        content_type="application/pdf",
    )
    raw = poller.result().as_dict()
    ocr_elapsed = time.time() - t0

    contents = raw.get("contents") or []
    paragraphs = contents[0].get("paragraphs", []) if contents else []
    raw_tables = contents[0].get("tables", []) if contents else []
    tables = [_normalize_table(t, paragraphs) for t in raw_tables]
    tables = [t for t in tables if t]
    if not keep_other:
        # Drop non-primary tables to cut noise (customer cares about the 5 primaries).
        primary = [t for t in tables if t["statementType"] != "Other"]
        if primary:
            tables = primary

    out_dir.mkdir(parents=True, exist_ok=True)
    out_path = out_dir / f"{pdf_path.stem}.json"
    out_path.write_text(
        json.dumps(_wrap_result(tables), indent=2, ensure_ascii=False),
        encoding="utf-8",
    )
    total = time.time() - t0
    primary_count = sum(1 for t in tables if t["statementType"] != "Other")
    print(
        f"[ok] {pdf_path.name} -> {out_path.name}  "
        f"({len(tables)} tables, {primary_count} primary, "
        f"OCR {ocr_elapsed:.1f}s / total {total:.1f}s)"
    )

    if excel_dir is not None:
        try:
            xlsx_path = export_document(out_path, excel_dir)
            print(f"[xlsx] {pdf_path.name} -> {xlsx_path.name}")
        except Exception as exc:  # pragma: no cover
            print(f"[xlsx-fail] {pdf_path.name}: {exc}", file=sys.stderr)

    return out_path


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--pdf", type=Path, action="append", help="Path to a single PDF (repeatable).")
    ap.add_argument("--pdf-dir", type=Path, default=DEFAULT_PDF_DIR)
    ap.add_argument("--output-dir", type=Path, default=DEFAULT_OUT_DIR)
    ap.add_argument("--excel", dest="excel", action="store_true", default=True)
    ap.add_argument("--no-excel", dest="excel", action="store_false")
    ap.add_argument("--excel-dir", type=Path, default=None)
    ap.add_argument("--concurrency", type=int, default=3)
    ap.add_argument(
        "--keep-other",
        action="store_true",
        help="Keep all tables, including MD&A / notes / schedules (default: drop Other when any primary statements were found).",
    )
    args = ap.parse_args()

    excel_dir = (args.excel_dir or (args.output_dir / "excel")) if args.excel else None
    client = _make_client()

    if args.pdf:
        pdfs = [p for p in args.pdf if p.exists()]
    else:
        pdfs = sorted(args.pdf_dir.glob("*.pdf"))
    if not pdfs:
        print("[error] no PDFs", file=sys.stderr)
        return 1

    conc = max(1, min(args.concurrency, len(pdfs)))
    print(f"[plan] {len(pdfs)} PDF(s) via prebuilt-layout; concurrency={conc}"
          + (f"; excel -> {excel_dir}" if excel_dir else "; excel disabled"))

    t0 = time.time()
    failures = 0
    if conc == 1:
        for pdf in pdfs:
            try:
                extract_pdf(client, pdf, args.output_dir, excel_dir, args.keep_other)
            except Exception as exc:
                failures += 1
                print(f"[fail] {pdf.name}: {exc}", file=sys.stderr)
    else:
        with ThreadPoolExecutor(max_workers=conc) as pool:
            futs = {
                pool.submit(extract_pdf, client, p, args.output_dir, excel_dir, args.keep_other): p
                for p in pdfs
            }
            for fut in as_completed(futs):
                try:
                    fut.result()
                except Exception as exc:
                    failures += 1
                    print(f"[fail] {futs[fut].name}: {exc}", file=sys.stderr)

    print(f"[done] {len(pdfs) - failures}/{len(pdfs)} succeeded in {time.time() - t0:.1f}s")
    return 0 if failures == 0 else 1


if __name__ == "__main__":
    raise SystemExit(main())
