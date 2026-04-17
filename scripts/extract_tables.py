"""Extract SEC financial tables from PDFs using Azure Content Understanding.

Runs the full analyzer pipeline against PDF files:

1. (Re)deploys the analyzer using the JSON template in analyzers/.
2. Analyzes each PDF with the deployed analyzer.
3. Writes the raw result JSON to --output-dir (one .json per PDF).

By default runs against every PDF in email/attachements/ using
analyzers/sec_financial_tables_v1.json as the schema, writing results to
output/. All paths are overridable via CLI flags.

Requires .env with:
  FOUNDRY_ENDPOINT            (Foundry project URL)
  AZURE_TENANT_ID/_CLIENT_ID/_CLIENT_SECRET  (service principal)
  GPT41_MODEL_DEPLOYMENT      (completion deployment name)
  EMBEDDING_MODEL_DEPLOYMENT  (embedding deployment name)

Usage:
    # All PDFs, default paths
    python scripts/extract_tables.py

    # Single PDF, custom output
    python scripts/extract_tables.py --pdf email/attachements/trno_10Q_Q3_2024.pdf --output-dir output/

    # Custom analyzer/template
    python scripts/extract_tables.py --analyzer-id sec_financial_tables_v2 \\
        --template analyzers/sec_financial_tables_v2.json

    # Skip redeploy (analyzer already exists)
    python scripts/extract_tables.py --no-redeploy
"""

from __future__ import annotations

import argparse
import json
import os
import sys
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path

from azure.ai.contentunderstanding import ContentUnderstandingClient
from azure.identity import ClientSecretCredential
from dotenv import load_dotenv

ROOT = Path(__file__).resolve().parent.parent
DEFAULT_TEMPLATE = ROOT / "analyzers" / "sec_financial_tables_v1.json"
DEFAULT_PDF_DIR = ROOT / "email" / "attachements"
DEFAULT_OUT_DIR = ROOT / "output"

# Reuse the Excel exporter as a library function.
sys.path.insert(0, str(Path(__file__).resolve().parent))
from export_to_excel import export_document  # noqa: E402


def _make_client() -> ContentUnderstandingClient:
    load_dotenv(ROOT / ".env", override=True)
    endpoint = (
        os.environ["FOUNDRY_ENDPOINT"].split("/api/projects/")[0].rstrip("/") + "/"
    )
    cred = ClientSecretCredential(
        tenant_id=os.environ["AZURE_TENANT_ID"],
        client_id=os.environ["AZURE_CLIENT_ID"],
        client_secret=os.environ["AZURE_CLIENT_SECRET"],
    )
    return ContentUnderstandingClient(endpoint=endpoint, credential=cred)


def deploy_analyzer(
    client: ContentUnderstandingClient, analyzer_id: str, template_path: Path
) -> None:
    tmpl = json.loads(template_path.read_text(encoding="utf-8"))
    tmpl["models"] = {
        "completion": os.environ["GPT41_MODEL_DEPLOYMENT"],
        "embedding": os.environ["EMBEDDING_MODEL_DEPLOYMENT"],
    }
    print(f"[deploy] {analyzer_id} <- {template_path.name}")
    client.begin_create_analyzer(
        analyzer_id=analyzer_id, resource=tmpl, allow_replace=True
    ).result()


def analyze_pdf(
    client: ContentUnderstandingClient,
    analyzer_id: str,
    pdf_path: Path,
    out_dir: Path,
    excel_dir: Path | None = None,
) -> Path:
    data = pdf_path.read_bytes()
    t0 = time.time()
    poller = client.begin_analyze_binary(
        analyzer_id=analyzer_id,
        binary_input=data,
        content_type="application/pdf",
    )
    result = poller.result().as_dict()
    elapsed = time.time() - t0

    out_dir.mkdir(parents=True, exist_ok=True)
    out_path = out_dir / f"{pdf_path.stem}.json"
    out_path.write_text(
        json.dumps(result, indent=2, ensure_ascii=False), encoding="utf-8"
    )

    tables = (
        (result.get("contents") or [{}])[0]
        .get("fields", {})
        .get("financialTables", {})
        .get("valueArray")
        or []
    )
    print(
        f"[ok] {pdf_path.name} -> {out_path.name}  "
        f"({len(tables)} tables, {elapsed:.1f}s)"
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
    ap.add_argument(
        "--analyzer-id",
        default="sec_financial_tables_v1",
        help="Analyzer ID to deploy/use (default: sec_financial_tables_v1).",
    )
    ap.add_argument(
        "--template",
        type=Path,
        default=DEFAULT_TEMPLATE,
        help=f"Schema JSON to deploy (default: {DEFAULT_TEMPLATE.relative_to(ROOT)}).",
    )
    ap.add_argument(
        "--pdf",
        type=Path,
        action="append",
        help="Path to a single PDF. Repeatable. If omitted, uses --pdf-dir.",
    )
    ap.add_argument(
        "--pdf-dir",
        type=Path,
        default=DEFAULT_PDF_DIR,
        help=f"Directory of PDFs (default: {DEFAULT_PDF_DIR.relative_to(ROOT)}).",
    )
    ap.add_argument(
        "--output-dir",
        type=Path,
        default=DEFAULT_OUT_DIR,
        help=f"Where to write result JSONs (default: {DEFAULT_OUT_DIR.relative_to(ROOT)}).",
    )
    ap.add_argument(
        "--no-redeploy",
        action="store_true",
        help="Skip analyzer (re)deployment; use the existing --analyzer-id as-is.",
    )
    ap.add_argument(
        "--excel",
        dest="excel",
        action="store_true",
        default=True,
        help="Also export each result JSON to an .xlsx workbook (default: on).",
    )
    ap.add_argument(
        "--no-excel",
        dest="excel",
        action="store_false",
        help="Skip the Excel export step.",
    )
    ap.add_argument(
        "--excel-dir",
        type=Path,
        default=None,
        help="Where to write .xlsx files (default: <output-dir>/excel).",
    )
    ap.add_argument(
        "--concurrency",
        type=int,
        default=1,
        help="Analyze this many PDFs in parallel (default: 1; try 3-4 for a batch).",
    )
    args = ap.parse_args()
    excel_dir = None
    if args.excel:
        excel_dir = args.excel_dir or (args.output_dir / "excel")

    client = _make_client()

    if not args.no_redeploy:
        if not args.template.exists():
            print(f"[error] template not found: {args.template}", file=sys.stderr)
            return 2
        deploy_analyzer(client, args.analyzer_id, args.template)

    if args.pdf:
        pdfs = [p for p in args.pdf if p.exists()]
        missing = [str(p) for p in args.pdf if not p.exists()]
        if missing:
            print(f"[error] missing PDFs: {missing}", file=sys.stderr)
            return 2
    else:
        pdfs = sorted(args.pdf_dir.glob("*.pdf"))

    if not pdfs:
        print("[error] no PDFs to analyze", file=sys.stderr)
        return 1

    concurrency = max(1, min(args.concurrency, len(pdfs)))
    print(
        f"[plan] {len(pdfs)} PDF(s) with analyzer '{args.analyzer_id}'"
        f"; concurrency={concurrency}"
        + (f"; excel -> {excel_dir}" if excel_dir else "; excel disabled")
    )
    failures = 0
    t0 = time.time()
    if concurrency == 1:
        for pdf in pdfs:
            try:
                analyze_pdf(
                    client, args.analyzer_id, pdf, args.output_dir, excel_dir=excel_dir
                )
            except Exception as exc:  # pragma: no cover
                failures += 1
                print(f"[fail] {pdf.name}: {exc}", file=sys.stderr)
    else:
        with ThreadPoolExecutor(max_workers=concurrency) as pool:
            fut_to_pdf = {
                pool.submit(
                    analyze_pdf,
                    client,
                    args.analyzer_id,
                    pdf,
                    args.output_dir,
                    excel_dir,
                ): pdf
                for pdf in pdfs
            }
            for fut in as_completed(fut_to_pdf):
                pdf = fut_to_pdf[fut]
                try:
                    fut.result()
                except Exception as exc:  # pragma: no cover
                    failures += 1
                    print(f"[fail] {pdf.name}: {exc}", file=sys.stderr)
    total = time.time() - t0

    print(
        f"[done] {len(pdfs) - failures}/{len(pdfs)} succeeded in {total:.1f}s"
    )
    return 0 if failures == 0 else 1


if __name__ == "__main__":
    raise SystemExit(main())
