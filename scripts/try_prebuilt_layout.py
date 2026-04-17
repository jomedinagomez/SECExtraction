"""Run prebuilt-layout on one PDF and dump the result for inspection.

Prebuilt-layout is CU's OCR-only analyzer — no LLM fields, just layout
(pages, paragraphs, tables, selection marks). Fast and cheap, but returns
tables as raw 2D cells with no hierarchy/semantics.

Usage:
    python scripts/try_prebuilt_layout.py [--pdf PATH] [--out PATH]
"""

from __future__ import annotations

import argparse
import json
import os
import time
from pathlib import Path

from azure.ai.contentunderstanding import ContentUnderstandingClient
from azure.identity import ClientSecretCredential
from dotenv import load_dotenv

ROOT = Path(__file__).resolve().parent.parent
DEFAULT_PDF = ROOT / "email" / "attachements" / "trno_10Q_Q3_2024.pdf"
DEFAULT_OUT = ROOT / "output" / "trno_prebuilt_layout.json"


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--pdf", type=Path, default=DEFAULT_PDF)
    ap.add_argument("--out", type=Path, default=DEFAULT_OUT)
    args = ap.parse_args()

    load_dotenv(ROOT / ".env", override=True)
    ep = os.environ["FOUNDRY_ENDPOINT"].split("/api/projects/")[0].rstrip("/") + "/"
    cred = ClientSecretCredential(
        tenant_id=os.environ["AZURE_TENANT_ID"],
        client_id=os.environ["AZURE_CLIENT_ID"],
        client_secret=os.environ["AZURE_CLIENT_SECRET"],
    )
    client = ContentUnderstandingClient(endpoint=ep, credential=cred)

    data = args.pdf.read_bytes()
    print(f"[go] {args.pdf.name} ({len(data)/1024:.0f} KB) via prebuilt-layout")
    t0 = time.time()
    poller = client.begin_analyze_binary(
        analyzer_id="prebuilt-layout",
        binary_input=data,
        content_type="application/pdf",
    )
    result = poller.result().as_dict()
    elapsed = time.time() - t0

    args.out.parent.mkdir(parents=True, exist_ok=True)
    args.out.write_text(json.dumps(result, indent=2, ensure_ascii=False), encoding="utf-8")

    # Quick summary
    contents = result.get("contents") or []
    n_pages = 0
    n_tables = 0
    n_paragraphs = 0
    for c in contents:
        n_pages += len(c.get("pages", []) or [])
        n_tables += len(c.get("tables", []) or [])
        n_paragraphs += len(c.get("paragraphs", []) or [])

    print(f"[done] {elapsed:.1f}s -> {args.out.relative_to(ROOT)}")
    print(f"  pages: {n_pages}  paragraphs: {n_paragraphs}  tables: {n_tables}")


if __name__ == "__main__":
    main()
