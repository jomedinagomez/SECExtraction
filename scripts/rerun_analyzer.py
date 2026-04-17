"""Replace sec_financial_tables_v1 with the updated schema (now including
periodGroupHeaders) and re-analyze all PDFs in email/attachements/, writing
results back to output/.
"""
from __future__ import annotations

import json
import os
from pathlib import Path

from azure.ai.contentunderstanding import ContentUnderstandingClient
from azure.identity import ClientSecretCredential
from dotenv import load_dotenv

ROOT = Path(__file__).resolve().parent.parent
ANALYZER_ID = "sec_financial_tables_v1"
TEMPLATE_PATH = ROOT / "analyzers" / "sec_financial_tables_v1.json"
PDF_DIR = ROOT / "email" / "attachements"
OUT_DIR = ROOT / "output"


def main() -> int:
    load_dotenv(ROOT / ".env", override=True)
    endpoint = os.environ["FOUNDRY_ENDPOINT"].split("/api/projects/")[0].rstrip("/") + "/"
    cred = ClientSecretCredential(
        tenant_id=os.environ["AZURE_TENANT_ID"],
        client_id=os.environ["AZURE_CLIENT_ID"],
        client_secret=os.environ["AZURE_CLIENT_SECRET"],
    )
    client = ContentUnderstandingClient(endpoint=endpoint, credential=cred)

    tmpl = json.loads(TEMPLATE_PATH.read_text(encoding="utf-8"))
    tmpl["models"] = {
        "completion": os.environ["GPT41_MODEL_DEPLOYMENT"],
        "embedding": os.environ["EMBEDDING_MODEL_DEPLOYMENT"],
    }

    print(f"[reset] {ANALYZER_ID} <- {TEMPLATE_PATH.name}")
    client.begin_create_analyzer(analyzer_id=ANALYZER_ID, resource=tmpl, allow_replace=True).result()

    OUT_DIR.mkdir(parents=True, exist_ok=True)
    pdfs = sorted(PDF_DIR.glob("*.pdf"))
    for pdf in pdfs:
        print(f"[analyze] {pdf.name}")
        data = pdf.read_bytes()
        poller = client.begin_analyze_binary(
            analyzer_id=ANALYZER_ID,
            binary_input=data,
            content_type="application/pdf",
        )
        result = poller.result()
        out_path = OUT_DIR / f"{pdf.stem}.json"
        # Dump full result
        out_path.write_text(json.dumps(result.as_dict(), indent=2, ensure_ascii=False), encoding="utf-8")
        tables = (result.as_dict().get("contents") or [{}])[0].get("fields", {}).get("financialTables", {}).get("valueArray") or []
        print(f"  -> {out_path.name}  ({len(tables)} tables)")
    print("done")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
