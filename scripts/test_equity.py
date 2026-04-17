"""Quick test: extract TRNO Equity (pages 5-6) with updated schema."""
import json, time, sys
from pathlib import Path
from dotenv import load_dotenv
import os

REPO = Path(__file__).resolve().parent.parent
load_dotenv(REPO / ".env")

from azure.ai.contentunderstanding import ContentUnderstandingClient
from azure.identity import ClientSecretCredential

cred = ClientSecretCredential(
    os.environ["AZURE_TENANT_ID"],
    os.environ["AZURE_CLIENT_ID"],
    os.environ["AZURE_CLIENT_SECRET"],
)
endpoint = os.environ["FOUNDRY_ENDPOINT"].split("/api/projects/")[0].rstrip("/") + "/"
client = ContentUnderstandingClient(endpoint, cred, api_version="2025-11-01")
ANALYZER_ID = "sec_financial_tables_v1"

pdf = REPO / "email" / "attachements" / "trno_10Q_Q3_2024.pdf"
print("Extracting TRNO Equity pages 5-6 ...")
t0 = time.time()
poller = client.begin_analyze_binary(
    analyzer_id=ANALYZER_ID,
    binary_input=pdf.read_bytes(),
    content_type="application/pdf",
    content_range="5-6",
)
result = poller.result().as_dict()
elapsed = time.time() - t0

# Save raw result
out_path = REPO / "output" / "trno_equity_test.json"
out_path.write_text(json.dumps(result, indent=2, ensure_ascii=False), encoding="utf-8")
print(f"Saved to {out_path} in {elapsed:.1f}s\n")

tables = (result.get("contents") or [{}])[0].get("fields", {}).get("financialTables", {}).get("valueArray", [])
print(f"Number of tables: {len(tables)}\n")

for i, t in enumerate(tables, 1):
    obj = t.get("valueObject", {})
    title = obj.get("tableTitle", {}).get("valueString", "?")
    stype = obj.get("statementType", {}).get("valueString", "?")
    headers = [h.get("valueString", "") for h in obj.get("periodHeaders", {}).get("valueArray", [])]
    group_headers = [h.get("valueString", "") for h in obj.get("periodGroupHeaders", {}).get("valueArray", [])]
    rows = obj.get("rows", {}).get("valueArray", [])
    print(f"--- Table {i}: {stype} ---")
    print(f"  Title:              {title}")
    print(f"  periodHeaders:      {headers}")
    print(f"  periodGroupHeaders: {group_headers}")
    print(f"  Rows: {len(rows)}")
    for r in rows:
        ro = r.get("valueObject", {})
        li = ro.get("lineItem", {}).get("valueString", "")
        lvl = ro.get("level", {}).get("valueInteger", "")
        parent = ro.get("parentLineItem", {}).get("valueString", "")
        is_hdr = ro.get("isSectionHeader", {}).get("valueBoolean", False)
        is_sub = ro.get("isSubtotal", {}).get("valueBoolean", False)
        vals = [v.get("valueString", "") for v in ro.get("values", {}).get("valueArray", [])]
        flags = []
        if is_hdr: flags.append("HDR")
        if is_sub: flags.append("SUB")
        flag_str = f" [{','.join(flags)}]" if flags else ""
        print(f"    L{lvl} {li[:55]:<55s} {vals}{flag_str}")
    print()
