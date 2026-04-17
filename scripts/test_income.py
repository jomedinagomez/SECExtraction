"""Quick test: extract TRNO Income Statement (page 4) to check periodGroupHeaders."""
import json, time, os
from pathlib import Path
from dotenv import load_dotenv

REPO = Path(__file__).resolve().parent.parent
load_dotenv(REPO / ".env")

from azure.ai.contentunderstanding import ContentUnderstandingClient
from azure.identity import ClientSecretCredential

cred = ClientSecretCredential(os.environ["AZURE_TENANT_ID"], os.environ["AZURE_CLIENT_ID"], os.environ["AZURE_CLIENT_SECRET"])
endpoint = os.environ["FOUNDRY_ENDPOINT"].split("/api/projects/")[0].rstrip("/") + "/"
client = ContentUnderstandingClient(endpoint, cred, api_version="2025-11-01")

pdf = REPO / "email" / "attachements" / "trno_10Q_Q3_2024.pdf"
print("Extracting TRNO Income Statement (page 4)...")
t0 = time.time()
poller = client.begin_analyze_binary(analyzer_id="sec_financial_tables_v1", binary_input=pdf.read_bytes(), content_type="application/pdf", content_range="4")
result = poller.result().as_dict()
print(f"Done in {time.time()-t0:.1f}s\n")

tables = (result.get("contents") or [{}])[0].get("fields", {}).get("financialTables", {}).get("valueArray", [])
for i, t in enumerate(tables, 1):
    obj = t.get("valueObject", {})
    title = obj.get("tableTitle", {}).get("valueString", "?")
    headers = [h.get("valueString", "") for h in obj.get("periodHeaders", {}).get("valueArray", [])]
    group_headers = [h.get("valueString", "") for h in obj.get("periodGroupHeaders", {}).get("valueArray", [])]
    rows = obj.get("rows", {}).get("valueArray", [])
    print(f"[{i}] {title}")
    print(f"    periodHeaders:      {headers}")
    print(f"    periodGroupHeaders: {group_headers}")
    print(f"    rows: {len(rows)}")
    for r in rows[:5]:
        ro = r.get("valueObject", {})
        li = ro.get("lineItem", {}).get("valueString", "")
        vals = [v.get("valueString", "") for v in ro.get("values", {}).get("valueArray", [])]
        print(f"      {li[:55]:<55s} {vals}")
