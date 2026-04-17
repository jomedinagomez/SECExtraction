"""Quick test of page detection logic against existing TRNO prebuilt-layout result."""
import json, re

_FS_TITLE = [
    (re.compile(r"consolidated\s+balance\s+sheets?", re.I), "BalanceSheet"),
    (re.compile(r"consolidated\s+statements?\s+of\s+(operations|income|earnings)", re.I), "IncomeStatement"),
    (re.compile(r"consolidated\s+statements?\s+of\s+comprehensive\s+(income|loss)", re.I), "ComprehensiveIncome"),
    (re.compile(r"consolidated\s+statements?\s+of.{0,20}equity", re.I), "Equity"),
    (re.compile(r"consolidated\s+statements?\s+of\s+cash\s+flows?", re.I), "CashFlow"),
]
_MAX_TITLE_LEN = 150

def _page_from_source(src):
    m = re.search(r"D\((\d+)", src)
    return int(m.group(1)) if m else None

data = json.load(open("output/trno_prebuilt_layout.json", encoding="utf-8"))
contents = data["contents"][0]
paragraphs = contents.get("paragraphs", [])
tables = contents.get("tables", [])

# Step 1: hits (filtered by length)
hits = []
for p in paragraphs:
    text = p.get("content", "")
    if len(text) > _MAX_TITLE_LEN:
        continue
    pg = _page_from_source(p.get("source", ""))
    if pg is None:
        continue
    for pat, stype in _FS_TITLE:
        if pat.search(text):
            hits.append((pg, stype))
            break

print("Paragraph hits:")
for pg, stype in hits:
    print(f"  page {pg}: {stype}")

# Step 2: cluster
all_hit_pages = sorted(set(pg for pg, _ in hits))
clusters = []
cur = [all_hit_pages[0]]
for pg in all_hit_pages[1:]:
    if pg - cur[-1] <= 3:
        cur.append(pg)
    else:
        clusters.append(cur)
        cur = [pg]
clusters.append(cur)
print(f"\nClusters: {clusters}")
fs_cluster = max(clusters, key=len)
fs_start, fs_end = fs_cluster[0], fs_cluster[-1]
print(f"Best cluster: pages {fs_start}-{fs_end}")

# Step 3: extend for multi-page tables
for t in tables:
    tpages = set()
    for c in t.get("cells", []):
        pg = _page_from_source(c.get("source", ""))
        if pg is not None:
            tpages.add(pg)
    if tpages and min(tpages) >= fs_start and min(tpages) <= fs_end:
        if max(tpages) > fs_end:
            print(f"  Extending from {fs_end} to {max(tpages)} (multi-page table)")
            fs_end = max(fs_end, max(tpages))

print(f"\nFinal FS range: pages {fs_start}-{fs_end}")
print(f'content_range = "{fs_start}-{fs_end}"')
total = contents.get("endPageNumber", "?")
print(f"Reduction: {fs_end - fs_start + 1}/{total} pages")

# Debug: show all hits with length and role to filter noise
print("\n\nDebug: paragraph details for hit pages")
for p in paragraphs:
    pg = _page_from_source(p.get("source", ""))
    if pg is None or pg not in {2, 3, 4, 5, 7, 10, 11}:
        continue
    text = p.get("content", "")
    role = p.get("role", "")
    matched = False
    for pat, stype in _FS_TITLE:
        if pat.search(text):
            matched = True
            print(f"  pg={pg} len={len(text):3d} role={role!r:20s} type={stype:25s} {text[:100]}")
            break
