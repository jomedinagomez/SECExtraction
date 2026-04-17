"""Fast extraction using pypdf (much faster than pdfplumber for text)."""
import sys, io
sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding="utf-8", errors="replace")
from pypdf import PdfReader

TARGETS = [
    (r"c:\Users\jomedin\Documents\SECExtraction\email\attachements\Safehold 2024 10-K (1) 1.pdf",
     "SAFE 10-K (PDF offset)",
     {"BalanceSheet": 49, "CashFlows": 53}),
    (r"c:\Users\jomedin\Documents\SECExtraction\email\attachements\trno_10Q_Q3_2024.pdf",
     "TRNO 10-Q",
     {"BalanceSheet_p3": 3, "CashFlows_p7": 7}),
]

out = []
for path, label, pages in TARGETS:
    reader = PdfReader(path)
    out.append(f"\n{'#'*80}\n# {label}  ({len(reader.pages)} pages)\n{'#'*80}")
    for name, pg in pages.items():
        if pg < 1 or pg > len(reader.pages):
            out.append(f"\n--- {name} p{pg} OUT OF RANGE ---")
            continue
        txt = reader.pages[pg - 1].extract_text() or ""
        out.append(f"\n--- {name} (page {pg}) ---")
        for line in txt.splitlines()[:80]:
            out.append(repr(line))
print("\n".join(out))
