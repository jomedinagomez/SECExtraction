"""Extract raw text of known financial-statement pages to inspect indentation."""
import pdfplumber

TARGETS = [
    # (path, label, 1-based page numbers to dump)
    (r"c:\Users\jomedin\Documents\SECExtraction\email\attachements\Safehold 2024 10-K (1) 1.pdf",
     "SAFE 10-K",
     {"BalanceSheet": 47, "Operations": 48, "Comprehensive": 49, "ChangesInEquity": 50, "CashFlows": 51}),
    (r"c:\Users\jomedin\Documents\SECExtraction\email\attachements\bnl_10K_2024.pdf",
     "BNL 10-K",
     {"BalanceSheet": 64, "IncomeAndCompInc": 65, "Equity": 66, "CashFlows": 67}),
    (r"c:\Users\jomedin\Documents\SECExtraction\email\attachements\trno_10Q_Q3_2024.pdf",
     "TRNO 10-Q",
     {"BalanceSheet": 2, "Operations": 3, "Equity": 4, "CashFlows": 5}),
]

for path, label, pages in TARGETS:
    with pdfplumber.open(path) as pdf:
        n = len(pdf.pages)
        print(f"\n{'#'*80}\n# {label}  ({n} pages total)  path={path}\n{'#'*80}")
        for name, pg in pages.items():
            if pg < 1 or pg > n:
                print(f"\n--- {name} p{pg} [OUT OF RANGE] ---")
                continue
            txt = pdf.pages[pg - 1].extract_text(x_tolerance=1, keep_blank_chars=True) or ""
            print(f"\n--- {name} (page {pg}) ---")
            # Show up to 70 non-empty lines preserving leading spaces
            shown = 0
            for line in txt.splitlines():
                if not line.strip() and shown == 0:
                    continue
                print(repr(line))  # repr shows leading whitespace clearly
                shown += 1
                if shown >= 70:
                    break
