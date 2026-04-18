# SEC Financial Statement Extraction

End-to-end pipeline for extracting the five primary consolidated financial statements (Balance Sheet, Income Statement, Comprehensive Income, Equity Changes, Cash Flows) from SEC 10-K / 10-Q filings using **Azure AI Content Understanding**.

The pipeline preserves row hierarchy (section headers, line items, subtotals) and exports clean multi-sheet Excel workbooks alongside structured JSON.

## Approach

A single Content Understanding **classifier** routes each PDF page to the correct splits (BalanceSheet, IncomeStatement, ComprehensiveIncome, Equity, CashFlow), then a custom **analyzer** (`sec_financial_tables_v2`) extracts each statement with a typed schema:

- **Row-level extraction:** `lineItem`, `level` (0–3 indent), `isSectionHeader`, `isSubtotal`, `values[]`
- **Multi-page merge:** continuation pages are concatenated into a single table per statement
- **Equity-statement guidance:** schema descriptions explicitly handle the transposed orientation (transaction events as rows, account categories as columns)
- **Auto-retry:** any extraction returning empty tables is retried up to 2× to mitigate non-determinism

## Repo Layout

```
analyzers/
  sec_financial_tables_v2.json      # CU analyzer schema (5 statements)
notebooks/
  0_markdown_extraction.ipynb       # Phase 1: prebuilt layout → markdown
  1_extract_via_layout.ipynb        # Heuristic layout-based extraction (alternative)
  2_extraction_comparison.ipynb     # Phase 3: classifier + v2 analyzer (primary)
scripts/
  export_to_excel.py                # Multi-sheet Excel export with hierarchy
  extract_via_layout.py             # Heuristic extractor used by notebook 1
email/
  attachements/                     # Source PDFs (5 sample filings)
output/v2/                          # Generated JSON + Excel per filing
```

## Setup

1. **Python 3.12+** (a `.venv` is recommended).
2. Install dependencies:
   ```powershell
   pip install -r requirements.txt
   ```
3. Copy `.env.sample` to `.env` and fill in your Azure AI Foundry endpoint, model deployment names, and service principal credentials.

## Usage

The primary workflow is **`notebooks/2_extraction_comparison.ipynb`**:

1. Initialize the CU client (cell 2)
2. Deploy the v2 analyzer (cell 4) and classifier (cell 5)
3. Run extraction on all PDFs in `email/attachements/` (cell 7) — runs in parallel with auto-retry
4. Inspect per-table summaries (cell 8)
5. Save JSON + Excel to `output/v2/` (cell 10)

Each Excel workbook contains one sheet per financial statement with indentation-preserving formatting and bolded subtotals/section headers.

## Sample Filings

Five filings ship as test inputs in `email/attachements/`:

| Filing                   | Type | Notes                                               |
|--------------------------|------|-----------------------------------------------------|
| GOOG (2023 10-K)         | 10-K | All 5 statements present                            |
| MSFT (FY2023 10-K)       | 10-K | All 5 statements; multi-segment Equity              |
| BNL (2024 10-K)          | 10-K | Combined Income & Comprehensive Income statement    |
| Safehold (2024 10-K)     | 10-K | All 5 statements                                    |
| TRNO (Q3 2024 10-Q)      | 10-Q | No OCI (typical REIT); transposed Equity statement  |

## Notes

- Raw analyzer JSON responses are not committed (large; see `.gitignore`).
- The `0_markdown_extraction.ipynb` and `1_extract_via_layout.ipynb` notebooks are kept as alternative/comparison approaches that use the prebuilt `prebuilt-layout` model.

