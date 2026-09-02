# EchoChain — Data Pipeline & Circularity Score Methodology

## 1. Data Sources

| Source | Role | Rows | Format |
|---|---|---|---|
| `EchoChain_Raw_Data_Sets.csv` (your upload) | Scraped secondary-market listings (eBay-style) | 4,183 | Bronze/raw |
| `internal_bom_warranty.csv` (synthetic, generated for this project) | Internal manufacturing cost + component warranty failure rates per model | 37 model keys | Bronze/raw |

Since you don't have real internal manufacturing/warranty data, I generated a **synthetic but realistic internal dataset**: manufacturing cost estimates and per-component warranty failure rates (Motherboard, Display, Battery, Keyboard, Storage, Chassis) for the most common laptop models found in your scraped data. This stands in for the "internal BOM/warranty" system EchoChain is designed to join against. You should clearly label this as a synthetic/simulated dataset in your submission, and note that in a real deployment it would come from the manufacturer's ERP/warranty claims system.

## 2. Cleaning (Bronze → Silver)

The raw scraped data had realistic messiness:
- Numeric fields stored as text with stray characters (`"o4.2GHz"` → `4.2`)
- Inconsistent units (RAM in `mb` vs `gb`, storage in `tb` vs `gb`)
- ~50% missing values in Screen Size, RAM, and Storage fields
- 10 different raw Condition labels collapsed into 5 clean buckets (New, Used, Refurbished, Open Box, For Parts, Unknown)
- Free-text processor strings (`"intel core i5-8259u"`, `"i5-7200u"`, `"8350u, core i5"`) classified into processor families (i3/i5/i7/i9/Celeron/Ryzen) for matching

## 3. Matching / Join (Silver → Gold)

Real internal SKU numbers don't appear on eBay listings, so listings are matched to internal models using a **fuzzy match key**: `Brand + Processor Family + Screen Size Bucket` (small/mid/standard/large). This is a simplified stand-in for the PySpark token-similarity fuzzy-matching your project spec calls for (e.g. matching listing titles to SKU descriptions using libraries like `rapidfuzz` in a distributed `mapPartitions` job — noted in the PySpark script).

## 4. Circularity Score

The score answers: *"Is this model a good buy-back/refurbishment candidate?"*

**Formula (0–100 scale):**
```
Circularity Score = 0.55 × Resale Value Retention
                   + 0.30 × Swappable-Part Failure Rate
                   - 0.15 × Motherboard Failure Rate
```

- **Resale Value Retention** = avg. resale price ÷ manufacturing cost — high value means the used market still pays a lot for this model.
- **Swappable-Part Failure Rate** = average failure rate of parts that are *cheap and easy to replace* (display, battery, keyboard, storage, chassis) — high failure here is actually a *good* signal, because refurbishing = swap the cheap broken part, resell the rest.
- **Motherboard Failure Rate** is a *penalty* — motherboard failures are expensive and hard to repair, so high motherboard failure drags a model's circularity down even if resale value is high.

This directly reproduces your example use case: a model with high display resale value but high motherboard failure gets flagged as a strong buy-back candidate specifically *because* the failure is concentrated in something other than the expensive-to-fix part.

## 5. Output Files

| File | Purpose |
|---|---|
| `cleaned_secondary_market.csv` | Cleaned listing-level data (Silver layer) |
| `internal_bom_warranty.csv` | Synthetic internal BOM/warranty data |
| `echochain_joined_circularity_scores.csv` | Final joined, scored table — **load this into Power BI** |
| `echochain_pyspark_pipeline.py` | Production-style PySpark/Delta Lake version of the full pipeline, for your Databricks module |

## 6. Suggested Power BI Dashboard

- **Bar chart**: Circularity Score by model, sorted descending — highlights top buy-back candidates
- **Scatter plot**: Resale Value Retention (x) vs. Motherboard Failure Rate (y), bubble size = listing count — visually shows the "high value / low hard-failure" sweet spot
- **Table with drill-down**: model → component-level failure breakdown
- **KPI card**: count of models above a Circularity Score threshold (e.g. 60) = "recommended for buy-back program"

## 7. Limitations to state in your report

- Internal BOM/warranty data is synthetic (no real manufacturer dataset was available)
- Model matching uses a simplified rule-based key rather than true text-similarity fuzzy matching — call this out as a "v1 approach, upgradeable with NLP-based matching"
- ~1,400 listings fell into an "unknown" model bucket due to missing screen size/processor data and were excluded from scoring
