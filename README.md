# EchoChain
### Circular Economy & Secondary Market Lifecycle Analytics

EchoChain closes the post-sale "data blind spot" manufacturers face once a product leaves the factory. It joins internal manufacturing/warranty data with scraped secondary-market (eBay-style) listings to compute a **Circularity Score** per product model — flagging which models are strong candidates for a buy-back and refurbishment program.

> Manufacturers track products rigorously until the point of sale. After that, they have no visibility into environmental impact, landfill diversion, or refurbishment potential. EchoChain rebuilds that visibility by connecting internal warranty/failure data to real-world resale market behavior.

---

## Use Case

A Sustainability Executive opens EchoChain and sees that a specific laptop model has a **high Circularity Score**: its motherboard fails frequently (internal warranty data), but its display retains high resale value on eBay (scraped market data). That combination signals a strategic buy-back opportunity — replace the failing part, resell the rest.

---

## Tech Stack

| Layer | Tool | Purpose |
|---|---|---|
| Web Scraping | **Scrapy** (Python) | Extract pricing and condition data from secondary electronics markets |
| Data Lakehouse | **Databricks + Delta Lake** | Unified storage for structured internal data and unstructured scraped web data |
| Big Data Processing | **PySpark** | Clean data, fuzzy-match scraped listings to internal SKUs, aggregate metrics |
| BI / Dashboarding | **Power BI** | Executive reporting and drill-down analysis |

---

## Repository Structure

```
echochain/
├── data/
│   ├── EchoChain_Raw_Data_Sets.csv           # Raw scraped secondary-market listings (bronze)
│   ├── cleaned_secondary_market.csv          # Cleaned listings (silver)
│   ├── internal_bom_warranty.csv             # Synthetic internal BOM/warranty data (bronze)
│   └── echochain_joined_circularity_scores.csv  # Final joined + scored table (gold) — load into Power BI
├── pipeline/
│   └── echochain_pyspark_pipeline.py         # PySpark/Delta Lake pipeline (Databricks-ready)
├── docs/
│   └── EchoChain_Methodology.md              # Full scoring methodology + dashboard guide
└── README.md
```

---

## Pipeline Overview

**Bronze → Silver (Clean)**
Raw scraped listings are normalized: numeric fields extracted from noisy text (e.g. `"o4.2GHz"` → `4.2`), units unified (RAM `mb`→`gb`, storage `tb`→`gb`), 10+ raw condition labels collapsed into 5 clean buckets, and processors classified into families (i3/i5/i7/i9/Celeron/Ryzen) for matching.

**Silver → Gold (Join + Score)**
Cleaned listings are matched to internal model records via a fuzzy key (`Brand + Processor Family + Screen Size Bucket`), then joined with manufacturing cost and component-level warranty failure rates.

**Circularity Score (0–100)**
```
Circularity Score = 0.55 × Resale Value Retention
                   + 0.30 × Swappable-Part Failure Rate
                   − 0.15 × Motherboard Failure Rate
```
- **Resale Value Retention** — avg. resale price ÷ manufacturing cost
- **Swappable-Part Failure Rate** — avg. failure rate of cheap-to-replace parts (display, battery, keyboard, storage, chassis) — a *positive* signal for refurb upside
- **Motherboard Failure Rate** — a *penalty*, since motherboard failures are expensive and hard to repair

Full derivation and normalization steps are in [`docs/EchoChain_Methodology.md`](docs/EchoChain_Methodology.md).

---

## Running the Pipeline

**Local prototyping (pandas):**
```bash
pip install pandas numpy
python pipeline/prototype_pandas_pipeline.py
```

**Production (Databricks / PySpark):**
```bash
# Upload data/*.csv to a Databricks volume or mount, then run:
spark-submit pipeline/echochain_pyspark_pipeline.py
```
The script reads from Delta tables at `/mnt/bronze/...` and writes the scored output to `/mnt/gold/echochain_circularity_scores`, which Power BI connects to via the Databricks SQL / Delta connector.

---

## Power BI Dashboard

Load `data/echochain_joined_circularity_scores.csv` as the primary table. Optionally relate it to `cleaned_secondary_market.csv` on `Model_Key` (many-to-one) for listing-level drill-down.

Recommended visuals:
- **Horizontal bar chart** — Circularity Score by model (ranked)
- **Scatter/bubble chart** — Resale Value Retention % (x) vs. Motherboard Failure % (y), bubble size = listing count
- **Clustered bar chart** — component-level failure breakdown per model
- **Bar chart** — average resale price by condition (New / Used / Refurbished / etc.)
- **KPI cards** — average score, count of "recommended" models, total listings analyzed

See [`docs/EchoChain_Methodology.md`](docs/EchoChain_Methodology.md) for the full build guide.

---

## Data Notes & Limitations

- `internal_bom_warranty.csv` is a **synthetic dataset** generated to simulate a manufacturer's internal BOM/warranty system, since real internal data wasn't available. In production this would come from the manufacturer's ERP/warranty claims system.
- Model matching uses a simplified rule-based key (brand + processor family + screen bucket) rather than full text-similarity fuzzy matching. This is noted in the pipeline script as an upgrade path (e.g. `rapidfuzz`/`jellyfish` distributed via PySpark `mapPartitions`).
- ~1,400 of 4,183 raw listings fell into an "unknown" bucket due to missing screen size/processor data and were excluded from scoring.

---
