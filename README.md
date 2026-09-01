# EchoChain

**Post-sale product lifecycle intelligence for circular economy decisions.**

Manufacturers track products rigorously until the point of sale — after that, the
lifecycle becomes a data blind spot. EchoChain closes that gap by joining scraped
secondary-market (eBay-style) listing data with internal manufacturing Bill-of-Materials
and warranty-claim data, producing a **Circularity Score** that flags products worth a
strategic buy-back / refurbishment program (e.g. "motherboard fails often, but the
display resells at a premium — refurbish and resell the panel").

## Architecture

```
Scrapy spiders  →  Bronze (raw)  →  Silver (cleaned/standardized)  →  Gold (joined + scored)  →  Power BI
     ↑                                        ↑
secondary market                    internal BOM / warranty data
   listings                              (ERP / PLM system)
```

| Layer | Tool | Purpose |
|---|---|---|
| Ingestion | Scrapy | Crawl secondary-market listings (whole-unit + parts) |
| Storage | Databricks + Delta Lake | Unified lakehouse for structured + scraped data |
| Processing | PySpark | Clean, fuzzy-match, aggregate at scale |
| Reporting | Power BI | Executive Circularity Score dashboard |

## Repo layout

```
EchoChain/
├── scraper/
│   └── scrapy_secondary_market_spider.py   # Scrapy spiders (listings + parts)
├── pipeline/
│   └── pyspark_databricks_pipeline.py      # Production Databricks/PySpark job
├── scripts/                                # Local/portable pandas prototypes
│   ├── 01_clean_market_data.py             # Bronze -> Silver
│   ├── 02_generate_synthetic_internal_data.py  # Synthetic BOM/warranty/parts data
│   └── 03_join_and_score.py                # Silver + internal -> Gold (Circularity Score)
├── data/                                   # Sample CSV outputs (Silver + Gold layers)
│   ├── 01_cleaned_market_listings.csv
│   ├── 02_synthetic_internal_bom_warranty.csv
│   ├── 03_synthetic_component_resale_values.csv
│   ├── 04_circularity_score_powerbi_dataset.csv   # Power BI source table
│   └── 05_component_level_detail_powerbi.csv      # Power BI drill-through table
├── requirements.txt
└── README.md
```

## Circularity Score

A 0–100 blended score per product family:

- **35%** Resale Value Retention — avg. secondary-market price ÷ estimated MSRP
- **25%** Component Reliability — 100 − BOM-cost-weighted warranty claim rate
- **20%** Refurbishment Viability — % of listings that are New / Like-New / Refurbished
- **20%** Parts Circularity — BOM-cost-weighted component resale value %

## Running locally

```bash
pip install -r requirements.txt

python scripts/01_clean_market_data.py
python scripts/02_generate_synthetic_internal_data.py
python scripts/03_join_and_score.py
```

Outputs land in `data/` — `04_circularity_score_powerbi_dataset.csv` and
`05_component_level_detail_powerbi.csv` are ready to load straight into Power BI.

## Production deployment

`pipeline/pyspark_databricks_pipeline.py` is the Databricks Job / Workflow version:
Bronze tables refresh from the Scrapy crawl (cloud storage) and the ERP/PLM export,
and the Gold `circularity_scores` Delta table is queried directly by Power BI
(DirectQuery).

## Note on internal data

The raw upstream dataset only contains scraped secondary-market listings. Internal
BOM/warranty and parts-resale tables are **clearly-labeled synthetic data** generated
by `scripts/02_generate_synthetic_internal_data.py` to make the pipeline runnable
end-to-end. In production, swap these for real exports from the manufacturer's
ERP/PLM and warranty systems — the rest of the pipeline works unchanged.
