"""
EchoChain :: Module - Synthetic Internal Manufacturing Data Generator
------------------------------------------------------------------------
IMPORTANT / TRANSPARENCY NOTE
The uploaded raw dataset (EchoChain_Raw_Data_Sets.csv) contains ONLY scraped
secondary-market (eBay-style) listings. It does not include the internal
Bill-of-Materials, warranty/failure-claim records, or parts-level resale
listings that the EchoChain use case describes joining against.

To deliver a complete, runnable proof-of-concept of the full pipeline
(web scrape -> lakehouse join -> circularity score -> BI dashboard),
this script generates REALISTIC, CLEARLY-LABELED SYNTHETIC data for:

  1. Internal BOM + Warranty Claims  (per product family, per component)
  2. Secondary-market PARTS resale values (per component) - the raw CSV only
     has whole-unit listings, so component-level resale (e.g. "this laptop's
     display panel resells for $80 on eBay") is simulated here.

In production, both of these would be REAL tables already living in the
Databricks Lakehouse: (1) from the manufacturer's PLM/ERP + warranty system,
and (2) from a second Scrapy spider crawling the "parts" category of
secondary-market sites. Replace this script's output with those real tables
and the rest of the pipeline (03_join_and_score.py) works unchanged.

Random seed is fixed for reproducibility.
"""

import pandas as pd
import numpy as np

np.random.seed(42)

MARKET_PATH = "/home/claude/EchoChain_Project/data/01_cleaned_market_listings.csv"
OUT_BOM = "/home/claude/EchoChain_Project/data/02_synthetic_internal_bom_warranty.csv"
OUT_PARTS = "/home/claude/EchoChain_Project/data/03_synthetic_component_resale_values.csv"

MIN_LISTINGS_PER_FAMILY = 5

df = pd.read_csv(MARKET_PATH)

# Drop the low-information / unmatched bucket (brand=Other, processor family unknown)
# -- in production these would be resolved via a real internal SKU lookup table.
valid = df[~((df["brand"] == "Other/Unbranded") & (df["processor_family"].isna() |
             (df["processor_family"] == "Other/Unspecified")))]

fam_counts = valid["product_family_key"].value_counts()
families = fam_counts[fam_counts >= MIN_LISTINGS_PER_FAMILY].index.tolist()

COMPONENTS = ["Motherboard", "Display Panel", "Battery", "RAM Module",
              "Storage (SSD/HDD)", "Keyboard/Chassis", "Power Adapter"]

# Base BOM cost share (% of total unit cost) per component - typical laptop teardown ratios
BASE_COST_SHARE = {
    "Motherboard": 0.34, "Display Panel": 0.22, "Battery": 0.08,
    "RAM Module": 0.06, "Storage (SSD/HDD)": 0.10,
    "Keyboard/Chassis": 0.14, "Power Adapter": 0.06,
}

# Base warranty failure/claim rate per component (typical 3-yr claim rates, %)
BASE_FAILURE_RATE = {
    "Motherboard": 6.5, "Display Panel": 2.5, "Battery": 4.0,
    "RAM Module": 1.0, "Storage (SSD/HDD)": 2.0,
    "Keyboard/Chassis": 3.0, "Power Adapter": 2.5,
}

# Base parts resale value as % of original component BOM cost (secondary parts market)
BASE_RESALE_PCT = {
    "Motherboard": 0.18, "Display Panel": 0.55, "Battery": 0.20,
    "RAM Module": 0.45, "Storage (SSD/HDD)": 0.40,
    "Keyboard/Chassis": 0.15, "Power Adapter": 0.25,
}

def estimate_unit_msrp(row):
    """Rough synthetic 'original MSRP' model driven by spec tier -- stands in for
    a real manufacturer price list."""
    base = 350
    proc_bump = {"i3": 80, "i5": 200, "i7": 400, "i9": 650, "celeron": -50,
                 "pentium": -30, "ryzen 5": 220, "ryzen 7": 380}.get(str(row["processor_family"]).lower(), 60)
    ram_val = row["ram_gb"] if pd.notna(row["ram_gb"]) else 8
    storage_val = row["total_storage_gb"] if pd.notna(row["total_storage_gb"]) else 256
    ram_bump = ram_val * 8
    storage_bump = storage_val * 0.15
    screen_bump = 60 if row["screen_tier"] == "16in+" else 20
    return round(base + proc_bump + ram_bump + storage_bump + screen_bump, 2)

bom_rows = []
parts_rows = []

for i, fam in enumerate(families):
    sub = valid[valid["product_family_key"] == fam]
    rep = sub.iloc[0]  # representative spec row for this family
    sku_id = f"SKU-{i+1:04d}"
    msrp = estimate_unit_msrp(rep)

    # Family-level "personality" perturbation so components differ meaningfully
    # (this is what lets a specific SKU realistically show "motherboard fails a lot,
    # display resells well" per the use case narrative)
    mobo_fail_boost = np.random.uniform(-2.0, 6.0)
    display_resale_boost = np.random.uniform(-0.10, 0.20)

    for comp in COMPONENTS:
        cost_share = BASE_COST_SHARE[comp] * np.random.uniform(0.9, 1.1)
        bom_cost = round(msrp * cost_share, 2)

        fail_rate = BASE_FAILURE_RATE[comp]
        if comp == "Motherboard":
            fail_rate += mobo_fail_boost
        fail_rate = round(max(0.2, fail_rate * np.random.uniform(0.85, 1.15)), 2)

        bom_rows.append({
            "sku_id": sku_id, "product_family_key": fam, "brand": rep["brand"],
            "component": comp, "bom_cost_usd": bom_cost,
            "cost_share_pct": round(cost_share * 100, 1),
            "warranty_claim_rate_pct": fail_rate,
        })

        resale_pct = BASE_RESALE_PCT[comp]
        if comp == "Display Panel":
            resale_pct += display_resale_boost
        resale_pct = round(min(0.9, max(0.05, resale_pct * np.random.uniform(0.85, 1.15))), 3)
        parts_rows.append({
            "sku_id": sku_id, "product_family_key": fam, "component": comp,
            "component_resale_value_usd": round(bom_cost * resale_pct, 2),
            "resale_pct_of_bom_cost": round(resale_pct * 100, 1),
            "avg_parts_listing_count": int(np.random.randint(3, 60)),
        })

    bom_rows.append({
        "sku_id": sku_id, "product_family_key": fam, "brand": rep["brand"],
        "component": "TOTAL / MSRP", "bom_cost_usd": msrp,
        "cost_share_pct": 100.0, "warranty_claim_rate_pct": np.nan,
    })

bom_df = pd.DataFrame(bom_rows)
parts_df = pd.DataFrame(parts_rows)

bom_df.to_csv(OUT_BOM, index=False)
parts_df.to_csv(OUT_PARTS, index=False)

print(f"Product families modeled: {len(families)}")
print(f"BOM/warranty rows: {len(bom_df)} -> {OUT_BOM}")
print(f"Parts resale rows: {len(parts_df)} -> {OUT_PARTS}")
