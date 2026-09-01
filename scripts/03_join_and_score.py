"""
EchoChain :: Module - Circularity Score Engine (Gold Layer)
------------------------------------------------------------
Joins:
  - Silver: cleaned secondary-market listings (real, scraped data)
  - Internal: synthetic BOM + warranty claims
  - Internal: synthetic parts-resale values
into one Gold-layer, Power-BI-ready table with a computed Circularity Score
per product family / SKU.

Circularity Score (0-100) = weighted blend of:
  35% Resale Value Retention   -> avg market price / estimated MSRP
  25% Component Reliability    -> 100 - BOM-cost-weighted warranty claim rate
  20% Refurbishment Viability  -> % of listings that are New/Like-New/Refurbished
  20% Parts Circularity        -> BOM-cost-weighted parts resale value %

This mirrors what would run as a PySpark job on Databricks against Delta
tables; see pyspark_databricks_pipeline.py for that version.
"""

import pandas as pd
import numpy as np

MARKET = "/home/claude/EchoChain_Project/data/01_cleaned_market_listings.csv"
BOM = "/home/claude/EchoChain_Project/data/02_synthetic_internal_bom_warranty.csv"
PARTS = "/home/claude/EchoChain_Project/data/03_synthetic_component_resale_values.csv"
OUT = "/home/claude/EchoChain_Project/data/04_circularity_score_powerbi_dataset.csv"
OUT_COMPONENT = "/home/claude/EchoChain_Project/data/05_component_level_detail_powerbi.csv"

market = pd.read_csv(MARKET)
bom = pd.read_csv(BOM)
parts = pd.read_csv(PARTS)

families = bom["product_family_key"].unique().tolist()
market_f = market[market["product_family_key"].isin(families)].copy()

# ---- 1. Market-side aggregation (per product family) ----
def refurb_viable(tier_series):
    return tier_series.isin(["New", "Like-New", "Refurbished"]).mean() * 100

agg = market_f.groupby("product_family_key").agg(
    brand=("brand", "first"),
    listing_count=("listing_id", "count"),
    avg_market_price_usd=("price_usd", "mean"),
    median_market_price_usd=("price_usd", "median"),
    pct_new=("condition_tier", lambda s: (s == "New").mean() * 100),
    pct_used=("condition_tier", lambda s: (s == "Used").mean() * 100),
    pct_refurbished=("condition_tier", lambda s: (s == "Refurbished").mean() * 100),
    pct_end_of_life=("condition_tier", lambda s: (s == "End-of-Life").mean() * 100),
    refurb_viability_pct=("condition_tier", refurb_viable),
    avg_ram_gb=("ram_gb", "mean"),
    avg_storage_gb=("total_storage_gb", "mean"),
    common_screen_tier=("screen_tier", lambda s: s.mode().iat[0] if not s.mode().empty else "Unk"),
    processor_family=("processor_family", lambda s: s.mode().iat[0] if not s.mode().empty else "Unk"),
).reset_index()

# ---- 2. BOM-side: MSRP + weighted reliability ----
msrp = bom[bom["component"] == "TOTAL / MSRP"][["product_family_key", "sku_id", "bom_cost_usd"]] \
    .rename(columns={"bom_cost_usd": "estimated_msrp_usd"})

comp_bom = bom[bom["component"] != "TOTAL / MSRP"].copy()
comp_bom["weighted_fail"] = comp_bom["cost_share_pct"] / 100 * comp_bom["warranty_claim_rate_pct"]
reliability = comp_bom.groupby("product_family_key").agg(
    weighted_failure_rate_pct=("weighted_fail", "sum"),
).reset_index()
reliability["component_reliability_score"] = (100 - reliability["weighted_failure_rate_pct"] * 4).clip(0, 100)

top_fail = comp_bom.loc[comp_bom.groupby("product_family_key")["warranty_claim_rate_pct"].idxmax()][
    ["product_family_key", "component"]].rename(columns={"component": "top_failing_component"})

# ---- 3. Parts resale: weighted parts-circularity score ----
parts_j = parts.merge(comp_bom[["product_family_key", "component", "cost_share_pct"]],
                       on=["product_family_key", "component"], how="left")
parts_j["weighted_resale"] = parts_j["cost_share_pct"] / 100 * parts_j["resale_pct_of_bom_cost"]
parts_agg = parts_j.groupby("product_family_key").agg(
    parts_circularity_raw=("weighted_resale", "sum"),
).reset_index()
parts_agg["parts_circularity_score"] = (parts_agg["parts_circularity_raw"] * 1.8).clip(0, 100)

top_resale = parts.loc[parts.groupby("product_family_key")["resale_pct_of_bom_cost"].idxmax()][
    ["product_family_key", "component"]].rename(columns={"component": "highest_resale_component"})

# ---- 4. Assemble Gold table ----
gold = agg.merge(msrp, on="product_family_key", how="left") \
          .merge(reliability, on="product_family_key", how="left") \
          .merge(parts_agg, on="product_family_key", how="left") \
          .merge(top_fail, on="product_family_key", how="left") \
          .merge(top_resale, on="product_family_key", how="left")

gold["resale_value_retention_pct"] = (gold["avg_market_price_usd"] / gold["estimated_msrp_usd"] * 100).clip(0, 150)
gold["resale_value_retention_score"] = gold["resale_value_retention_pct"].clip(0, 100)

gold["circularity_score"] = (
    0.35 * gold["resale_value_retention_score"] +
    0.25 * gold["component_reliability_score"] +
    0.20 * gold["refurb_viability_pct"] +
    0.20 * gold["parts_circularity_score"]
).round(1)

def bucket(score):
    if score >= 70:
        return "High"
    if score >= 50:
        return "Medium"
    return "Low"

gold["circularity_tier"] = gold["circularity_score"].apply(bucket)
gold["buyback_refurb_recommended"] = (
    (gold["circularity_score"] >= 65) & (gold["parts_circularity_score"] >= 40)
)

for c in ["avg_market_price_usd", "median_market_price_usd", "estimated_msrp_usd",
          "resale_value_retention_pct", "avg_ram_gb", "avg_storage_gb"]:
    gold[c] = gold[c].round(2)

gold = gold.sort_values("circularity_score", ascending=False).reset_index(drop=True)
gold.to_csv(OUT, index=False)

# ---- 5. Component-level detail table (for drill-through in Power BI) ----
comp_detail = comp_bom.merge(
    parts[["product_family_key", "component", "component_resale_value_usd", "resale_pct_of_bom_cost"]],
    on=["product_family_key", "component"], how="left"
).merge(gold[["product_family_key", "brand", "circularity_tier"]], on="product_family_key", how="left")
comp_detail.to_csv(OUT_COMPONENT, index=False)

print(f"Gold circularity dataset: {len(gold)} product families -> {OUT}")
print(f"Component-level detail:   {len(comp_detail)} rows -> {OUT_COMPONENT}")
print("\nTop 5 by Circularity Score:")
print(gold[["product_family_key", "brand", "circularity_score", "circularity_tier",
            "top_failing_component", "highest_resale_component"]].head(5).to_string(index=False))
