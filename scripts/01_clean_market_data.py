"""
EchoChain :: Module - Secondary Market Data Cleaning
------------------------------------------------------
Simulates the PySpark cleaning stage of the Databricks Lakehouse "Bronze -> Silver"
transformation for data scraped by the Scrapy spiders (see scrapy_secondary_market_spider.py).

Input : EchoChain_Raw_Data_Sets.csv  (raw scraped eBay-style laptop listings)
Output: 01_cleaned_market_listings.csv (Silver layer - analytics ready)

NOTE: This script is written in pandas for local/portable execution, but every
transformation maps 1:1 to a PySpark equivalent. See pyspark_databricks_pipeline.py
for the production Databricks/Delta Lake version of this same logic.
"""

import pandas as pd
import numpy as np
import re

RAW_PATH = "/mnt/user-data/uploads/EchoChain_Raw_Data_Sets.csv"
OUT_PATH = "/home/claude/EchoChain_Project/data/01_cleaned_market_listings.csv"

# --------------------------------------------------------------------------
# 1. LOAD
# --------------------------------------------------------------------------
df = pd.read_csv(RAW_PATH)
df["listing_id"] = np.arange(1, len(df) + 1)

# --------------------------------------------------------------------------
# 2. NORMALIZE "MISSING" SENTINELS
#    Scraped text commonly encodes nulls as literal strings.
# --------------------------------------------------------------------------
NULL_TOKENS = {"undefined", "unknown", "not applicable", "n/a", "na", "none", "", "nan"}

def clean_null_tokens(val):
    if pd.isna(val):
        return np.nan
    s = str(val).strip()
    if s.lower() in NULL_TOKENS:
        return np.nan
    return s

text_cols = ["Brand", "Color", "Features", "Condition", "Condition Description",
             "Seller Note", "GPU", "Processor", "Processor Speed Unit", "Type",
             "OS", "Storage Type", "Hard Drive Capacity Unit", "SSD Capacity Unit",
             "Ram Size Unit"]
for c in text_cols:
    df[c] = df[c].apply(clean_null_tokens)

# --------------------------------------------------------------------------
# 3. DEDUPLICATE EXACT ROWS
# --------------------------------------------------------------------------
before = len(df)
df = df.drop_duplicates(subset=[c for c in df.columns if c != "listing_id"]).copy()
dupes_removed = before - len(df)

# --------------------------------------------------------------------------
# 4. BRAND STANDARDIZATION
# --------------------------------------------------------------------------
df["brand"] = df["Brand"].str.lower().str.strip().fillna("other")
BRAND_MAP = {"hp": "HP", "dell": "Dell", "lenovo": "Lenovo", "acer": "Acer",
             "samsung": "Samsung", "microsoft": "Microsoft", "lg": "LG",
             "asus": "Asus", "auo": "AUO", "other": "Other/Unbranded"}
df["brand"] = df["brand"].map(BRAND_MAP).fillna(df["brand"].str.title())

# --------------------------------------------------------------------------
# 5. PRICE / CURRENCY
# --------------------------------------------------------------------------
df["price_usd"] = pd.to_numeric(df["Price"], errors="coerce").round(2)
df = df[(df["price_usd"] > 0) & (df["price_usd"] < 10000)]  # drop impossible prices

# --------------------------------------------------------------------------
# 6. CONDITION GRADE STANDARDIZATION  (this drives refurbishment-viability metrics)
# --------------------------------------------------------------------------
COND_MAP = {
    "new": "New",
    "open box": "Open Box",
    "used": "Used",
    "for parts or not working": "For Parts / Not Working",
    "seller refurbished": "Refurbished - Seller Grade",
    "certified - refurbished": "Refurbished - Certified",
    "excellent - refurbished": "Refurbished - Excellent",
    "very good - refurbished": "Refurbished - Very Good",
    "good - refurbished": "Refurbished - Good",
    "undefined": "Unknown",
}
df["condition_grade"] = (
    df["Condition"].str.lower().str.strip().map(COND_MAP).fillna("Unknown")
)
# Simplified 5-bucket tier used for scoring / BI slicers
COND_TIER = {
    "New": "New",
    "Open Box": "Like-New",
    "Refurbished - Certified": "Refurbished",
    "Refurbished - Excellent": "Refurbished",
    "Refurbished - Very Good": "Refurbished",
    "Refurbished - Good": "Refurbished",
    "Refurbished - Seller Grade": "Refurbished",
    "Used": "Used",
    "For Parts / Not Working": "End-of-Life",
    "Unknown": "Unknown",
}
df["condition_tier"] = df["condition_grade"].map(COND_TIER)

# --------------------------------------------------------------------------
# 7. FEATURES -> count + boolean flags for common ones (useful BI slicers)
# --------------------------------------------------------------------------
df["feature_count"] = df["Features"].fillna("").apply(
    lambda s: 0 if s == "" else len([x for x in s.split(",") if x.strip()])
)
for feat, colname in [("touchscreen", "has_touchscreen"),
                       ("backlit keyboard", "has_backlit_keyboard"),
                       ("bluetooth", "has_bluetooth"),
                       ("wi-fi", "has_wifi")]:
    df[colname] = df["Features"].fillna("").str.lower().str.contains(feat)

# --------------------------------------------------------------------------
# 8. PROCESSOR PARSING (brand / family / generation)
# --------------------------------------------------------------------------
def parse_processor(raw):
    if pd.isna(raw):
        return pd.Series(["Unknown", "Unknown", np.nan])
    s = str(raw).lower().replace("inter ", "intel ")  # fix common scrape typo
    brand = "Intel" if "intel" in s or re.search(r"\bi[3579]\b", s) else \
            "AMD" if "amd" in s or "ryzen" in s else \
            "Apple" if "apple" in s or re.search(r"\bm[123]\b", s) else \
            "Other"
    fam_match = re.search(r"i[3579]|celeron|pentium|ryzen\s?\d?|core 2 duo|atom", s)
    family = fam_match.group(0).strip() if fam_match else "Other/Unspecified"
    gen_match = re.search(r"(\d{1,2})(st|nd|rd|th)\s+generation", s)
    generation = int(gen_match.group(1)) if gen_match else np.nan
    return pd.Series([brand, family, generation])

df[["processor_brand", "processor_family", "processor_generation"]] = df["Processor"].apply(parse_processor)

# Processor speed -> unify to GHz
speed = pd.to_numeric(df["Processor Speed"], errors="coerce")
unit = df["Processor Speed Unit"].str.upper()
df["processor_speed_ghz"] = np.where(unit == "MHZ", speed / 1000, speed)

# --------------------------------------------------------------------------
# 9. GPU
# --------------------------------------------------------------------------
df["gpu_brand"] = df["GPU"].str.title().fillna("Unknown")

# --------------------------------------------------------------------------
# 10. DISPLAY / RESOLUTION
# --------------------------------------------------------------------------
df["display_width_px"] = pd.to_numeric(df["Width of the Display"], errors="coerce")
df["display_height_px"] = pd.to_numeric(df["Height of the Display"], errors="coerce")

screen = df["Screen Size (inch)"].astype(str).str.rstrip(".")
screen = pd.to_numeric(screen, errors="coerce")
df["screen_size_inch"] = screen.where(screen.between(6, 20))  # plausible laptop range

# --------------------------------------------------------------------------
# 11. STORAGE -> unify all capacities to GB
# --------------------------------------------------------------------------
def to_gb(value, unit):
    v = pd.to_numeric(value, errors="coerce")
    u = str(unit).lower() if pd.notna(unit) else ""
    return v * 1000 if u == "tb" else v

df["hdd_capacity_gb"] = [to_gb(v, u) for v, u in zip(df["Hard Drive Capacity"], df["Hard Drive Capacity Unit"])]
df["ssd_capacity_gb"] = [to_gb(v, u) for v, u in zip(df["SSD Capacity"], df["SSD Capacity Unit"])]
df["total_storage_gb"] = df[["hdd_capacity_gb", "ssd_capacity_gb"]].sum(axis=1, skipna=True)
df.loc[df["total_storage_gb"] == 0, "total_storage_gb"] = np.nan

df["storage_type"] = df["Storage Type"].str.upper().fillna("Unknown")

# --------------------------------------------------------------------------
# 12. RAM -> unify to GB
# --------------------------------------------------------------------------
def ram_to_gb(value, unit):
    v = pd.to_numeric(value, errors="coerce")
    u = str(unit).lower() if pd.notna(unit) else ""
    return v / 1000 if u == "mb" else v

df["ram_gb"] = [ram_to_gb(v, u) for v, u in zip(df["Ram Size"], df["Ram Size Unit"])]

# --------------------------------------------------------------------------
# 13. OS / TYPE
# --------------------------------------------------------------------------
df["os"] = df["OS"].str.title().fillna("Unknown")
df["device_type"] = df["Type"].fillna("Other").str.title()

# --------------------------------------------------------------------------
# 14. FUZZY-MATCH KEY -> "product_family_key"
#     Stand-in for the PySpark fuzzy-matching stage that links a scraped
#     listing to an internal manufacturer SKU (BOM/warranty system) when no
#     exact SKU/model-number field exists in the scraped source.
#     Key = Brand + Processor Family + rounded RAM + rounded Storage tier + Screen tier
# --------------------------------------------------------------------------
def storage_tier(gb):
    if pd.isna(gb):
        return "Unk"
    for t in [16, 32, 64, 128, 256, 512, 1000, 2000]:
        if gb <= t:
            return f"{t}GB" if t < 1000 else f"{t//1000}TB"
    return "2TB+"

def ram_tier(gb):
    if pd.isna(gb):
        return "Unk"
    for t in [2, 4, 8, 16, 32]:
        if gb <= t:
            return f"{t}GB"
    return "32GB+"

def screen_tier(inch):
    if pd.isna(inch):
        return "Unk"
    if inch < 12:
        return "Sub-12in"
    if inch < 14:
        return "12-14in"
    if inch < 16:
        return "14-16in"
    return "16in+"

df["storage_tier"] = df["total_storage_gb"].apply(storage_tier)
df["ram_tier"] = df["ram_gb"].apply(ram_tier)
df["screen_tier"] = df["screen_size_inch"].apply(screen_tier)

df["product_family_key"] = (
    df["brand"] + "_" +
    df["processor_family"].fillna("Unk").str.replace(" ", "") + "_" +
    df["ram_tier"] + "_" + df["storage_tier"] + "_" + df["screen_tier"]
)

# --------------------------------------------------------------------------
# 15. FINAL COLUMN SELECTION
# --------------------------------------------------------------------------
final_cols = [
    "listing_id", "product_family_key", "brand", "device_type",
    "price_usd", "condition_grade", "condition_tier",
    "gpu_brand", "processor_brand", "processor_family", "processor_generation",
    "processor_speed_ghz", "os", "storage_type", "hdd_capacity_gb",
    "ssd_capacity_gb", "total_storage_gb", "storage_tier",
    "ram_gb", "ram_tier", "screen_size_inch", "screen_tier",
    "display_width_px", "display_height_px", "color", "feature_count",
    "has_touchscreen", "has_backlit_keyboard", "has_bluetooth", "has_wifi",
]
df["color"] = df["Color"].str.title().fillna("Unknown")

clean = df[final_cols].reset_index(drop=True)
clean.to_csv(OUT_PATH, index=False)

print(f"Raw rows:        {before}")
print(f"Duplicates removed: {dupes_removed}")
print(f"Clean rows:       {len(clean)}")
print(f"Unique product_family_key groups: {clean['product_family_key'].nunique()}")
print(f"Saved -> {OUT_PATH}")
