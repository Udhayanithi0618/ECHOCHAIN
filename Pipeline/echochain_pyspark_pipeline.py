"""
EchoChain — Circular Economy & Secondary Market Lifecycle Analytics
PySpark pipeline (Databricks / Delta Lake compatible)

This script mirrors the pandas prototype used to validate the logic, written
as a PySpark job so it can run directly on a Databricks cluster against
Delta Lake tables. It performs three stages:

  1. CLEAN   - normalize the raw scraped eBay listings
  2. JOIN    - fuzzy-match cleaned listings to internal BOM/warranty records
  3. SCORE   - compute a Circularity Score per model to flag buy-back candidates
"""

from pyspark.sql import SparkSession, functions as F
from pyspark.sql.types import DoubleType

spark = SparkSession.builder.appName("EchoChain_Lifecycle_Analytics").getOrCreate()

# -----------------------------------------------------------------------
# STAGE 1: CLEAN — raw scraped secondary market data
# -----------------------------------------------------------------------
# Source: Delta table of raw Scrapy output (landed via Databricks Autoloader)
raw_df = spark.read.format("delta").load("/mnt/bronze/echochain_raw_listings")
# For local/dev testing, swap the line above for:
# raw_df = spark.read.option("header", True).csv("EchoChain_Raw_Data_Sets.csv")

# Extract numeric processor speed from noisy strings like "o4.2GHz" -> 4.2
extract_speed = F.regexp_extract(F.col("Processor Speed"), r"(\d+\.?\d*)", 1)
cleaned_df = raw_df.withColumn(
    "Processor_Speed_GHz", extract_speed.cast(DoubleType())
)

# Normalize screen size, dropping obviously invalid values (e.g. > 20 inches)
cleaned_df = cleaned_df.withColumn(
    "Screen_Size_in",
    F.when(F.col("Screen Size (inch)").cast(DoubleType()) <= 20,
           F.col("Screen Size (inch)").cast(DoubleType()))
)

# Normalize RAM (fix mis-entered "mb" units) and storage (tb -> gb)
cleaned_df = cleaned_df.withColumn(
    "RAM_GB",
    F.when(F.lower(F.col("Ram Size Unit")) == "mb", F.col("Ram Size") / 1024)
     .otherwise(F.col("Ram Size"))
)
cleaned_df = cleaned_df.withColumn(
    "SSD_GB",
    F.when(F.lower(F.col("SSD Capacity Unit")) == "tb", F.col("SSD Capacity") * 1024)
     .otherwise(F.col("SSD Capacity"))
)
cleaned_df = cleaned_df.withColumn(
    "HDD_GB",
    F.when(F.lower(F.col("Hard Drive Capacity Unit")) == "tb", F.col("Hard Drive Capacity") * 1024)
     .otherwise(F.col("Hard Drive Capacity"))
)
cleaned_df = cleaned_df.withColumn(
    "Storage_GB", F.coalesce(F.col("SSD_GB"), F.col("HDD_GB"))
)

# Simplify Condition into standard buckets
cleaned_df = cleaned_df.withColumn(
    "Condition_clean",
    F.when(F.col("Condition").contains("Refurbished"), "Refurbished")
     .when(F.col("Condition") == "New", "New")
     .when(F.col("Condition") == "Used", "Used")
     .when(F.col("Condition") == "Open box", "Open Box")
     .when(F.col("Condition") == "For parts or not working", "For Parts")
     .otherwise("Unknown")
)

# Classify processor family for matching (regex-based, mirrors fuzzy-match logic)
cleaned_df = cleaned_df.withColumn(
    "Processor_Family",
    F.when(F.lower(F.col("Processor")).contains("i9"), "i9")
     .when(F.lower(F.col("Processor")).contains("i7"), "i7")
     .when(F.lower(F.col("Processor")).contains("i5"), "i5")
     .when(F.lower(F.col("Processor")).contains("i3"), "i3")
     .when(F.lower(F.col("Processor")).contains("celeron"), "celeron")
     .when(F.lower(F.col("Processor")).contains("ryzen 7"), "ryzen7")
     .when(F.lower(F.col("Processor")).contains("ryzen 5"), "ryzen5")
     .otherwise("other")
)

# Screen size bucket, used as part of the fuzzy-match key back to internal SKUs
cleaned_df = cleaned_df.withColumn(
    "Screen_Bucket",
    F.when(F.col("Screen_Size_in") <= 12, "small")
     .when(F.col("Screen_Size_in") <= 14, "mid")
     .when(F.col("Screen_Size_in") <= 15.9, "standard")
     .when(F.col("Screen_Size_in") > 15.9, "large")
     .otherwise("unknown")
)

cleaned_df = cleaned_df.withColumn(
    "Brand_clean", F.lower(F.trim(F.col("Brand")))
)

# Fuzzy-match key: brand + processor family + screen bucket
# (In production, this fuzzy-matching step would use PySpark string-similarity
#  UDFs or a library like `jellyfish`/`rapidfuzz` distributed via mapPartitions
#  to match listing titles against internal SKU descriptions token-by-token.)
cleaned_df = cleaned_df.withColumn(
    "Model_Key",
    F.concat_ws("_", F.col("Brand_clean"), F.col("Processor_Family"), F.col("Screen_Bucket"))
)

cleaned_df.write.format("delta").mode("overwrite") \
    .save("/mnt/silver/echochain_cleaned_listings")

# -----------------------------------------------------------------------
# STAGE 2: JOIN — cleaned listings + internal BOM/warranty table
# -----------------------------------------------------------------------
bom_df = spark.read.format("delta").load("/mnt/bronze/internal_bom_warranty")
# Local/dev equivalent:
# bom_df = spark.read.option("header", True).csv("internal_bom_warranty.csv")

market_agg = cleaned_df.groupBy("Model_Key").agg(
    F.count("Price").alias("Listing_Count"),
    F.avg("Price").alias("Avg_Resale_Price"),
    F.expr("percentile_approx(Price, 0.5)").alias("Median_Resale_Price"),
    F.max("Price").alias("Max_Resale_Price"),
)

joined_df = bom_df.join(market_agg, on="Model_Key", how="inner")

# -----------------------------------------------------------------------
# STAGE 3: SCORE — Circularity Score
# -----------------------------------------------------------------------
joined_df = joined_df.withColumn(
    "Resale_Value_Retention_pct",
    F.round(F.col("Avg_Resale_Price") / F.col("Manufacturing_Cost_USD") * 100, 1)
)

joined_df = joined_df.withColumn(
    "Swappable_Failure_Avg_pct",
    F.round((
        F.col("Warranty_Failure_Display_pct") + F.col("Warranty_Failure_Battery_pct") +
        F.col("Warranty_Failure_Keyboard_pct") + F.col("Warranty_Failure_Storage_pct") +
        F.col("Warranty_Failure_Chassis_pct")
    ) / 5, 1)
)

# Min-max normalize each component 0-100, then blend into final score.
# Weighting: resale retention 55%, swappable-part failure 30% (repair upside),
# motherboard failure 15% penalty (hard/expensive to fix -> lowers circularity).
def minmax_norm(colname, out_name):
    stats = joined_df.agg(F.min(colname).alias("mn"), F.max(colname).alias("mx")).collect()[0]
    return F.round((F.col(colname) - stats["mn"]) / (stats["mx"] - stats["mn"]) * 100, 1).alias(out_name)

joined_df = joined_df.withColumn("resale_score", minmax_norm("Resale_Value_Retention_pct", "resale_score"))
joined_df = joined_df.withColumn("swap_score", minmax_norm("Swappable_Failure_Avg_pct", "swap_score"))
joined_df = joined_df.withColumn("mobo_penalty", minmax_norm("Warranty_Failure_Motherboard_pct", "mobo_penalty"))

joined_df = joined_df.withColumn(
    "Circularity_Score_raw",
    0.55 * F.col("resale_score") + 0.30 * F.col("swap_score") - 0.15 * F.col("mobo_penalty")
)
joined_df = joined_df.withColumn("Circularity_Score", minmax_norm("Circularity_Score_raw", "Circularity_Score"))

joined_df.orderBy(F.desc("Circularity_Score")).write.format("delta").mode("overwrite") \
    .save("/mnt/gold/echochain_circularity_scores")

# Gold table is what Power BI connects to via the Databricks SQL / Delta connector.
print("Pipeline complete. Gold table written to /mnt/gold/echochain_circularity_scores")
