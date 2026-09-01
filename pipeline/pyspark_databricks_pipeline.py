"""
EchoChain :: Production Pipeline (Databricks / PySpark / Delta Lake)
=======================================================================
This is the PRODUCTION version of the logic prototyped in
01_clean_market_data.py, 02_generate_synthetic_internal_data.py, and
03_join_and_score.py. Run this as a Databricks Job / Workflow on a
cluster, scheduled after the Scrapy crawl lands raw files in cloud
storage (S3/ADLS) and internal BOM/warranty tables are refreshed from
the ERP/PLM system.

Layers (Medallion Architecture):
  BRONZE  -> raw scraped listings + raw internal exports, ingested as-is
  SILVER  -> cleaned, typed, deduplicated, standardized
  GOLD    -> fuzzy-matched, joined, aggregated Circularity Score table
             consumed directly by Power BI (DirectQuery or Import)

Requires: pyspark, delta-spark  (pip install pyspark delta-spark)
Run on Databricks Runtime with Delta Lake enabled (default on DBR).
"""

from pyspark.sql import SparkSession, functions as F, Window
from pyspark.sql.types import DoubleType

spark = (
    SparkSession.builder.appName("EchoChain_CircularityPipeline")
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
    .getOrCreate()
)

CATALOG = "echochain"
BRONZE_MARKET_PATH = "/mnt/bronze/secondary_market_listings/"   # written by Scrapy job (Parquet/JSON)
BRONZE_BOM_PATH = "/mnt/bronze/internal_bom_warranty/"          # nightly ERP/PLM export
SILVER_MARKET_TABLE = f"{CATALOG}.silver.market_listings"
GOLD_CIRCULARITY_TABLE = f"{CATALOG}.gold.circularity_scores"


# ---------------------------------------------------------------------
# BRONZE -> SILVER : clean & standardize scraped listings
# ---------------------------------------------------------------------
def clean_market_bronze_to_silver():
    raw = spark.read.format("parquet").load(BRONZE_MARKET_PATH)

    null_tokens = ["undefined", "unknown", "not applicable", "n/a", "na", "none", ""]

    def null_clean(col):
        return F.when(F.lower(F.trim(col)).isin(null_tokens), None).otherwise(F.trim(col))

    df = raw
    for c in ["Brand", "Color", "Condition", "GPU", "Processor", "OS", "Storage Type"]:
        df = df.withColumn(c, null_clean(F.col(c)))

    df = df.dropDuplicates()

    df = df.withColumn("brand", F.initcap(F.lower(F.col("Brand"))))
    df = df.withColumn("price_usd", F.col("Price").cast(DoubleType()))
    df = df.filter((F.col("price_usd") > 0) & (F.col("price_usd") < 10000))

    condition_map = {
        "new": "New", "open box": "Open Box", "used": "Used",
        "for parts or not working": "For Parts / Not Working",
        "seller refurbished": "Refurbished - Seller Grade",
        "certified - refurbished": "Refurbished - Certified",
        "excellent - refurbished": "Refurbished - Excellent",
        "very good - refurbished": "Refurbished - Very Good",
        "good - refurbished": "Refurbished - Good",
    }
    mapping_expr = F.create_map([F.lit(x) for pair in condition_map.items() for x in pair])
    df = df.withColumn("condition_grade",
                        F.coalesce(mapping_expr[F.lower(F.col("Condition"))], F.lit("Unknown")))

    tier_map = {"New": "New", "Open Box": "Like-New", "Used": "Used",
                "For Parts / Not Working": "End-of-Life"}
    tier_expr = F.when(F.col("condition_grade").contains("Refurbished"), "Refurbished") \
                 .otherwise(F.coalesce(
                     F.create_map([F.lit(x) for pair in tier_map.items() for x in pair])[F.col("condition_grade")],
                     F.lit("Unknown")))
    df = df.withColumn("condition_tier", tier_expr)

    # Processor parsing (Intel/AMD/Apple, family, generation) via regex
    df = df.withColumn("processor_lc", F.lower(F.col("Processor")))
    df = df.withColumn(
        "processor_brand",
        F.when(F.col("processor_lc").rlike(r"intel|\bi[3579]\b"), "Intel")
         .when(F.col("processor_lc").rlike(r"amd|ryzen"), "AMD")
         .when(F.col("processor_lc").rlike(r"apple|\bm[123]\b"), "Apple")
         .otherwise("Other")
    )
    df = df.withColumn("processor_family",
                        F.regexp_extract(F.col("processor_lc"), r"i[3579]|celeron|pentium|ryzen ?\d?", 0))
    df = df.withColumn("processor_generation",
                        F.regexp_extract(F.col("processor_lc"), r"(\d{1,2})(st|nd|rd|th) generation", 1).cast("int"))

    # Unify storage/RAM to GB
    df = df.withColumn("ssd_capacity_gb",
                        F.when(F.lower(F.col("SSD Capacity Unit")) == "tb", F.col("SSD Capacity") * 1000)
                         .otherwise(F.col("SSD Capacity")))
    df = df.withColumn("hdd_capacity_gb",
                        F.when(F.lower(F.col("Hard Drive Capacity Unit")) == "tb", F.col("Hard Drive Capacity") * 1000)
                         .otherwise(F.col("Hard Drive Capacity")))
    df = df.withColumn("total_storage_gb",
                        F.coalesce(F.col("ssd_capacity_gb"), F.lit(0)) + F.coalesce(F.col("hdd_capacity_gb"), F.lit(0)))
    df = df.withColumn("ram_gb",
                        F.when(F.lower(F.col("Ram Size Unit")) == "mb", F.col("Ram Size") / 1000)
                         .otherwise(F.col("Ram Size")))

    df = df.withColumn("screen_size_inch", F.col("Screen Size (inch)").cast(DoubleType()))
    df = df.withColumn("screen_size_inch",
                        F.when(F.col("screen_size_inch").between(6, 20), F.col("screen_size_inch")).otherwise(None))

    # Fuzzy-match key -> links a listing to an internal SKU family
    df = df.withColumn(
        "product_family_key",
        F.concat_ws("_", F.col("brand"), F.col("processor_family"),
                    F.when(F.col("ram_gb").isNull(), "Unk").otherwise(F.round(F.col("ram_gb")).cast("string")),
                    F.when(F.col("total_storage_gb") == 0, "Unk").otherwise(F.col("total_storage_gb").cast("string")))
    )

    (df.write.format("delta").mode("overwrite")
       .option("mergeSchema", "true")
       .saveAsTable(SILVER_MARKET_TABLE))
    return df


# ---------------------------------------------------------------------
# SILVER + INTERNAL -> GOLD : join, aggregate, score
# ---------------------------------------------------------------------
def build_gold_circularity_table():
    market = spark.table(SILVER_MARKET_TABLE)
    bom = spark.read.format("delta").load(BRONZE_BOM_PATH)  # internal component/warranty table

    market_agg = market.groupBy("product_family_key", "brand").agg(
        F.count("*").alias("listing_count"),
        F.avg("price_usd").alias("avg_market_price_usd"),
        F.expr("percentile_approx(price_usd, 0.5)").alias("median_market_price_usd"),
        (F.avg(F.when(F.col("condition_tier").isin("New", "Like-New", "Refurbished"), 1.0).otherwise(0.0)) * 100)
            .alias("refurb_viability_pct"),
    )

    reliability = (
        bom.groupBy("product_family_key")
           .agg(F.sum(F.col("cost_share_pct") / 100 * F.col("warranty_claim_rate_pct")).alias("weighted_failure_rate_pct"),
                F.first("estimated_msrp_usd").alias("estimated_msrp_usd"))
           .withColumn("component_reliability_score",
                       F.greatest(F.lit(0.0), F.least(F.lit(100.0), 100 - F.col("weighted_failure_rate_pct") * 4)))
    )

    gold = market_agg.join(reliability, "product_family_key", "left")
    gold = gold.withColumn("resale_value_retention_score",
                            F.least(F.lit(100.0), F.col("avg_market_price_usd") / F.col("estimated_msrp_usd") * 100))
    gold = gold.withColumn(
        "circularity_score",
        F.round(
            0.35 * F.col("resale_value_retention_score") +
            0.25 * F.col("component_reliability_score") +
            0.20 * F.col("refurb_viability_pct") +
            0.20 * F.lit(50.0),  # parts_circularity_score joined similarly from parts table in full version
            1)
    )
    gold = gold.withColumn(
        "circularity_tier",
        F.when(F.col("circularity_score") >= 70, "High")
         .when(F.col("circularity_score") >= 50, "Medium")
         .otherwise("Low")
    )

    (gold.write.format("delta").mode("overwrite")
        .saveAsTable(GOLD_CIRCULARITY_TABLE))
    return gold


if __name__ == "__main__":
    clean_market_bronze_to_silver()
    build_gold_circularity_table()
    # Power BI connects directly to GOLD_CIRCULARITY_TABLE via the Databricks SQL
    # connector (DirectQuery) for live executive dashboarding.
