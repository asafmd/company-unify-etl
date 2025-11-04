"""
PySpark transformation job for Company Unification
--------------------------------------------------
Steps:
1. Read staging tables from Postgres
2. Clean and normalize company names
3. Perform entity matching (exact + fuzzy + AI confirmation)
4. Write unified view to dwh schema
"""

import os
import re
import json
from dotenv import load_dotenv
from pyspark.sql import SparkSession, functions as F
from pyspark.sql.types import *
from pyspark.sql.window import Window

# Optional fuzzy matching + AI
from pyspark.sql.functions import udf
from difflib import SequenceMatcher
import openai  # only if AI confirmation is enabled

# ------------------------------------------------------------------
# Setup
# ------------------------------------------------------------------
load_dotenv()
PG_JDBC_URL = os.getenv("PG_JDBC_URL")
PG_USER = os.getenv("PG_USER")
PG_PASS = os.getenv("PG_PASS")
PG_JDBC_JAR = os.getenv("PG_JDBC_JAR", r"C:\spark\jars\postgresql-42.6.0.jar")

OPENAI_API_KEY = os.getenv("OPENAI_API_KEY")  # optional
openai.api_key = OPENAI_API_KEY

spark = (
    SparkSession.builder
    .appName("CompanyUnifyTransform")
    .config("spark.jars", PG_JDBC_JAR)
    .config("spark.driver.extraClassPath", PG_JDBC_JAR)
    .config("spark.sql.shuffle.partitions", 8)
    .getOrCreate()
)

# ------------------------------------------------------------------
# Helper Functions
# ------------------------------------------------------------------
def normalize_name(name: str):
    """Standardize company names for matching."""
    if not name:
        return None
    name = name.lower().strip()
    name = re.sub(r"[^a-z0-9& ]", "", name)
    name = re.sub(r"\bpty\b|\bltd\b|\bplc\b|\binc\b|\bco\b", "", name)
    return re.sub(r"\s+", " ", name).strip()

normalize_udf = F.udf(normalize_name, StringType())

def fuzzy_score(a, b):
    if not a or not b:
        return 0.0
    return SequenceMatcher(None, a, b).ratio()

fuzzy_udf = F.udf(fuzzy_score, FloatType())

def llm_confirm(a, b):
    """
    Confirm if two company names refer to the same entity using an LLM.
    Only triggered for fuzzy matches above 0.8 and below 0.95.
    """
    if not OPENAI_API_KEY:
        return False
    try:
        prompt = f"Do '{a}' and '{b}' refer to the same company entity? Answer only 'yes' or 'no'."
        resp = openai.ChatCompletion.create(
            model="gpt-4o-mini",
            messages=[{"role": "user", "content": prompt}],
            temperature=0
        )
        ans = resp.choices[0].message.content.strip().lower()
        return "yes" in ans
    except Exception:
        return False

llm_confirm_udf = F.udf(llm_confirm, BooleanType())

# ------------------------------------------------------------------
# Read from Postgres (staging)
# ------------------------------------------------------------------
def read_pg_table(table):
    return (
        spark.read.format("jdbc")
        .option("url", PG_JDBC_URL)
        .option("dbtable", table)
        .option("user", PG_USER)
        .option("password", PG_PASS)
        .option("driver", "org.postgresql.Driver")
        .load()
    )

df_abr = read_pg_table("stg.abr_bulk")
df_cc = read_pg_table("stg.commoncrawl_raw")

# ------------------------------------------------------------------
# Transformations
# ------------------------------------------------------------------
df_abr_clean = (
    df_abr
    .withColumn("norm_name", normalize_udf("entity_name"))
    .withColumn("source", F.lit("ABR"))
)

df_cc_clean = (
    df_cc
    .withColumn("norm_name", normalize_udf("extracted_name"))
    .withColumn("source", F.lit("COMMONCRAWL"))
)

# Step 1: Exact match
df_exact = df_abr_clean.join(
    df_cc_clean,
    on=[df_abr_clean.norm_name == df_cc_clean.norm_name],
    how="inner"
).select(
    df_abr_clean["*"],
    df_cc_clean["domain"],
    df_cc_clean["extracted_industry"]
).dropDuplicates(["abn", "domain"])

# Step 2: Fuzzy match (names not matched in exact)
df_fuzzy_candidates = (
    df_abr_clean.crossJoin(df_cc_clean)
    .filter(F.col("df_cc_clean.norm_name").isNotNull() & F.col("df_abr_clean.norm_name").isNotNull())
    .withColumn("fuzzy_score", fuzzy_udf("df_abr_clean.norm_name", "df_cc_clean.norm_name"))
    .filter(F.col("fuzzy_score") > 0.8)
)

# Optional AI confirmation for borderline cases
df_fuzzy_confirmed = df_fuzzy_candidates.withColumn(
    "ai_confirmed",
    F.when((F.col("fuzzy_score") < 0.95), llm_confirm_udf("df_abr_clean.norm_name", "df_cc_clean.norm_name")).otherwise(F.lit(True))
).filter("ai_confirmed = true")

# Step 3: Union all matched
df_unified = (
    df_exact.select("abn", "entity_name", "entity_type", "address", "postcode", "state", "domain", "extracted_industry")
    .unionByName(
        df_fuzzy_confirmed.select(
            F.col("df_abr_clean.abn").alias("abn"),
            F.col("df_abr_clean.entity_name").alias("entity_name"),
            F.col("df_abr_clean.entity_type").alias("entity_type"),
            F.col("df_abr_clean.address").alias("address"),
            F.col("df_abr_clean.postcode").alias("postcode"),
            F.col("df_abr_clean.state").alias("state"),
            F.col("df_cc_clean.domain").alias("domain"),
            F.col("df_cc_clean.extracted_industry").alias("extracted_industry")
        )
    )
).withColumn("processed_at", F.current_timestamp())

# ------------------------------------------------------------------
# Write to Postgres (DWH)
# ------------------------------------------------------------------
(
    df_unified.write
    .format("jdbc")
    .option("url", PG_JDBC_URL)
    .option("dbtable", "dwh.unified_companies")
    .option("user", PG_USER)
    .option("password", PG_PASS)
    .option("driver", "org.postgresql.Driver")
    .mode("overwrite")
    .save()
)

print(f"✅ Transformation complete: {df_unified.count()} unified rows written.")
spark.stop()
