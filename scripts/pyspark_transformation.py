#!/usr/bin/env python3
"""
PySpark transformation job for Company Unification (improved)

Summary of changes / improvements:
- Safer name normalization (better suffix removal, punctuation handling)
- Blocking strategy to avoid crossJoin explosion: use first N chars of normalized name
- Fuzzy matching via Python UDF (SequenceMatcher) but only applied within blocks
- LLM confirmation done on driver for a limited number of *borderline* candidates (configurable)
  — avoids calling OpenAI from executors (which is slow/expensive and unreliable)
- Clearer column aliasing to avoid ambiguous column references
- Safer JDBC read/write using withOptions and configurable write mode
- Logging and configurable limits for LLM confirmations and candidate collection
"""

import os
import re
import json
import logging
from datetime import datetime, timezone
from typing import List, Tuple, Dict

from dotenv import load_dotenv

from pyspark.sql import SparkSession, functions as F, types as T
from pyspark.sql.window import Window

# fuzzy
from difflib import SequenceMatcher

# Optional: only used if OPENAI_API_KEY is set and LLM confirmation is enabled.
import openai
import time

# ---------------------------
# Config + environment
# ---------------------------
load_dotenv()
PG_JDBC_URL = os.getenv("PG_JDBC_URL")         # e.g. "jdbc:postgresql://host:5432/dbname"
PG_USER = os.getenv("PG_USER")
PG_PASS = os.getenv("PG_PASS")
PG_JDBC_JAR = os.getenv("PG_JDBC_JAR")         # path to postgres driver jar if needed
OPENAI_API_KEY = os.getenv("OPENAI_API_KEY")  # optional; if not present, LLM confirmation disabled

# Tune these for your cluster
BLOCK_KEY_CHARS = int(os.getenv("BLOCK_KEY_CHARS", "3"))   # first N chars used for blocking
FUZZY_THRESHOLD_LO = float(os.getenv("FUZZY_THRESHOLD_LO", "0.80"))
FUZZY_THRESHOLD_HI = float(os.getenv("FUZZY_THRESHOLD_HI", "0.95"))
LLM_CONFIRM_LIMIT = int(os.getenv("LLM_CONFIRM_LIMIT", "2000"))  # max borderline pairs to check with LLM
LLM_RATE_LIMIT_SLEEP = float(os.getenv("LLM_RATE_LIMIT_SLEEP", "0.2"))  # seconds between LLM calls
WRITE_MODE = os.getenv("WRITE_MODE", "append")  # or "overwrite"

# Basic logging
logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s", force=True)
logger = logging.getLogger("company_unify_transform")

if OPENAI_API_KEY:
    openai.api_key = OPENAI_API_KEY

# ---------------------------
# Spark session
# ---------------------------
builder = SparkSession.builder.appName("CompanyUnifyTransform")
if PG_JDBC_JAR:
    builder = builder.config("spark.jars", PG_JDBC_JAR).config("spark.driver.extraClassPath", PG_JDBC_JAR)

# tune shuffle partitions for medium-size jobs; override via spark-submit if needed
builder = builder.config("spark.sql.shuffle.partitions", int(os.getenv("SPARK_SHUFFLE_PARTS", "8")))

spark = builder.getOrCreate()

# ---------------------------
# Helper functions
# ---------------------------
_suffix_pattern = re.compile(
    r"\b(pty|pvt|ltd|plc|inc|co|company|limited|llc|corporation|corp|pty\.? ltd\.?)\b", flags=re.I
)

_non_alnum = re.compile(r"[^a-z0-9& ]")

def normalize_name_py(name: str) -> str:
    """Normalize a company name to a compact canonical form for comparisons.
    - lowercases
    - removes common corporate suffixes
    - removes punctuation except '&'
    - collapses whitespace
    - returns None for falsy input
    """
    if not name:
        return None
    s = str(name).lower().strip()
    # remove parentheses content
    s = re.sub(r"\([^)]*\)", " ", s)
    # remove corporate suffixes
    s = _suffix_pattern.sub(" ", s)
    # remove punctuation except ampersand
    s = _non_alnum.sub(" ", s)
    # collapse spaces
    s = re.sub(r"\s+", " ", s).strip()
    return s or None

# register as Spark UDF
normalize_udf = F.udf(normalize_name_py, T.StringType())

def fuzzy_score_py(a: str, b: str) -> float:
    """Simple similarity using SequenceMatcher (0..1)."""
    if not a or not b:
        return 0.0
    return float(SequenceMatcher(None, a, b).ratio())

fuzzy_udf = F.udf(fuzzy_score_py, T.DoubleType())

# Utility to build block key (first N chars of normalized name)
def block_key_expr(norm_col, n=BLOCK_KEY_CHARS):
    # safe substring (norm_name may be null)
    return F.when(F.col(norm_col).isNull(), F.lit(None)).otherwise(F.substring(F.col(norm_col), 1, n))

# ---------------------------
# JDBC read helper
# ---------------------------
def read_pg_table(table: str):
    opts = {
        "url": PG_JDBC_URL,
        "dbtable": table,
        "user": PG_USER,
        "password": PG_PASS,
        "driver": "org.postgresql.Driver",
    }
    logger.info("Reading table %s from Postgres", table)
    return spark.read.format("jdbc").options(**opts).load()

# ---------------------------
# Read staging tables
# ---------------------------
df_abr = read_pg_table("stg.abr_bulk").limit(100)
df_cc = read_pg_table("stg.commoncrawl_raw").limit(100)


# Add normalized names and blocking keys
df_abr_clean = (
    df_abr
    .withColumn("norm_name", normalize_udf(F.col("entity_name")))
    .withColumn("block_key", block_key_expr("norm_name"))
    .withColumn("source", F.lit("ABR"))
)

df_cc_clean = (
    df_cc
    .withColumn("norm_name", normalize_udf(F.col("extracted_name")))
    .withColumn("block_key", block_key_expr("norm_name"))
    .withColumn("source", F.lit("COMMONCRAWL"))
)

# ---------------------------
# Exact matches (on normalized name)
# ---------------------------
# alias to avoid ambiguous columns after join
abr = df_abr_clean.alias("abr")
cc = df_cc_clean.alias("cc")

df_exact = (
    abr.join(cc, on=(abr.norm_name == cc.norm_name), how="inner")
    # select canonical fields from both sides; prefer ABR authoritative fields where relevant
    .select(
        abr.abn.alias("abn"),
        abr.entity_name.alias("entity_name"),
        abr.entity_type.alias("entity_type"),
        abr.address.alias("address"),
        abr.postcode.alias("postcode"),
        abr.state.alias("state"),
        cc.domain.alias("domain"),
        cc.extracted_industry.alias("extracted_industry"),
        F.lit("EXACT").alias("match_type")
    )
).dropDuplicates(["abn", "domain"])

logger.info("Exact matches computed")

# ---------------------------
# Fuzzy matching WITH blocking (avoid full cross join)
# ---------------------------
# Only compare rows that share the same block_key (first N chars of norm_name)
# and where both norm_name are non-null.
abr_blocked = abr.filter(F.col("norm_name").isNotNull() & F.col("block_key").isNotNull()).select(
    "abn", "entity_name", "entity_type", "address", "postcode", "state", "norm_name", "block_key"
).alias("abr_b")

cc_blocked = cc.filter(F.col("norm_name").isNotNull() & F.col("block_key").isNotNull()).select(
    "domain", "extracted_industry", "norm_name", "block_key"
).alias("cc_b")

# join on block_key to limit candidate pairs
candidates = abr_blocked.join(cc_blocked, on=(F.col("abr_b.block_key") == F.col("cc_b.block_key")), how="inner")

# compute fuzzy score
candidates = candidates.withColumn(
    "fuzzy_score",
    fuzzy_udf(F.col("abr_b.norm_name"), F.col("cc_b.norm_name"))
)

# keep only pairs above lower threshold
fuzzy_candidates = candidates.filter(F.col("fuzzy_score") >= FUZZY_THRESHOLD_LO)

logger.info("Fuzzy candidate pairs generated (after blocking and thresholding)")

# ---------------------------
# Collect borderline pairs for LLM confirmation (on driver)
# ---------------------------
# borderline: fuzzy_score >= LO and < HI
borderline_df = fuzzy_candidates.filter((F.col("fuzzy_score") >= FUZZY_THRESHOLD_LO) & (F.col("fuzzy_score") < FUZZY_THRESHOLD_HI))

borderline_count = borderline_df.count()
logger.info("Borderline candidate count (needs optional LLM confirmation): %d", borderline_count)

confirmed_pairs = None  # will become a Spark DataFrame of confirmed fuzzy matches

if OPENAI_API_KEY and borderline_count > 0:
    # Limit number of borderline candidates sent to LLM (prevent runaway costs)
    limit = min(borderline_count, LLM_CONFIRM_LIMIT)
    logger.info("Collecting up to %d borderline pairs to confirm via LLM (driver-side)", limit)
    # select minimal necessary fields to driver
    to_confirm = borderline_df.select(
        F.col("abr_b.abn").alias("abn"),
        F.col("abr_b.entity_name").alias("entity_name"),
        F.col("abr_b.norm_name").alias("abr_norm"),
        F.col("cc_b.domain").alias("domain"),
        F.col("cc_b.norm_name").alias("cc_norm"),
        F.col("cc_b.extracted_industry").alias("extracted_industry"),
        F.col("fuzzy_score").alias("fuzzy_score")
    ).limit(limit).toPandas()

    logger.info("Driver collected %d rows for LLM confirmation", len(to_confirm))

    # Perform sequential LLM confirmations on driver (you can batch / parallelize if needed)
    confirmed_list: List[Tuple[str, str]] = []  # list of (abn, domain) pairs that are confirmed
    def llm_confirm_pair(a: str, b: str) -> bool:
        """Synchronous LLM confirmation for a single pair (driver-side)."""
        try:
            prompt = (
                "Do these two company names refer to the same legal entity? "
                "Answer 'yes' or 'no' only.\n\n"
                f"Company A: '{a}'\n"
                f"Company B: '{b}'\n"
            )
            # NOTE: model choice can be changed; keep temperature 0 for deterministic replies.
            resp = openai.ChatCompletion.create(
                model="gpt-4o-mini",  # change if unavailable in your account
                messages=[{"role": "user", "content": prompt}],
                temperature=0
            )
            ans = resp.choices[0].message.content.strip().lower()
            return ans.startswith("yes")
        except Exception as e:
            logger.warning("LLM call failed for pair (%s | %s): %s", a, b, str(e))
            return False

    # iterate and confirm (rate-limited)
    for idx, row in to_confirm.iterrows():
        a = row.get("abr_norm") or row.get("entity_name") or ""
        b = row.get("cc_norm") or ""
        try:
            confirmed = llm_confirm_pair(a, b)
            if confirmed:
                confirmed_list.append((row["abn"], row["domain"], row["extracted_industry"], float(row["fuzzy_score"])))
        except Exception as e:
            logger.warning("Error confirming pair index %d: %s", idx, e)
        # gentle sleep to avoid hitting rate limits (tunable)
        time.sleep(LLM_RATE_LIMIT_SLEEP)

    logger.info("LLM confirmed %d pairs out of %d checked", len(confirmed_list), len(to_confirm))

    # build DataFrame of confirmed fuzzy matches
    if confirmed_list:
        confirmed_schema = T.StructType([
            T.StructField("abn", T.StringType(), True),
            T.StructField("domain", T.StringType(), True),
            T.StructField("extracted_industry", T.StringType(), True),
            T.StructField("fuzzy_score", T.DoubleType(), True),
        ])
        confirmed_pairs = spark.createDataFrame(confirmed_list, schema=confirmed_schema)
        # mark match_type
        confirmed_pairs = confirmed_pairs.withColumn("match_type", F.lit("FUZZY_LLM"))
else:
    logger.info("LLM confirmation disabled or no borderline rows; using only high-confidence fuzzy matches")

# ---------------------------
# High-confidence fuzzy matches (above HI threshold) - auto accept
# ---------------------------
high_conf_fuzzy = fuzzy_candidates.filter(F.col("fuzzy_score") >= FUZZY_THRESHOLD_HI).select(
    F.col("abr_b.abn").alias("abn"),
    F.col("abr_b.entity_name").alias("entity_name"),
    F.col("abr_b.entity_type").alias("entity_type"),
    F.col("abr_b.address").alias("address"),
    F.col("abr_b.postcode").alias("postcode"),
    F.col("abr_b.state").alias("state"),
    F.col("cc_b.domain").alias("domain"),
    F.col("cc_b.extracted_industry").alias("extracted_industry"),
    F.col("fuzzy_score").alias("fuzzy_score")
).withColumn("match_type", F.lit("FUZZY_HIGH"))

logger.info("High-confidence fuzzy matches computed")

# ---------------------------
# Combine confirmed fuzzy matches (from LLM) + high-confidence fuzzy
# ---------------------------
fuzzy_final = None

if confirmed_pairs is not None:
    # confirmed_pairs contains abn + domain + industry + fuzzy_score
    # join back to abr to get remaining columns
    cp = confirmed_pairs.alias("cp")
    abr_for_join = abr.select("abn", "entity_name", "entity_type", "address", "postcode", "state").alias("abr_j")
    joined_confirmed = cp.join(abr_for_join, on=(cp.abn == abr_for_join.abn), how="inner").select(
        abr_for_join.abn,
        abr_for_join.entity_name,
        abr_for_join.entity_type,
        abr_for_join.address,
        abr_for_join.postcode,
        abr_for_join.state,
        cp.domain,
        cp.extracted_industry,
        cp.fuzzy_score,
        F.lit("FUZZY_LLM").alias("match_type")
    )
    fuzzy_final = joined_confirmed.unionByName(high_conf_fuzzy.select(*joined_confirmed.columns))
else:
    fuzzy_final = high_conf_fuzzy

# deduplicate fuzzy_final
fuzzy_final = fuzzy_final.dropDuplicates(["abn", "domain"])

logger.info("Fuzzy matching (final) prepared")

# ---------------------------
# Union exact + fuzzy results and write to DWH
# ---------------------------
# Ensure both dataframes have same schema for unionByName
select_cols = ["abn", "entity_name", "entity_type", "address", "postcode", "state", "domain", "extracted_industry", "match_type"]

df_exact_sel = df_exact.select(*select_cols)
df_fuzzy_sel = fuzzy_final.select(*select_cols)

df_unified = df_exact_sel.unionByName(df_fuzzy_sel).withColumn("processed_at", F.current_timestamp())

# optional: deduplicate final unified view by abn+domain keeping highest match_type precedence
# define precedence: EXACT > FUZZY_LLM > FUZZY_HIGH
precedence = F.when(F.col("match_type") == "EXACT", F.lit(3)) \
              .when(F.col("match_type") == "FUZZY_LLM", F.lit(2)) \
              .when(F.col("match_type") == "FUZZY_HIGH", F.lit(1)) \
              .otherwise(F.lit(0))

window = Window.partitionBy("abn", "domain").orderBy(F.desc(precedence), F.desc("processed_at"))
df_unified_ranked = df_unified.withColumn("rank", F.row_number().over(window)).filter(F.col("rank") == 1).drop("rank")

# ---------------------------
# Write to Postgres DWH
# ---------------------------
dwh_table = os.getenv("DWH_TABLE", "dwh.unified_companies")
logger.info("Writing unified results to %s (mode=%s)", dwh_table, WRITE_MODE)

jdbc_opts = {
    "url": PG_JDBC_URL,
    "dbtable": dwh_table,
    "user": PG_USER,
    "password": PG_PASS,
    "driver": "org.postgresql.Driver"
}

(
    df_unified_ranked.write
    .format("jdbc")
    .options(**jdbc_opts)
    .mode(WRITE_MODE)   # "append" or "overwrite"
    .save()
)

count_written = df_unified_ranked.count()
logger.info("✅ Transformation complete: %d unified rows written.", count_written)

spark.stop()
