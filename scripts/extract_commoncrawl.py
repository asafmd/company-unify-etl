#!/usr/bin/env python3
"""
CommonCrawl ETL Script
----------------------
Fetches URLs from the Common Crawl Index, scrapes basic info, and loads data into Postgres staging schema.
"""

import os
import time
import json
import re
import logging
from urllib.parse import urlparse
from typing import List, Dict
from datetime import datetime

import requests
from requests.adapters import HTTPAdapter, Retry
from bs4 import BeautifulSoup
import psycopg2
from psycopg2.extras import execute_values
from dotenv import load_dotenv


# -------------------------------------------------------
# 🧩 Setup & Configuration
# -------------------------------------------------------
load_dotenv()
PG_DSN = os.getenv("PG_DSN")

if not PG_DSN:
    raise EnvironmentError("❌ Environment variable PG_DSN not found in .env file")

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
)

CC_INDEX_URL = "https://index.commoncrawl.org/CC-MAIN-2025-43-index"
COMMONCRAWL_QUERY = "*.com.au"
LIMIT = 500
DELAY_SECONDS = 0.5
TIMEOUT = 10
BATCH_SIZE = 50


# -------------------------------------------------------
# ⚙️ Helper Functions
# -------------------------------------------------------
def create_session() -> requests.Session:
    """Create a requests session with retry logic and custom headers."""
    s = requests.Session()
    retries = Retry(
        total=3,
        backoff_factor=1,
        status_forcelist=[429, 500, 502, 503, 504],
        allowed_methods=["GET", "POST"]
    )
    s.mount("https://", HTTPAdapter(max_retries=retries))
    s.headers.update({
        "User-Agent": "CompanyUnifyBot/1.0 (+https://yourdomain.example) - contact: you@example.com"
    })
    return s


def ensure_table_pg(dsn: str):
    """Ensure staging table exists in Postgres."""
    conn = psycopg2.connect(dsn)
    cur = conn.cursor()
    cur.execute("""
        CREATE SCHEMA IF NOT EXISTS stg;

        CREATE TABLE IF NOT EXISTS stg.commoncrawl_raw (
            source_url TEXT,
            domain TEXT,
            extracted_name TEXT,
            extracted_industry TEXT,
            http_status INT,
            raw_html TEXT,
            extra JSONB,
            inserted_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        );
    """)
    conn.commit()
    cur.close()
    conn.close()
    logging.info("✅ Ensured staging table exists: stg.commoncrawl_raw")


def fetch_cc_index_hits(session: requests.Session, pattern: str, limit: int = 200) -> List[Dict]:
    """Fetch URLs from Common Crawl Index API."""
    url = f"{CC_INDEX_URL}?url={pattern}&output=json"
    resp = session.get(url, timeout=TIMEOUT)
    resp.raise_for_status()

    hits = []
    for i, line in enumerate(resp.iter_lines(decode_unicode=True)):
        if not line:
            continue
        try:
            j = json.loads(line)
            hits.append(j)
        except json.JSONDecodeError:
            continue
        if len(hits) >= limit:
            break
    logging.info(f"✅ Retrieved {len(hits)} URLs from Common Crawl Index")
    return hits


def get_domain(u: str) -> str:
    """Extract domain name from URL."""
    try:
        return urlparse(u).netloc.lower()
    except Exception:
        return None


def extract_from_html(html: str) -> Dict:
    """Extract basic info from HTML (organization name, keywords, etc.)."""
    out = {"extracted_name": None, "extracted_industry": None}
    if not html:
        return out

    soup = BeautifulSoup(html, "lxml")

    # Try schema.org Organization
    org = soup.find(attrs={"itemtype": re.compile("schema.org/Organization", re.I)})
    if org:
        name_tag = org.find(attrs={"itemprop": "name"})
        if name_tag and name_tag.get_text(strip=True):
            out["extracted_name"] = name_tag.get_text(strip=True)

    # Fallback to og:title or <title>
    if not out["extracted_name"]:
        meta_title = soup.find("meta", property="og:site_name") or soup.find("meta", property="og:title")
        if meta_title and meta_title.get("content"):
            out["extracted_name"] = meta_title.get("content").strip()
    if not out["extracted_name"] and soup.title and soup.title.string:
        out["extracted_name"] = soup.title.string.split("|")[0].strip()

    # Industry detection
    meta_keywords = soup.find("meta", attrs={"name": "keywords"})
    if meta_keywords and meta_keywords.get("content"):
        out["extracted_industry"] = meta_keywords["content"].split(",")[0].strip()

    meta_industry = soup.find("meta", attrs={"name": "industry"})
    if not out["extracted_industry"] and meta_industry and meta_industry.get("content"):
        out["extracted_industry"] = meta_industry.get("content").strip()

    return out


def insert_batch_pg(dsn: str, rows: List[Dict]):
    """Insert records into Postgres in batches."""
    if not rows:
        return

    conn = psycopg2.connect(dsn)
    cur = conn.cursor()
    sql = """
        INSERT INTO stg.commoncrawl_raw
        (source_url, domain, extracted_name, extracted_industry, http_status, raw_html, extra)
        VALUES %s
    """

    values = [
        (
            r.get("url"),
            r.get("domain"),
            r.get("extracted_name"),
            r.get("extracted_industry"),
            r.get("http_status"),
            r.get("raw_html"),
            json.dumps(r.get("extra") or {})
        )
        for r in rows
    ]

    try:
        execute_values(cur, sql, values, page_size=100)
        conn.commit()
        logging.info(f"📥 Inserted {len(rows)} rows into stg.commoncrawl_raw")
    except Exception as e:
        logging.error(f"❌ Insert failed: {e}")
    finally:
        cur.close()
        conn.close()


def polite_fetch(session: requests.Session, url: str):
    """Fetch a URL safely with retries and fallback."""
    try:
        return session.get(url, timeout=TIMEOUT)
    except Exception as e:
        return type("R", (), {"status_code": None, "text": None, "reason": str(e)})


# -------------------------------------------------------
# 🚀 Main ETL Logic
# -------------------------------------------------------
def main():
    start_time = datetime.now()
    logging.info("🚀 CommonCrawl ETL Started")

    ensure_table_pg(PG_DSN)
    session = create_session()

    try:
        hits = fetch_cc_index_hits(session, COMMONCRAWL_QUERY, limit=LIMIT)
    except Exception as e:
        logging.error(f"❌ Failed to fetch index hits: {e}")
        return

    batch = []
    for i, hit in enumerate(hits):
        url = hit.get("url") or hit.get("original")
        if not url:
            continue

        domain = get_domain(url)
        resp = polite_fetch(session, url)
        http_status = getattr(resp, "status_code", None)
        html = getattr(resp, "text", None) if http_status == 200 else None

        parsed = extract_from_html(html) if html else {}
        row = {
            "url": url,
            "domain": domain,
            "extracted_name": parsed.get("extracted_name"),
            "extracted_industry": parsed.get("extracted_industry"),
            "http_status": http_status,
            "raw_html": html if html and len(html) < 200_000 else None,
            "extra": {"cc_index": hit}
        }
        batch.append(row)

        if len(batch) >= BATCH_SIZE:
            insert_batch_pg(PG_DSN, batch)
            batch = []
            logging.info(f"Inserted batch up to index {i}")
        time.sleep(DELAY_SECONDS)

    if batch:
        insert_batch_pg(PG_DSN, batch)

    logging.info(f"🏁 ETL Completed in {(datetime.now() - start_time).seconds} seconds.")


# -------------------------------------------------------
# 🪶 Airflow Callable
# -------------------------------------------------------
def run_commoncrawl_etl(**kwargs):
    """Airflow-friendly entrypoint."""
    main()


if __name__ == "__main__":
    main()
