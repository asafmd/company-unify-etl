#!/usr/bin/env python3
"""
Production-ready ABR ETL (fixed)

- Backward-compatible timezone handling
- Robust HTTP retry configuration
- Split multi-statement DDL into single execute() calls
- Safe element cleanup when parent is None
- Normalizes dates before inserting into DATE column
- Better quarantine records (include timestamp)
- Proper resource cleanup on DB operations
"""

import os
import json
import logging
import argparse
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Dict, Iterator, List, Optional

import requests
from requests.adapters import HTTPAdapter
from urllib3.util import Retry
from dotenv import load_dotenv
import psycopg2
from psycopg2.extras import execute_values
from lxml import etree

# ---------------------------
# Configuration & logging
# ---------------------------
load_dotenv()
PG_DSN = os.getenv("PG_DSN")  # required

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    force=True,
)

DEFAULT_SAVE_DIR = Path(os.getenv("ABR_RAW_DIR", "data/raw/abr"))
QUARANTINE_DIR = Path(os.getenv("ABR_QUARANTINE_DIR", "data/quarantine/abr"))

# ---------------------------
# Helpers: HTTP session with retries
# ---------------------------
def create_http_session(total_retries: int = 3, backoff: float = 1.0) -> requests.Session:
    s = requests.Session()
    # Use urllib3 Retry; allowed_methods expects a frozenset
    retries = Retry(
        total=total_retries,
        backoff_factor=backoff,
        status_forcelist=[429, 500, 502, 503, 504],
        allowed_methods=frozenset(["GET", "POST"]),
    )
    s.mount("https://", HTTPAdapter(max_retries=retries))
    s.mount("http://", HTTPAdapter(max_retries=retries))
    s.headers.update({
        "User-Agent": "ABR-ETL/1.0 (+your-email@example.com) - demo"
    })
    return s


def download_file(url: str, dest_path: Path, session: Optional[requests.Session] = None, timeout: int = 60) -> Path:
    """Download remote file with streaming to disk. Returns Path to saved file."""
    dest_path.parent.mkdir(parents=True, exist_ok=True)
    session = session or create_http_session()
    logging.info(f"Downloading {url} -> {dest_path}")
    with session.get(url, stream=True, timeout=timeout) as r:
        r.raise_for_status()
        with open(dest_path, "wb") as f:
            for chunk in r.iter_content(chunk_size=8192):
                if chunk:
                    f.write(chunk)
    logging.info(f"Saved file: {dest_path} ({dest_path.stat().st_size} bytes)")
    return dest_path

# ---------------------------
# XML parsing (namespace-robust)
# ---------------------------

def iter_abrs_from_file(xml_path: Path, limit: int = None) -> Iterator[etree._Element]:
    """Stream ABR entity elements from file with optional limit."""
    xml_path = Path(xml_path)
    if not xml_path.exists():
        raise FileNotFoundError(f"{xml_path} not found")

    context = etree.iterparse(str(xml_path), events=("end",), recover=True, huge_tree=True)
    count = 0

    for _, elem in context:
        try:
            ln = etree.QName(elem.tag).localname if isinstance(elem.tag, str) else None
        except Exception:
            ln = None

        if ln and ln.lower() in ("abr", "entity", "entityrecord", "record"):
            yield elem
            count += 1

            # ✅ stop after N records for testing
            if limit and count >= limit:
                print(f"⚠️ Limit reached ({limit} records). Stopping early for testing.")
                break



def get_text_by_localname(elem: etree._Element, local: str) -> Optional[str]:
    """Return text from subelement by local-name, namespace-safe."""
    try:
        res = elem.xpath('.//*[local-name()=$name]', name=local)
        if res and getattr(res[0], 'text', None):
            return res[0].text.strip()
        return None
    except Exception as e:
        logging.debug(f"XPath lookup failed for {local}: {e}")
        return None


def parse_abr_element(elem: etree._Element) -> Optional[Dict]:
    try:
        abn = get_text_by_localname(elem, "ABN")
        entity_name = (
            get_text_by_localname(elem, "NonIndividualNameText")
            or get_text_by_localname(elem, "OrganisationName")
            or get_text_by_localname(elem, "EntityName")
        )
        entity_type = get_text_by_localname(elem, "EntityTypeText") or get_text_by_localname(elem, "EntityType")
        entity_status = get_text_by_localname(elem, "ABNStatus") or get_text_by_localname(elem, "EntityStatus")
        address_line = get_text_by_localname(elem, "AddressLine") or get_text_by_localname(elem, "StreetAddress")
        postcode = get_text_by_localname(elem, "Postcode")
        state = get_text_by_localname(elem, "State")
        start_date = get_text_by_localname(elem, "ABNStatusFromDate") or get_text_by_localname(elem, "StartDate")

        return {
            "abn": abn,
            "entity_name": entity_name,
            "entity_type": entity_type,
            "entity_status": entity_status,
            "address": address_line,
            "postcode": postcode,
            "state": state,
            "start_date": start_date,
        }
    except Exception as e:
        logging.exception("Error parsing element: %s", e)
        return None

# ---------------------------
# Postgres helpers
# ---------------------------

def ensure_staging_table(dsn: str, table_name: str):
    create_sql = f"""
    CREATE SCHEMA IF NOT EXISTS stg;
    CREATE TABLE IF NOT EXISTS {table_name} (
        abn TEXT,
        entity_name TEXT,
        entity_type TEXT,
        entity_status TEXT,
        address TEXT,
        postcode TEXT,
        state TEXT,
        start_date DATE,
        inserted_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    )
    """

    # unique index DDL separate (run after table exists)
    index_sql = f"""
    CREATE UNIQUE INDEX IF NOT EXISTS idx_{table_name.replace('.', '_')}_abn
    ON {table_name} (abn);
    """

    conn = psycopg2.connect(dsn)
    try:
        with conn.cursor() as cur:
            # Split statements and execute individually to avoid multi-statement issues
            stmts = [s.strip() for s in create_sql.split(";") if s.strip()]
            for stmt in stmts:
                cur.execute(stmt)
            conn.commit()
            cur.execute(index_sql)
            conn.commit()
    finally:
        conn.close()
    logging.info("Ensured staging table: %s (with ABN deduplication)", table_name)


def _normalize_date(value: Optional[str]):
    if not value:
        return None
    s = value.strip()
    # strip timezone Z
    if s.endswith("Z"):
        s = s[:-1]
    try:
        # Try isoformat first (fast, stdlib)
        dt = datetime.fromisoformat(s)
        return dt.date()
    except Exception:
        try:
            # Optional: dateutil if available for messy formats
            from dateutil.parser import parse as _parse
            dt = _parse(s)
            return dt.date()
        except Exception:
            logging.debug("Failed to parse date '%s'", value)
            return None


def batch_insert_records(dsn: str, table_name: str, records: List[Dict], max_retries: int = 3) -> int:
    if not records:
        return 0
    insert_sql = f"""
    INSERT INTO {table_name}
    (abn, entity_name, entity_type, entity_status, address, postcode, state, start_date)
    VALUES %s
    ON CONFLICT (abn) DO NOTHING;
    """

    values = [
        (
            r.get("abn"),
            r.get("entity_name"),
            r.get("entity_type"),
            r.get("entity_status"),
            r.get("address"),
            r.get("postcode"),
            r.get("state"),
            _normalize_date(r.get("start_date")),
        )
        for r in records
    ]
    attempt = 0
    while attempt < max_retries:
        conn = None
        try:
            conn = psycopg2.connect(dsn)
            with conn.cursor() as cur:
                execute_values(cur, insert_sql, values, page_size=500)
                conn.commit()
            logging.info("Inserted up to %d new (non-duplicate) records into %s", len(records), table_name)
            return len(records)
        except Exception as e:
            attempt += 1
            logging.error("Insert attempt %d failed: %s", attempt, e)
            if conn:
                try:
                    conn.close()
                except Exception:
                    pass
            if attempt < max_retries:
                logging.info("Retrying insert in 5s...")
                time.sleep(5)
            else:
                raise

# ---------------------------
# Quarantine bad records
# ---------------------------

def write_quarantine(records: List[Dict], xml_file: Path, suffix: str = "parsing_failed"):
    QUARANTINE_DIR.mkdir(parents=True, exist_ok=True)
    timestamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    out_file = QUARANTINE_DIR / f"{xml_file.stem}_{suffix}_{timestamp}.jsonl"
    with open(out_file, "w", encoding="utf-8") as fh:
        for r in records:
            # add helpful metadata
            entry = {"quarantine_time": timestamp, **(r or {})}
            fh.write(json.dumps(entry, default=str, ensure_ascii=False) + "\n")
    logging.info("Wrote %d quarantined records to %s", len(records), out_file)

# ---------------------------
# Main ETL flow
# ---------------------------

def abr_etl(source_uri: str, local_path: Optional[str] = None,
            table_name: str = "stg.abr_bulk", batch_size: int = 1000, save_raw: bool = True):

    if not PG_DSN:
        raise EnvironmentError("PG_DSN must be set in environment (.env)")

    # Resolve local path
    if local_path:
        xml_path = Path(local_path)
    else:
        fname = (
            Path(source_uri).name
            if source_uri and not source_uri.startswith("http")
            else f"abr_{datetime.now(timezone.utc).strftime('%Y%m%dT%H%M%SZ')}.xml"
        )
        xml_path = DEFAULT_SAVE_DIR / fname

    if source_uri.startswith("http"):
        if save_raw or not xml_path.exists():
            session = create_http_session()
            download_file(source_uri, xml_path, session=session)
    else:
        xml_path = Path(source_uri)
        if not xml_path.exists():
            raise FileNotFoundError(f"Input file not found: {xml_path}")

    logging.info("Starting parse of %s", xml_path)
    ensure_staging_table(PG_DSN, table_name)

    inserted = 0
    batch: List[Dict] = []
    quarantined: List[Dict] = []
    total_parsed = 0

    try:
        for elem in iter_abrs_from_file(xml_path,limit = 500):
            total_parsed += 1
            rec = parse_abr_element(elem)

            # safe cleanup of parsed element
            parent = elem.getparent()
            elem.clear()
            if parent is not None:
                # attempt to remove previous siblings to free memory
                try:
                    while parent.getprevious() is not None:
                        del parent[0]
                except Exception:
                    # best-effort cleanup; ignore on failure
                    pass

            if not rec:
                quarantined.append({"error": "parse_failed"})
                continue

            if not rec.get("abn") and not rec.get("entity_name"):
                quarantined.append({"error": "missing_key_fields", "record": rec})
                continue

            batch.append(rec)
            if len(batch) >= batch_size:
                inserted += batch_insert_records(PG_DSN, table_name, batch)
                batch = []

        if batch:
            inserted += batch_insert_records(PG_DSN, table_name, batch)

        if quarantined:
            write_quarantine(quarantined, xml_file=xml_path, suffix="final_quarantine")

    except Exception as e:
        logging.exception("Fatal error: %s", e)
        if batch:
            write_quarantine(batch, xml_file=xml_path, suffix="fatal_batch")
        if quarantined:
            write_quarantine(quarantined, xml_file=xml_path, suffix="fatal_quarantine")
        raise

    logging.info("ETL complete. Parsed=%d, Inserted=%d", total_parsed, inserted)

# ---------------------------
# CLI entrypoint
# ---------------------------
if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="ABR ETL (download -> parse -> load)")
    parser.add_argument("--source-uri", required=True, help="Local file path or remote URL to ABR XML")
    parser.add_argument("--local-path", default=None)
    parser.add_argument("--table", default="stg.abr_bulk")
    parser.add_argument("--batch-size", type=int, default=1000)
    parser.add_argument("--save-raw", action="store_true")
    args = parser.parse_args()

    t0 = datetime.now(timezone.utc)
    logging.info("Starting ABR ETL at %s", t0.isoformat())
    try:
        abr_etl(
            source_uri=args.source_uri,
            local_path=args.local_path,
            table_name=args.table,
            batch_size=args.batch_size,
            save_raw=args.save_raw,
        )
    except Exception as exc:
        logging.exception("ABR ETL failed: %s", exc)
        raise
    finally:
        logging.info("Finished ABR ETL in %.2f seconds", (datetime.now(timezone.utc) - t0).total_seconds())
