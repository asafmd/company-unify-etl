#!/usr/bin/env python3
"""
Production-ready ABR ETL

Features:
- Accepts either a local file path or a remote URL to download the ABR XML.
- Saves raw XML to a configurable local folder (data/raw/abr/) for reproducibility.
- Streams XML parsing using lxml.iterparse and uses local-name() XPaths to be namespace-safe.
- Batch inserts into Postgres with table-creation DDL, retry logic, and quarantine of bad records.
- Clear logging and metrics.
- Airflow-friendly entry function: run_abr_etl(...)
"""

import os
import json
import logging
import argparse
from datetime import datetime
from pathlib import Path
from typing import Dict, Iterator, List, Optional

import requests
from requests.adapters import HTTPAdapter, Retry
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
    format="%(asctime)s [%(levelname)s] %(message)s"
)

DEFAULT_SAVE_DIR = Path(os.getenv("ABR_RAW_DIR", "data/raw/abr"))
QUARANTINE_DIR = Path(os.getenv("ABR_QUARANTINE_DIR", "data/quarantine/abr"))

# ---------------------------
# Helpers: HTTP session with retries
# ---------------------------
def create_http_session(total_retries: int = 3, backoff: float = 1.0) -> requests.Session:
    s = requests.Session()
    retries = Retry(
        total=total_retries,
        backoff_factor=backoff,
        status_forcelist=[429, 500, 502, 503, 504],
        allowed_methods=["GET", "POST"]
    )
    s.mount("https://", HTTPAdapter(max_retries=retries))
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
def iter_abrs_from_file(xml_path: Path) -> Iterator[etree._Element]:
    """
    Stream ABR entity elements from file. This function yields element objects that should be cleared by the caller.
    We don't assume a fixed tag name; we use heuristics: yield elements where local-name is 'ABR' or 'Entity' or contains 'Record'
    """
    xml_path = Path(xml_path)
    if not xml_path.exists():
        raise FileNotFoundError(f"{xml_path} not found")

    # Use lxml.iterparse for streaming
    context = etree.iterparse(str(xml_path), events=("end",), recover=True)
    for event, elem in context:
        try:
            ln = etree.QName(elem.tag).localname if isinstance(elem.tag, str) else None
        except Exception:
            ln = None

        if ln and ln.lower() in ("abr", "entity", "entityrecord", "record"):
            yield elem
            # caller must elem.clear() and delete references to free memory

# Utility to extract text by local-name (safe across namespaces)
def get_text_by_localname(elem: etree._Element, local: str) -> Optional[str]:
    res = elem.find('.//*[local-name()="%s"]' % local)
    if res is not None and res.text:
        return res.text.strip()
    return None

# Convert XML element into a dict record (fields we care about)
def parse_abr_element(elem: etree._Element) -> Optional[Dict]:
    try:
        # Example fields: ABN, NonIndividualNameText (entity name), EntityTypeText, ABNStatus, Address details
        abn = get_text_by_localname(elem, "ABN")
        # entity name often under NonIndividualNameText or OrganisationName
        entity_name = get_text_by_localname(elem, "NonIndividualNameText") or \
                      get_text_by_localname(elem, "OrganisationName") or \
                      get_text_by_localname(elem, "EntityName")

        entity_type = get_text_by_localname(elem, "EntityTypeText") or get_text_by_localname(elem, "EntityType")
        # ABN status or attributes
        entity_status = get_text_by_localname(elem, "ABNStatus" ) or get_text_by_localname(elem, "EntityStatus")
        # address components (best-effort)
        address_line = get_text_by_localname(elem, "AddressLine") or get_text_by_localname(elem, "StreetAddress")
        postcode = get_text_by_localname(elem, "Postcode")
        state = get_text_by_localname(elem, "State")
        start_date = get_text_by_localname(elem, "ABNStatusFromDate") or get_text_by_localname(elem, "StartDate")

        record = {
            "abn": abn,
            "entity_name": entity_name,
            "entity_type": entity_type,
            "entity_status": entity_status,
            "address": address_line,
            "postcode": postcode,
            "state": state,
            "start_date": start_date
        }

        return record
    except Exception as e:
        logging.exception("Error parsing element: %s", e)
        return None

# ---------------------------
# Postgres helpers (ensure table & batch insert with retry)
# ---------------------------
def ensure_staging_table(dsn: str, table_name: str):
    """Create staging schema and table if not exists."""
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
    );
    """
    conn = psycopg2.connect(dsn)
    try:
        with conn.cursor() as cur:
            cur.execute(create_sql)
            conn.commit()
    finally:
        conn.close()
    logging.info("Ensured staging table: %s", table_name)

def batch_insert_records(dsn: str, table_name: str, records: List[Dict], max_retries: int = 3):
    """Insert a batch of dict records into Postgres with simple retry logic."""
    if not records:
        return 0
    insert_sql = f"""
    INSERT INTO {table_name}
    (abn, entity_name, entity_type, entity_status, address, postcode, state, start_date)
    VALUES %s
    """
    values = []
    for r in records:
        values.append((
            r.get("abn"), r.get("entity_name"), r.get("entity_type"), r.get("entity_status"),
            r.get("address"), r.get("postcode"), r.get("state"), r.get("start_date")
        ))
    attempt = 0
    while attempt < max_retries:
        try:
            conn = psycopg2.connect(dsn)
            with conn.cursor() as cur:
                execute_values(cur, insert_sql, values, page_size=500)
                conn.commit()
            conn.close()
            logging.info("Inserted %d records into %s", len(records), table_name)
            return len(records)
        except Exception as e:
            attempt += 1
            logging.error("Insert attempt %d failed: %s", attempt, e)
            if attempt < max_retries:
                logging.info("Retrying insert in 5s...")
                import time; time.sleep(5)
            else:
                logging.exception("Insert failed after %d attempts", attempt)
                # On final failure, raise to allow caller to quarantine batch
                raise

# ---------------------------
# Quarantine bad records
# ---------------------------
def write_quarantine(records: List[Dict], xml_file: Path, suffix: str = "parsing_failed"):
    QUARANTINE_DIR.mkdir(parents=True, exist_ok=True)
    timestamp = datetime.utcnow().strftime("%Y%m%dT%H%M%S")
    out_file = QUARANTINE_DIR / f"{xml_file.stem}_{suffix}_{timestamp}.jsonl"
    with open(out_file, "w", encoding="utf-8") as fh:
        for r in records:
            fh.write(json.dumps(r, default=str, ensure_ascii=False) + "\n")
    logging.info("Wrote %d quarantined records to %s", len(records), out_file)

# ---------------------------
# Main ETL flow
# ---------------------------
def abr_etl(
    source_uri: str,
    local_path: Optional[str] = None,
    table_name: str = "stg.abr_bulk",
    batch_size: int = 1000,
    save_raw: bool = True
):
    """
    ETL controller:
      - If source_uri is a URL and local_path provided, download and save xml.
      - Parse file streaming and batch insert into Postgres table.
    """
    if not PG_DSN:
        raise EnvironmentError("PG_DSN must be set in environment (.env)")

    # Resolve local path
    if local_path:
        xml_path = Path(local_path)
    else:
        # derive filename from URL or fallback to timestamped filename
        fname = Path(source_uri).name if source_uri and not source_uri.startswith("http") == False else f"abr_{datetime.utcnow().strftime('%Y%m%dT%H%M%SZ')}.xml"
        xml_path = DEFAULT_SAVE_DIR / fname

    # If source_uri is a URL (starts with http), download if file not present or save_raw True
    if source_uri.startswith("http"):
        if save_raw or not xml_path.exists():
            session = create_http_session()
            download_file(source_uri, xml_path, session=session)
    else:
        # source_uri is local file path
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
        for elem in iter_abrs_from_file(xml_path):
            total_parsed += 1
            rec = parse_abr_element(elem)
            # clear to free memory ASAP
            elem.clear()
            while elem.getprevious() is not None:
                del elem.getparent()[0]  # free siblings

            if not rec:
                # couldn't parse element: record minimal info to quarantine
                quarantined.append({"error": "parse_failed", "element_preview": etree.tostring(elem, method="text", encoding="unicode")})
                if len(quarantined) >= batch_size:
                    write_quarantine(quarantined, xml_file=xml_path, suffix="parse_failed")
                    quarantined = []
                continue

            # optional: basic validation (must have ABN or entity_name)
            if not rec.get("abn") and not rec.get("entity_name"):
                # missing key identifiers -> quarantine
                quarantined.append({"error": "missing_key_fields", "record": rec})
                if len(quarantined) >= batch_size:
                    write_quarantine(quarantined, xml_file=xml_path, suffix="missing_fields")
                    quarantined = []
                continue

            batch.append(rec)
            if len(batch) >= batch_size:
                try:
                    inserted += batch_insert_records(PG_DSN, table_name, batch)
                except Exception:
                    # if insert fails, quarantine the batch and continue (so pipeline won't lose everything)
                    write_quarantine(batch, xml_file=xml_path, suffix="insert_failed")
                batch = []

        # final batch flush
        if batch:
            try:
                inserted += batch_insert_records(PG_DSN, table_name, batch)
            except Exception:
                write_quarantine(batch, xml_file=xml_path, suffix="insert_failed")

        # write remaining quarantined
        if quarantined:
            write_quarantine(quarantined, xml_file=xml_path, suffix="final_quarantine")

    except Exception as e:
        logging.exception("Fatal error during parsing/inserting: %s", e)
        # optional: ensure remaining batch/quarantine saved
        if batch:
            write_quarantine(batch, xml_file=xml_path, suffix="fatal_batch")
        if quarantined:
            write_quarantine(quarantined, xml_file=xml_path, suffix="fatal_quarantine")
        raise

    logging.info("ETL complete. Parsed=%d, Inserted=%d, Quarantined=%d", total_parsed, inserted, 0)

# ---------------------------
# Airflow-friendly wrapper
# ---------------------------
def run_abr_etl(**kwargs):
    """
    Airflow wrapper. Expects kwargs: source_uri, local_path, table_name, batch_size, save_raw
    Example op_kwargs from DAG:
      {"source_uri": "https://.../abr.xml", "local_path": "/opt/airflow/data/raw/abr/{{ ds }}.xml", "table_name": "stg.abr_bulk"}
    """
    params = kwargs or {}
    abr_etl(
        source_uri=params.get("source_uri"),
        local_path=params.get("local_path"),
        table_name=params.get("table_name", "stg.abr_bulk"),
        batch_size=int(params.get("batch_size", 1000)),
        save_raw=bool(params.get("save_raw", True))
    )

# ---------------------------
# CLI entrypoint
# ---------------------------
if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="ABR ETL (download -> parse -> load)")
    parser.add_argument("--source-uri", required=True, help="Local file path or remote URL to ABR XML")
    parser.add_argument("--local-path", default=None, help="Local path to save/use XML file (overrides auto path)")
    parser.add_argument("--table", default="stg.abr_bulk", help="Target Postgres table (schema.table)")
    parser.add_argument("--batch-size", type=int, default=1000, help="Batch size for DB inserts")
    parser.add_argument("--save-raw", action="store_true", help="Force saving raw XML even if file exists")
    args = parser.parse_args()

    t0 = datetime.now()
    logging.info("Starting ABR ETL at %s", t0.isoformat())
    try:
        abr_etl(
            source_uri=args.source_uri,
            local_path=args.local_path,
            table_name=args.table,
            batch_size=args.batch_size,
            save_raw=args.save_raw
        )
    except Exception as exc:
        logging.exception("ABR ETL failed: %s", exc)
        raise
    finally:
        logging.info("Finished ABR ETL in %s seconds", (datetime.now() - t0).total_seconds())
