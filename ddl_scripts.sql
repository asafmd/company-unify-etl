-- Staging table for raw ABR XML company data
CREATE SCHEMA IF NOT EXISTS stg;
CREATE TABLE IF NOT EXISTS stg.abr_bulk (
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
CREATE UNIQUE INDEX IF NOT EXISTS idx_stg_abr_bulk_abn ON stg.abr_bulk (abn);

-- Staging table for raw Common Crawl company data
CREATE SCHEMA IF NOT EXISTS stg;
CREATE TABLE IF NOT EXISTS stg.commoncrawl_raw (
    source_url TEXT,
    url_hash TEXT UNIQUE,
    domain TEXT,
    extracted_name TEXT,
    extracted_industry TEXT,
    http_status INT,
    raw_html TEXT,
    extra JSONB,
    inserted_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);
CREATE UNIQUE INDEX IF NOT EXISTS idx_commoncrawl_raw_url_hash ON stg.commoncrawl_raw (url_hash);

-- Normalized company entities table (after fuzzy/AI matching/unification) -- DWH
CREATE SCHEMA IF NOT EXISTS dwh;
CREATE TABLE IF NOT EXISTS dwh.unified_companies (
    abn TEXT,
    entity_name TEXT,
    entity_type TEXT,
    address TEXT,
    postcode TEXT,
    state TEXT,
    domain TEXT,
    extracted_industry TEXT,
    processed_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);
