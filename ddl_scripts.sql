-- Staging table for raw ABR XML company data
CREATE TABLE IF NOT EXISTS abr_company_raw (
    id SERIAL PRIMARY KEY,
    abn VARCHAR(20) UNIQUE,
    name TEXT,
    type TEXT,
    status TEXT,
    data JSONB,
    source_file TEXT,
    extracted_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Staging table for raw Common Crawl company data
CREATE TABLE IF NOT EXISTS commoncrawl_company_raw (
    id SERIAL PRIMARY KEY,
    domain TEXT,
    company_name TEXT,
    data JSONB,
    source_file TEXT,
    extracted_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Normalized company entities table (after fuzzy/AI matching/unification)
CREATE TABLE IF NOT EXISTS company_normalized (
    company_id SERIAL PRIMARY KEY,
    canonical_name TEXT NOT NULL,
    abn VARCHAR(20),
    domain TEXT,
    country TEXT,
    source_ids JSONB,
    metadata JSONB,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Table for alternate/spellings/aliases/domains
CREATE TABLE IF NOT EXISTS company_aliases (
    alias_id SERIAL PRIMARY KEY,
    company_id INT REFERENCES company_normalized(company_id) ON DELETE CASCADE,
    alias_name TEXT,
    alias_domain TEXT,
    source TEXT,
    UNIQUE (company_id, alias_name, alias_domain)
);

-- ETL pipeline run log
CREATE TABLE IF NOT EXISTS etl_log (
    run_id SERIAL PRIMARY KEY,
    job_name TEXT,
    status TEXT,
    started_at TIMESTAMP,
    finished_at TIMESTAMP,
    log TEXT
);
