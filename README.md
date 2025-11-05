Data Engineering Project

## Table of Contents
1. [Project Overview](#project-overview)  
2. [Database Schema (PostgreSQL DDL)](#database-schema-postgresql-ddl)  
3. [Pipeline Architecture](#pipeline-architecture)  
4. [Technology Justification](#technology-justification)  
5. [AI Model Used & Rationale](#ai-model-used--rationale)  
6. [Setup & Running Instructions](#setup--running-instructions)  
7. [Python Scripts](#python-scripts)  
8. [ETL Pipeline & Transformations](#etl-pipeline--transformations)  
9. [Entity Matching & Design Choices](#entity-matching--design-choices)  
10. [IDE Used](#ide-used)

---

## Project Overview
This project demonstrates an end-to-end **data engineering pipeline** that unifies company data from multiple sources:

- **ABR** (Australian Business Register XML files)  
- **Common Crawl** (web scraped company data)

The goal is to ingest, clean, and unify data into a **PostgreSQL Data Warehouse (DWH)**, enabling downstream analytics and reporting.

---

## Database Schema (PostgreSQL DDL)

### Staging Tables
```sql
-- ABR staging
DROP TABLE IF EXISTS stg.abr_bulk;
CREATE TABLE stg.abr_bulk (
    abn TEXT PRIMARY KEY,
    entity_name TEXT,
    entity_type TEXT,
    entity_status TEXT,
    address TEXT,
    postcode TEXT,
    state TEXT,
    start_date DATE,
    end_date DATE,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Common Crawl staging
DROP TABLE IF EXISTS stg.commoncrawl_raw;
CREATE TABLE stg.commoncrawl_raw (
    source_url TEXT,
    url_hash TEXT PRIMARY KEY,
    domain TEXT,
    extracted_name TEXT,
    extracted_industry TEXT,
    http_status INT,
    raw_html TEXT,
    extra JSONB,
    inserted_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Data Warehouse table
DROP TABLE IF EXISTS dwh.unified_companies;
CREATE TABLE dwh.unified_companies (
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
Pipeline Architecture
rust
Copy code
ABR XML ----\
             \
              --> Staging (PostgreSQL) --> PySpark Transformation --> DWH (PostgreSQL)
Common Crawl -/
Description:

Data Extraction: extract_abr.py and extract_commoncrawl.py ingest raw data into staging tables.

Data Transformation: pyspark_transformation.py cleans, normalizes, and performs entity matching.

Unification: The result is written to the DWH for analytics.

Orchestration: Airflow DAG manages dependencies and scheduling.

Notes: ABR is considered the primary source for company identity; Common Crawl enriches missing information (domain, industry).

Technology Justification
Python: ETL scripting and orchestration.

PostgreSQL: Reliable, ACID-compliant staging & DWH.

PySpark: Scalable transformations and fuzzy/AI-based entity matching.

BeautifulSoup + Requests: Web scraping for Common Crawl.

Airflow: DAG orchestration, dependency management, scheduling.

Docker: Containerized reproducible environment.

AI Model Used & Rationale
LLM Used: OpenAI GPT-4o-mini

Purpose: Confirm entity matching for borderline fuzzy matches (0.8 < score < 0.95).

Rationale: Fast, cost-effective, accurate confirmation of human-readable company names.

Setup & Running Instructions
Clone the repository

bash
Copy code
git clone <your-repo-url>
cd "Firmable - Data Engineering Project"
Create a Python virtual environment

bash
Copy code
python -m venv .venv
.venv\Scripts\activate  # Windows
pip install -r requirements.txt
Set environment variables in .env

pgsql
Copy code
PG_DSN=postgresql://user:password@localhost:5432/dbname
OPENAI_API_KEY=<your_openai_key>
Run extraction scripts

bash
Copy code
python scripts/extract_abr.py --source-uri "scripts/data/ABR.xml" --local-path "scripts/data/ABR.xml" --table stg.abr_bulk --batch-size 100 --save-raw
python scripts/extract_commoncrawl.py
Run PySpark transformation

bash
Copy code
python scripts/pyspark_transformation.py
Orchestrate via Airflow (optional)

Place etl_dag.py in your Airflow dags/ folder

Start Airflow using Docker or local installation

Trigger DAG company_unification_etl

Python Scripts
extract_abr.py – Stream and insert ABR XML data into PostgreSQL staging.

extract_commoncrawl.py – Scrape and stage Common Crawl data.

pyspark_transformation.py – Normalize, fuzzy match, AI-confirm, and unify entities into DWH.

ETL Pipeline & Transformations
Normalizes company names: lowercasing, removing suffixes (Ltd, Pty, Inc).

Exact + fuzzy matching using difflib.SequenceMatcher.

AI-based confirmation for borderline matches.

Writes unified table to dwh.unified_companies.

Entity Matching & Design Choices
Primary source: ABR data ensures canonical identity (abn as primary key).

Secondary source: Common Crawl enriches with domain and industry info.

Matching logic:

Exact match on normalized names.

Fuzzy match > 0.8.

AI confirmation for 0.8 < fuzzy score < 0.95.

IDE Used
VS Code for development, debugging, and Git integration.

Author: Asaf Mohammed
Date: 2025-11-05
