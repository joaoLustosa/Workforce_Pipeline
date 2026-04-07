# Workforce Analytics Pipeline (CAGED)

## Overview

This project implements a production-oriented data pipeline to process Brazilian labor market data from CAGED (Cadastro Geral de Empregados e Desempregados).

The goal is to transform raw, messy government data into structured, validated, and analytics-ready datasets using industry-standard data engineering practices.

---

## Architecture

The pipeline follows a layered data architecture:

```
raw → staging → curated
```

### Raw Layer

* Source: Original CAGED `.txt` files
* Characteristics: Unmodified, schema not enforced

### Staging Layer

* Purpose: Clean and standardize data
* Key features:

  * Column normalization
  * Type parsing (dates, integers, floats, booleans)
  * Schema validation using Pandera
  * Partitioned Parquet storage (`competencia_mov_partition`)
  * Deterministic partition overwrite logic

### Curated Layer

* Purpose: Business-level aggregations
* Current dataset:

  * `caged_employment_by_uf`
* Partitioned by `competencia_mov_partition`

---

## Project Structure

```
src/
  ingestion/        # Load raw data
  transformation/   # Cleaning and parsing logic
  validation/       # Schema and data validation
  storage/          # Parquet writing logic
  curation/         # Business aggregations

scripts/
  run_pipeline.py   # Pipeline entrypoint

data/
  raw/              # Original files
  staging/          # Cleaned data (partitioned)
  curated/          # Aggregated data
```

---

## Key Features

### 1. Schema Validation

* Implemented using Pandera
* Enforces data types and constraints
* Detects invalid or inconsistent records early

### 2. Data Normalization

* Handles encoding inconsistencies
* Standardizes column naming
* Converts Brazilian numeric formats

### 3. Partitioned Storage

* Uses Parquet with partitioning by month
* Enables efficient querying and scalability

### 4. Deterministic Writes

* Partition-level overwrite strategy
* Prevents data duplication
* Ensures idempotent pipeline runs

---

## How to Run

```bash
python -m scripts.run_pipeline
```

Pipeline steps:

1. Load raw data
2. Transform and validate
3. Save to staging (partitioned Parquet)
4. Run curated aggregations
5. Save curated outputs

---

## Current Output

### Staging Example

```
data/staging/caged/
  competencia_mov_partition=2024-01/
    *.parquet
```

### Curated Example

```
data/curated/caged_employment_by_uf/
  competencia_mov_partition=2024-01/
    *.parquet
```

---

## Tech Stack

* Python 3.12
* Pandas
* PyArrow
* Pandera

---

## Next Steps

* Expand business metrics in curated layer
* Add curated data validation schema
* Improve logging and observability
* Introduce configurable write modes (overwrite / append)
* Prepare for cloud storage (S3-compatible)

---

## Motivation

This project is designed to demonstrate:

* Data pipeline design
* Data quality enforcement
* Partitioned data lake patterns
* Reproducibility and idempotency
