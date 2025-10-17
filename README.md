# dbt-olids

Foundational data layers for OLIDS (One London Integrated Data Set).

## What This Project Does

Builds two data layers:

**Base Layer**
Filtered views of OLIDS source tables applying:
- NCL practice filtering
- Sensitive patient exclusion
- Concept mapping for clinical codes

**Stable Layer**
Incrementally updated tables providing stability whilst the One London team develops the alpha OLIDS data. Uses merge strategy to process only new/changed records based on `lds_start_date_time`, tracking historical changes (SCD Type 2). Includes:
- Incremental updates (processes only changes since last run)
- `person_id` workaround (hashed from `sk_patient_id` and cascaded throughout, addressing poor population in upstream OLIDS until ISL fixes at source)
- Clustering (physically organises data by key columns for faster queries)

**Full refresh required when ISL truncates/reloads or reprocesses upstream data.**

Analytical models built on the stable layer: [dbt-ncl-analytics](https://github.com/ncl-icb-analytics/dbt-ncl-analytics)

## Quick Start

```bash
# Clone and setup
git clone https://github.com/ncl-icb-analytics/dbt-olids
cd dbt-olids
python -m venv venv
pip install -r requirements.txt

# Configure connection
cp profiles.yml.template profiles.yml
cp env.example .env
# Edit .env with your Snowflake credentials

# Activate environment (run this every session)
.\start_dbt.ps1

# Build
dbt deps
dbt build  # Builds and tests all models
```

## Common Commands

**Always start with:** `.\start_dbt.ps1` (loads credentials from `.env`)

```bash
# Regular development runs (use XS-sized warehouse)
dbt build              # Build and test everything
dbt build -s tag:base  # Base layer only

# Full refresh of stable layer (use L-sized warehouse)
dbt build --full-refresh
```

**Warehouse sizing:**
- Regular runs: XS-sized warehouse in `.env`
- Full refresh: L-sized warehouse in `.env`

## Configuration

Copy templates and configure your Snowflake connection:

```bash
cp profiles.yml.template profiles.yml
cp env.example .env
# Edit .env: account, user, role, database, warehouse
dbt debug  # Test connection
```

**Prerequisites:**
- Snowflake access with the ISL-USERGROUP-SECONDEES-NCL role
- Access to DATA_LAB_OLIDS_NCL and Data_Store_OLIDS_Alpha databases

Never commit `.env` or `profiles.yml`.

## Project Structure

```
models/olids/
├── base/           # Filtered views
├── stable/         # Incremental tables
└── intermediate/   # NCL practices lookup
```

## Where Objects Are Built

All models are built in the database specified by `SNOWFLAKE_TARGET_DATABASE` in your `.env` file (typically `DATA_LAB_OLIDS_NCL`):

- **Base layer**: `DATA_LAB_OLIDS_NCL.olids_base.*` (views)
- **Stable layer**: `DATA_LAB_OLIDS_NCL.olids.*` (tables)
- **Intermediate**: `DATA_LAB_OLIDS_NCL.DBT_STABLE.*` (tables)

The stable layer reads from `Data_Store_OLIDS_Alpha` source tables.

## Contributing

See [Contributing Guide](CONTRIBUTING.md) for workflow details.

## License

Dual licensed under Open Government v3 & MIT. All code outputs subject to Crown Copyright.


