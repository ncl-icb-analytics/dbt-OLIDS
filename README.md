# NCL Analytics DBT Project - OLIDS

## ⚠️ Project Migration Notice

**This project now provides only the base and stable data layers for OLIDS.**

All marts, staging, intermediate models, and analytical logic have been migrated to [dbt-ncl-analytics](https://github.com/ncl-icb-analytics/dbt-ncl-analytics).

**What remains in this project:**
- Base layer: Filtered views of OLIDS source tables (NCL practices only, sensitive patients excluded)
- Stable layer: Incrementally updated tables providing a stable interface for downstream analytics

**For analytical models, disease registers, and quality measures**, please see [dbt-ncl-analytics](https://github.com/ncl-icb-analytics/dbt-ncl-analytics).

## What This Is

**dbt** (data build tool) is a modern data transformation tool that lets you write SQL models and automatically handles dependencies, testing, and documentation.

This project provides the foundational data layer for the One London Integrated Data Set (OLIDS), transforming raw healthcare data into a clean, consistent base for analytics.

## One London Integrated Data Set (OLIDS)

This project is built to use data from the [One London Integrated Data Set (OLIDS)](https://github.com/NHSISL/Datasets) - a canonical data model that transforms data from GP Systems (EMIS and SystmOne) into a standardised format, closely resembling the FHIR specification.

**Key OLIDS tables you'll work with:**
- `patient` - Patient demographics and registration
- `person` - Individual person records and identifiers
- `observation` - Clinical measurements, test results, diagnoses
- `medication_order` - Prescriptions and medication data
- `organisation` - GP practices and healthcare providers

## Architecture

```
OLIDS Source Data → Base Layer (views) → Stable Layer (incremental tables)
                                               ↓
                              dbt-ncl-analytics (marts, measures, registers)
```

**This project:**
- Base layer: Applies NCL practice filtering and excludes sensitive patients
- Stable layer: Incrementally updated tables with concept mapping

**Downstream project ([dbt-ncl-analytics](https://github.com/ncl-icb-analytics/dbt-ncl-analytics)):**
- Uses stable layer as source
- Contains all analytical models, disease registers, quality measures

## Quick Start

**Prerequisites:** Python 3.8+, access to Snowflake, requirements outlined in the [Contributing Guide](CONTRIBUTING.md) 

```bash
# 1. Get the code
git clone https://github.com/ncl-icb-analytics/dbt-olids
cd dbt-olids

# 2. Setup Python environment
python -m venv venv && venv\Scripts\activate
pip install -r requirements.txt

# 3. Configure Snowflake connection
cp env.example .env
# Edit .env file with your Snowflake credentials (see next section)

# 4. Install dbt dependencies and run
dbt deps
dbt run         # Builds all models in your dev environment (safe)
dbt test        # Runs data quality tests
```

**That's it!** You now have the base and stable OLIDS data layers built.

**For disease registers and quality measures**, see [dbt-ncl-analytics](https://github.com/ncl-icb-analytics/dbt-ncl-analytics).

## Environment Setup

### **Snowflake Configuration**

1. **Copy and configure environment file:**

   ```bash
   cp env.example .env
   ```
2. **Edit `.env` with your Snowflake details:**

   - `SNOWFLAKE_ACCOUNT`: Your Snowflake account identifier
   - `SNOWFLAKE_USER`: Your Snowflake username
   - `SNOWFLAKE_ROLE`: Your assigned role
   - `SNOWFLAKE_WAREHOUSE`: Your warehouse name
   - `SNOWFLAKE_PASSWORD`: Optional (you can use SSO or key-pair authentication)

3. **Create the a profile.yml:**
Follow the instructions in the [profiles.yml.template](profiles.yml.template)

4. **Run start_dbt.ps1 to initialise your .env:**
```bash
   .\start_dbt.ps1
```

5. **Verify connection:**

   ```bash
   dbt debug
   ```

**Important:** Never commit `.env` to version control! The file is already in `.gitignore`.

### **Code Quality**

This project automatically fixes common issues when you save files (trailing spaces, formatting, etc.). Most of the time this happens silently and you won't notice.

## Project Structure

```
models/
├── olids/
│   ├── base/                # Filtered base views (NCL practices only, no sensitive patients)
│   │                        # - Applies practice and patient filtering
│   │                        # - Adds concept mapping for clinical codes
│   │                        # - Generates fabricated person_id
│   ├── stable/              # Incremental tables (merge strategy)
│   │                        # - SCD Type 2 tracking with lds_start_date_time
│   │                        # - Clustered for query performance
│   └── intermediate/
│       └── organisation/
│           └── int_ncl_practices.sql  # NCL practice lookup (STPCode = 'QMJ')
└── sources.yml              # Source definitions (olids_masked, olids_common, olids_terminology)

macros/                      # Reusable SQL macros
├── add_model_comment.sql    # Adds metadata comments to models
├── generate_table_comment.sql # Generates comment text
└── get_custom_schema.sql    # Schema naming logic

scripts/                     # Python utilities
├── fix_source_schemas.py    # Reorganise sources by schema
├── reorganize_sources_by_schema.py
├── update_base_model_sources.py
└── query_snowflake_schema.py # Query Snowflake information schema
```

## Essential dbt Commands

**For daily use:**
```bash
dbt run         # Build all models (base views + stable tables)
dbt test        # Run data quality tests
dbt docs serve  # Open documentation in browser
```

**For development:**
```bash
dbt run -s model_name              # Build just one model
dbt run -s tag:base                # Build all base layer models
dbt run -s tag:stable              # Build all stable layer models
dbt run -s +model_name             # Build model + everything it depends on
dbt run -s model_name+             # Build model + everything that depends on it
```

**Getting help:**
```bash
dbt --help                    # See all commands
dbt run --help                # Help for specific command
dbt debug                     # Test your connection to Snowflake
```

## Key Features

### **Base Layer**
- **NCL Practice Filtering**: Only includes patients registered to North Central London ICB practices (STPCode = 'QMJ')
- **Sensitive Patient Exclusion**: Filters out spine-sensitive, confidential, and dummy patients
- **Concept Mapping**: Joins to OLIDS_TERMINOLOGY to provide mapped clinical codes
- **Fabricated person_id**: Generates deterministic person IDs using MD5 hashing

### **Stable Layer**
- **Incremental Updates**: Uses merge strategy for efficient updates
- **SCD Type 2**: Tracks changes over time using lds_start_date_time
- **Clustered Tables**: Optimised for query performance (typically by source_concept_id and clinical_effective_date)
- **Secure Views**: Configured with secure=true for patient data protection

## Making Changes

When you run `dbt run`, it creates tables/views in the dev Snowflake environment. To share your SQL code changes with the team, you need to save them to git:

1. **Get latest code:**
   ```bash
   git pull origin main
   ```

2. **Make your changes and test:**
   ```bash
   dbt run --select +your_model    # Test your changes in dev environment
   dbt test --select +your_model   # Run quality checks
   ```

3. **Save and share your code:**
   ```bash
   git add .
   git commit -m "describe what you changed"
   git push origin main
   ```

**Remember:** Running dbt creates tables in Snowflake, but only committing to git saves your SQL code for others to see.

## Deploying to Production

By default, `dbt run` builds in the dev environment. To deploy to prod:

**Production Environment:**
```bash
dbt build --target prod  # Builds models AND runs tests in Production
```

**Important:** 
- Always test in dev first: `dbt build`
- Only deploy to production after approval: `dbt build --target prod`
- Using `dbt build` ensures upstream tests pass before creating downstream tables, in DAG order.

## Learning dbt

New to dbt? Here are some helpful resources:

**Getting Started:**
- [dbt Fundamentals Course](https://courses.getdbt.com/courses/fundamentals) - Free interactive course (3-4 hours)
- [What is dbt?](https://docs.getdbt.com/docs/introduction) - Official introduction
- [dbt Best Practices](https://docs.getdbt.com/guides/best-practices) - How to structure projects

**Quick References:**
- [dbt Command Reference](https://docs.getdbt.com/reference/dbt-commands) - All commands explained
- [SQL Style Guide](https://docs.getdbt.com/guides/best-practices/how-we-style/2-how-we-style-our-sql) - Writing clean SQL
- [Testing in dbt](https://docs.getdbt.com/docs/build/tests) - Data quality testing

## Contributing

Please see our [Contributing Guide](CONTRIBUTING.md) for details on:
- Setting up SSH keys and commit signing
- Branch protection rules and workflow
- Commit message conventions
- Creating pull requests

## License

This repository is dual licensed under the Open Government v3 & MIT. All code outputs are subject to Crown Copyright.


