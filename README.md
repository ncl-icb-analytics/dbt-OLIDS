# HealtheIntent -> Snowflake Data Migration

## What This Is

**dbt** (data build tool) is a modern data transformation tool that lets you write SQL models and automatically handles dependencies, testing, and documentation. Think of it as a tool to help bring software engineering best practices to SQL analysts.

This project uses dbt to migrate and transform healthcare data that used to be transformed in HealtheIntent (Vertica) to Snowflake, creating analytics-ready datasets for healthcare analysis.

**Data models included:**
- Disease registers (diabetes, hypertension, heart failure, etc.)
- Quality measures and clinical indicators  
- Patient demographics and status tracking
- Medication and prescription analytics
- Data quality monitoring and validation

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
Raw Snowflake → Staging (views) → Intermediate (tables) → Marts (tables)
                              ↓
                      Data Quality & Tests
```

## Quick Start

**Prerequisites:** Python 3.8+ and access to Snowflake

```bash
# 1. Get the code
git clone https://github.com/ncl-icb-analytics/snowflake-hei-migration
cd snowflake-hei-migration

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

**That's it!** You now have disease registers and quality measures ready for analysis.

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
3. **Verify connection:**

   ```bash
   dbt debug
   ```

**Important:** Never commit `.env` to version control! The file is already in `.gitignore`.

### **Code Quality**

This project automatically fixes common issues when you save files (trailing spaces, formatting, etc.). Most of the time this happens silently and you won't notice.

## Project Structure

```
models/
├── staging/                 # 1:1 source mappings (views)
├── intermediate/            # Business logic & consolidation (tables)
│   ├── diagnoses/           # Clinical observations (observation-level)
│   │   └── qof/             # QOF-specific diagnosis models
│   ├── medications/         # Medication orders & prescriptions
│   ├── observations/        # Clinical measurements & lab results
│   ├── person_attributes/   # Demographics & characteristics
│   └── programme/           # specific programme intermediate models
└── marts/                   # Analytics-ready models (tables)
    ├── clinical_safety/     # Safety monitoring & alerts
    ├── data_quality/        # Data quality reports
    ├── disease_registers/   # Person-level clinical registers
    │   └── qof/             # QOF disease registers
    ├── geography/           # Households & geographic analytics
    ├── measures/            # Healthcare quality indicators
    ├── organisation/        # Practice & organisational data
    ├── person_demographics/ # Demographics with households
    ├── person_status/       # Patient activity & status
    └── programme/           # specific programmes (valproate, ltc_lcs, etc.)

macros/                      # Reusable SQL macros
├── get_observations.sql     # Extract clinical observations
├── get_medication_orders.sql # Extract medication data
└── testing/                 # custom macros for generic tests

legacy/                      # Original SQL scripts for reference
scripts/                     # Python utilities and automation
```

## Essential dbt Commands

**For daily use:**
```bash
dbt run         # Build all models (creates tables/views in Snowflake)
dbt test        # Run data quality tests
dbt docs serve  # Open documentation in browser
```

**For development:**
```bash
dbt run --select model_name              # Build just one model
dbt run --select staging                 # Build all staging models
dbt run --select +model_name             # Build model + everything it depends on
```

**Getting help:**
```bash
dbt --help                    # See all commands
dbt run --help                # Help for specific command
dbt debug                     # Test your connection to Snowflake
```

## Development Patterns

### **Macro Usage**

```sql
-- Direct SELECT clause usage (most common)
SELECT * FROM ({{ get_observations("'DM_COD'") }}) obs
SELECT * FROM ({{ get_medication_orders(bnf_code='02050501') }}) meds

-- With WHERE clause
SELECT observation_id, person_id, clinical_effective_date
FROM ({{ get_observations("'HTN_COD', 'HTNRES_COD'", "PCD") }}) obs -- from PCD Refset only
WHERE obs.clinical_effective_date IS NOT NULL

-- BNF code filtering for medications
{{ get_medication_orders(bnf_code='02050501') }}  -- ACE inhibitors (BNF Chapter 2.5.5.1)
{{ get_medication_orders(bnf_code='0304') }}      -- Asthma medications (BNF Chapter 3.4)
```

### **YAML Structure**

```yaml
models:
  - name: int_diabetes_diagnoses_all
    description: "Clinical diabetes observations (observation-level)"
    columns:
      - name: observation_id
        tests:
          - not_null
          - unique
      - name: person_id
        tests:
          - not_null
          - relationships:
              to: ref('dim_person')
              field: person_id
    tests:
      - cluster_ids_exist:
          cluster_ids: "DM_COD,DMTYPE1_COD,DMTYPE2_COD,DMRES_COD"
      - dbt_utils.at_least_one:
          name: "has_at_least_one_observation"
          column_name: observation_id
```

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

## Deploying to QA and Production

By default, `dbt run` builds in the dev environment. To deploy to other environments:

**QA Environment:**
```bash
dbt build --target qa    # Builds models AND runs tests in QA
```

**Production Environment:**
```bash
dbt build --target prod  # Builds models AND runs tests in Production
```

**Important:** 
- Always test in dev first: `dbt build`
- Deploy to QA for integration testing: `dbt build --target qa`
- Only deploy to production after QA approval: `dbt build --target prod`
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

## License

This repository is dual licensed under the Open Government v3 & MIT. All code outputs are subject to Crown Copyright.
