# Snowflake Healthcare Data Migration - dbt Project

## Overview

A comprehensive dbt project for migrating healthcare data models from HealtheIntent (Vertica) to Snowflake. Implements modern dimensional modeling with robust data quality testing for NHS primary care analytics.

## Architecture

```
Raw Snowflake → Staging (views) → Intermediate (tables) → Marts (tables)
                              ↓
                      Data Quality & Tests
```

## Quick Start

```bash
# Setup
git clone <repository-url>
cd snowflake-hei-migration-dbt
python -m venv venv && venv\Scripts\activate
pip install -r requirements.txt

# Configure (copy templates and edit .env)
cp profiles.yml.template profiles.yml
cp env.example .env

# Run
dbt deps
dbt run         # Safe - always dev environment
dbt test
```

## Environment Management

**Simple Three-Environment Setup:**

- **`dbt run`** or `dbt build` → `DBT_DEV` schema (safe default)
- **`dbt build --target qa`** → `DBT_QA` schema (quality assurance)
- **`dbt build --target prod`** → Production database (explicit confirmation)

## Project Structure

```
models/
├── staging/          # 1:1 source mappings (views)
├── intermediate/     # Business logic & consolidation (tables)
│   ├── diagnoses/    # Clinical observations (observation-level)
│   ├── medications/  # Medication orders
│   ├── observations/ # Clinical measurements
│   └── person_attributes/  # Demographics & characteristics
└── marts/           # Analytics-ready models (tables)
    ├── clinical_safety/     # Safety monitoring & alerts
    ├── data_quality/        # Data quality reports
    ├── disease_registers/   # Person-level clinical registers
    ├── geography/           # Households & geographic analytics
    ├── measures/            # Healthcare quality indicators
    ├── organisation/        # Practice & organisational data
    ├── person_demographics/ # Demographics with households
    ├── person_status/       # Patient activity & status
    └── programme/           # NHS programmes (health checks, etc.)
```

## Development Commands

### **Core Commands**

- **`dbt deps`** - Install package dependencies (run first!)
- **`dbt parse`** - Parse project files and check for syntax errors
- **`dbt compile`** - Compile models to SQL (useful for debugging)
- **`dbt run`** - Runs models only (faster for development iteration)
- **`dbt test`** - Runs tests only
- **`dbt build`** - Runs models + tests in dependency order (recommended for qa and prod)

### **Common Workflows**

```bash
# First time setup
dbt deps
dbt parse  # Check for syntax errors

# Debugging a model
dbt compile --select dim_person_demographics
# Check target/compiled/ folder for generated SQL

# Daily development (fast iteration)
dbt run
dbt run --select +dim_person_demographics  # Model + upstream dependencies

# Quality assurance (recommended for qa/prod)
dbt build
dbt build --target qa  # Quality assurance testing with tests

# Specific selections
dbt run --select staging    # All staging models
dbt test --select marts     # All mart tests
dbt build --select +fct_person_diabetes_register  # Model + dependencies + tests

# Documentation
dbt docs generate && dbt docs serve
```

## Development Patterns

### **Macro Usage**

```sql
-- Direct FROM clause usage (most common)
FROM ({{ get_observations("'DM_COD', 'DMTYPE1_COD', 'DMRES_COD'") }}) obs
FROM ({{ get_medication_orders(bnf_code='02050501') }}) meds

-- In subqueries for complex logic
WITH prioritized_observations AS (
    SELECT observation_id, person_id, clinical_effective_date
    FROM ({{ get_observations("'HTN_COD', 'HTNRES_COD'", "PCD") }}) obs -- from PCD Refset only
    WHERE obs.clinical_effective_date IS NOT NULL
)

-- Multiple parameters for observations
{{ get_observations("'DM_COD', 'DMTYPE1_COD', 'DMTYPE2_COD', 'DMRES_COD'") }}

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
      - dbt_utils.expression_is_true:
          expression: "count(*) >= 1"
```

## Contributing

### **Creating a Feature Branch**

1. **Ensure you're on the latest main branch:**

   ```bash
   git checkout main
   git pull origin main
   ```
2. **Create a descriptive feature branch:**

   ```bash
   # Use descriptive branch names following the pattern:
   git checkout -b feature/add-heart-failure-register
   git checkout -b fix/diabetes-type-classification
   git checkout -b refactor/consolidate-person-dimensions
   ```
3. **Make your changes following project conventions:**

   - Follow the established naming patterns and folder structure
   - Add appropriate YAML documentation for all new models
   - Include relevant tests (not excessive, but in key places)

### **Testing Your Changes**

```bash
# Parse and compile to check for syntax errors
dbt parse
dbt compile --select +your_new_model

# Run your changes in dev environment (always safe)
dbt run --select +your_new_model
dbt test --select +your_new_model

# For broader changes, test the entire project
dbt build  		# Runs models and tests in DAG order for DEV environment
dbt build --target qa   # Full build in qa environment
```

### **Creating a Pull Request**

1. **Commit your changes with clear messages:**

   ```bash
   git add .
   git commit -m "feat: add heart failure register with LVSD classification

   - Implements QOF HF register business logic
   - Includes general HF and LVSD-specific flags
   - Adds comprehensive test coverage
   - Updates documentation with clinical context"
   ```
2. **Push your branch and create a PR:**

   ```bash
   git push origin feature/add-heart-failure-register
   ```
3. **Open a Pull Request on GitHub:**

   - Use a descriptive title summarising the change
   - Include a clear description of what was changed and why
   - Reference any related issues using `#issue-number`
   - Request reviews from relevant team members

### **PR Review Process**

- **TO DO: Automated Checks**: CI will run `dbt parse`, `dbt compile`, and basic tests
- **Code Review**: At least one team member should review for:
  - Code quality and maintainability
  - Adherence to naming conventions
  - Appropriate use of facts vs dimensions
  - Documentation completeness
- **Testing**: Reviewers should verify the changes work in a dev environment
- **TO DO: Snowflake automatically runs `dbt deps` and `dbt build --target prod` on commits to main and on a schedule**

### **Merging**

- PRs are merged into `main` after approval
- Delete the feature branch after merging

## License

This repository is dual licensed under the Open Government v3 & MIT. All code outputs are subject to Crown Copyright.
