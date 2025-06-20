# Snowflake Healthcare Data Migration - dbt Project

## Overview

A comprehensive dbt project for migrating healthcare data models from HealtheIntent (Vertica) to Snowflake. This project implements a modern dimensional modeling approach with robust data quality testing, automated workflows, and a clean separation of concerns across staging, intermediate, and mart layers.

## 🏗️ Architecture

### Data Flow

```
Raw Snowflake Tables → Staging → Intermediate → Marts
                                      ↓
                              Data Quality & Tests
```

### Key Design Principles

- **Dimensional Modeling**: Star schema with conformed dimensions
- **Separation of Concerns**: Clean layering between staging, intermediate, and marts
- **Data Quality First**: Comprehensive testing at every layer
- **Maintainability**: Standardised patterns and reusable macros
- **Privacy**: Hashed identifiers for sensitive data (UPRN, postcodes)

## 📁 Project Structure

### Models

```
models/
├── staging/          # Source system interface layer
│   ├── stg_*.sql     # Cleaned, typed source tables
│   └── schema.yml    # Source definitions and tests
├── intermediate/     # Business logic and transformations
│   ├── diagnoses/    # Diagnosis-related transformations
│   ├── medications/  # Medication orders and prescribing
│   ├── observations/ # Clinical observations (BP, BMI, etc.)
│   ├── person_attributes/ # Person characteristics
│   └── programme/    # Programme-specific logic
└── marts/           # Business-ready dimensional models
    ├── person_demographics/  # Person dimensions
    ├── person_status/       # Patient status dimensions
    ├── organisation/        # Practice and organisational hierarchy
    ├── geography/          # Households and geographic dimensions
    ├── disease_registers/  # Disease register fact tables
    ├── measures/          # Healthcare measures and quality indicators
    ├── clinical_safety/   # Clinical safety monitoring
    ├── data_quality/     # Data quality monitoring
    └── programme/        # Programme-specific fact tables
```

### Supporting Infrastructure

```
macros/              # Reusable SQL functions
├── get_observations.sql      # Clinical observations macro
├── get_medication_orders.sql # Medication orders macro
├── get_latest_events.sql     # Event deduplication
└── filter_by_date.sql       # Date filtering utilities

scripts/             # Automation and utilities
├── check_yaml_column_tests.py    # Test validation
├── flag_macro_models.py          # Macro usage analysis
├── build_dependency_graph.py     # Dependency visualization
├── metadata_to_dbt_yaml.py      # YAML generation
└── generate_staging_models.py   # Staging model creation

legacy/              # Original Vertica models (reference)
```

## 🎯 Key Features

### Dimensional Model Highlights

- **`dim_person_demographics`**: Comprehensive person dimension with age bands, ethnicity, language, and geographic context
- **`dim_households`**: Physical dwelling dimension using deterministic UPRN hashing
- **`fct_household_members`**: Bridge table linking people to households with practice registrations
- **Disease Registers**: Standardised fact tables for 25+ clinical conditions
- **Geographic Hierarchy**: Practice → PCN → Neighbourhood → Local Authority

### Data Quality Framework

- **645+ Data Type Corrections**: Automated Snowflake type mapping
- **Comprehensive Testing**: YAML-defined tests for every model
- **Referential Integrity**: Foreign key relationships validated
- **Business Logic Tests**: Clinical rules and constraints enforced

### Healthcare-Specific Features

- **Clinical Coding**: BNF, Read codes, and concept mapping
- **Programme Support**: NHS Health Checks, Immunisations, LTC management
- **Quality Indicators**: Diabetes care processes, BP control, screening rates
- **Clinical Safety**: Valproate pregnancy monitoring, drug interactions

## 🚀 Getting Started

### Prerequisites

- Python 3.8+
- Snowflake account with appropriate permissions
- Git

### Installation

1. **Clone and setup environment**:

   ```bash
   git clone <repository-url>
   cd snowflake-hei-migration-dbt
   python -m venv venv
   venv\Scripts\activate
   pip install -r requirements.txt
   ```
2. **Configure connection**:

   ```bash
   # Copy templates
   cp profiles.yml.template profiles.yml
   cp env.example .env
   ```
3. **Environment variables** (`.env` file):

   ```env
   SNOWFLAKE_ACCOUNT=your_account
   SNOWFLAKE_USER=your_username
   SNOWFLAKE_ROLE=your_role
   SNOWFLAKE_WAREHOUSE=your_warehouse
   # Note: Database and schema are configured in dbt_project.yml and profiles.yml
   ```
4. **Test connection**:

   ```bash
   dbt debug
   ```

### Environment Management

**Safe by Default:** All standard `dbt` commands run in development environment with personal schemas.

```bash
# Safe default - always goes to dev environment
dbt run
dbt test

# Explicit targeting for other environments  
dbt run --target staging    # Shared testing environment
dbt run --target prod      # Production (with safety confirmation)
```

**Environment Details:**
- **`dev`** (default): Branch-based schemas (`DBT_{BRANCH_NAME}`) - feature isolation
- **`staging`**: Shared `DBT_STAGING` schema for integration testing  
- **`prod`**: Production database with explicit confirmation prompts

**Key Benefits:**
- ✅ **No accidental production deployments** - requires explicit `--target prod`
- ✅ **No environment variable caching issues** - uses dbt's native `--target` flag
- ✅ **Feature-based isolation** - each Git branch gets its own schema

### Running the Project

```bash
# Install dependencies
dbt deps

# Set branch-based schema (run once per branch)
python scripts/set_branch_env.py

# Run all models (safe default - goes to dev)
dbt run --full-refresh

# Target other environments explicitly
dbt run --target staging --full-refresh
dbt run --target prod --full-refresh

# Run specific layer (dev environment)
dbt run --select staging
dbt run --select intermediate
dbt run --select marts

# Run tests (dev environment)
dbt test

# Generate and serve documentation
dbt docs generate
dbt docs serve
```

## 🧪 Testing Strategy

### Test Coverage

- **Source Tests**: Data freshness and volume checks
- **Staging Tests**: Data type validation and not-null constraints
- **Intermediate Tests**: Business logic and transformation validation
- **Mart Tests**: Referential integrity and business rules
- **Cross-Model Tests**: Relationship validation between dimensions and facts

### Key Test Patterns

```yaml
# Data quality tests
- dbt_utils.expression_is_true:
    expression: "active_patients_have_practice_registration"

# Referential integrity
- relationships:
    to: ref('dim_households')
    field: household_id

# Business logic validation
- accepted_values:
    values: ['Recently active', 'Previously active', 'Historically active only']
```

## 🔧 Development Workflow

### Adding New Models

1. Create staging model in `models/staging/`
2. Add intermediate transformations in appropriate subfolder
3. Build mart model following dimensional patterns
4. Add comprehensive YAML documentation and tests
5. Update this README if introducing new concepts

### Useful Commands

```bash
# Environment management (safe by default)
dbt run                                        # Always dev environment
dbt run --target staging                      # Explicit staging
dbt run --target prod                         # Explicit production

# Development workflow
dbt parse                                      # Check model syntax
dbt compile --select dim_person_demographics   # Compile specific model
dbt test --select dim_person_demographics      # Test specific model
dbt run --select +dim_person_demographics      # Run model and dependencies

# Quality assurance
python scripts/check_yaml_column_tests.py      # Validate YAML tests
python scripts/build_dependency_graph.py       # Visualise dependencies
dbt test --store-failures                      # Store test failures for analysis
```

## 📊 Data Sources

### Primary Sources

- **OLIDS**: Patient demographics, appointments, clinical data
- **CODESETS**: Clinical coding and reference data
- **RULESETS**: Clinical decision rules and algorithms
- **POPULATION_HEALTH**: Geographic and organisational hierarchies

### Key Staging Models

- `stg_olids_patient_*`: Patient core data
- `stg_olids_clinical_*`: Clinical events and observations
- `stg_codesets_*`: Reference data and code mappings
- `stg_population_health_*`: Geographic lookups

## 🏥 Healthcare Context

This project supports NHS primary care analytics including:

- **Population Health Management**: Patient demographics, registration patterns
- **Quality Improvement**: Clinical indicators, screening rates, care processes
- **Programme Delivery**: Immunisations, health checks, disease management
- **Clinical Safety**: Medication monitoring, safety alerts
- **Health Inequalities**: Geographic and demographic analysis

## 📈 Monitoring & Maintenance

### Key Metrics to Monitor

- Model build times and success rates
- Test pass rates across layers
- Data freshness and volume trends
- Query performance on mart tables

### Regular Maintenance Tasks

- Review test failures and data quality issues
- Update business logic as clinical guidelines change
- Refresh geographic and organisational hierarchies
- Archive historical data following retention policies

## 🤝 Contributing

1. Add comprehensive tests for new models
3. Document business logic clearly
4. Use established patterns and macros
5. Update README for architectural changes

## 📄 License

This repository is dual licensed under the Open Government v3 & MIT. All code outputs are subject to Crown Copyright.
