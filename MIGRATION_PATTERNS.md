# Snowflake HEI Migration - dbt Patterns & Guidelines

## Overview

This document serves as the canonical guide for migrating legacy dynamic tables to dbt models in the Snowflake HEI (Higher Education Institution) migration project. The patterns documented here should be followed consistently to ensure maintainable, testable, and performant healthcare data transformations.

## Core Architecture

### Layer Structure

```
models/
├── staging/          # 1:1 source mappings (views)
├── intermediate/     # Business logic & consolidation (tables)
└── marts/           # Final analytical models (tables)
```

### Database & Schema Strategy

- **Development Database**: `DATA_LAB_NCL_TRAINING_TEMP`
- **Staging Schema**: Source-specific (e.g., `OLIDS_MASKED`, `CODESETS`)
- **Transformed Schema**: `DBT`
- **Materialisation**: Views for staging, tables for intermediate/marts

## New Dimensional Structure (Following Legacy Patterns)

### Core Dimension Tables

Following the established legacy patterns from `dim_person_active_patients` and `dim_person_historical_practice`:

1. **`dim_person`** - Core person demographics and status (one row per person)
2. **`dim_practice`** - Practice reference data (one row per practice)
3. **`dim_person_historical_practice`** - Practice registration history (multiple rows per person)

### Key Relationships

- **`person_id`**: Unique per individual (never changes)
- **`patient_id`**: Changes when registering at new practice (EMIS number)
- **`sk_patient_id`**: Surrogate key (also not necessarily unique per person)
- **Practice details**: Separated into dedicated dimensions

## Migration Principles

### 1. Data Completeness by Layer

**Critical Rule: Intermediate models include ALL persons, analytical models filter**

- **Staging Layer**: 1:1 source mappings (no filtering)
- **Intermediate Layer**: ALL persons/patients (comprehensive data for flexibility)
- **Mart Layer**: Filtered populations as needed (active patients, specific age groups, etc.)

**Why this matters:**
- Intermediate tables serve multiple downstream use cases
- Historical analysis often needs inactive/deceased patients
- Different analytical models need different population filters
- Avoids having to rebuild base data for different audiences

**Example Flow:**
```
stg_olids_observation → int_blood_pressure_all (ALL persons) → fct_bp_active_patients (active only)
                                                             → fct_bp_population_trends (ALL persons)
                                                             → fct_bp_paediatric (age <18 only)
```

### 2. Legacy Dynamic Table Migration

The `legacy/` folder contains numerous dynamic tables that require systematic migration:

**Before Migration Checklist:**

- [ ] Identify the clinical domain (observations, medications, encounters, etc.)
- [ ] Determine data sources and dependencies
- [ ] Map to appropriate dbt layer (staging → intermediate → mart)
- [ ] Identify reusable patterns that can use existing macros

**Migration Strategy:**

1. **Extract** source logic to staging models
2. **Transform** business logic using our macro patterns
3. **Load** into appropriate intermediate/mart models
4. **Test** with custom healthcare-specific tests
5. **Document** clinical logic and validation rules

### 2. Dimensional Patterns & Data Completeness

**Core Principle: Intermediate models should include ALL persons, marts can filter as needed**

#### Person/Patient Identification Patterns

```sql
-- ALWAYS use dimension tables directly for person/patient relationships
-- DON'T write custom patient linkage logic

-- INTERMEDIATE MODELS: Include ALL persons (active, inactive, deceased)
FROM {{ get_observations("'CLUSTER_IDS'") }} obs
LEFT JOIN {{ ref('dim_person') }} p
    ON obs.person_id = p.person_id
-- No filtering by active status - comprehensive intermediate data

-- MART/ANALYTICAL MODELS: Filter to specific populations as needed
FROM {{ ref('int_observations_all') }} obs
INNER JOIN {{ ref('dim_person_active_patients') }} ap
    ON obs.person_id = ap.person_id
-- Filters to active patients only with full practice details

-- Alternative: Use main dimension with manual filtering
FROM {{ ref('int_observations_all') }} obs
INNER JOIN {{ ref('dim_person') }} p ON obs.person_id = p.person_id
INNER JOIN {{ ref('dim_person_age') }} age ON p.person_id = age.person_id
WHERE p.person_id IN (
    SELECT person_id FROM {{ ref('dim_person_active_patients') }}
)
```

#### Clinical Observations

```sql
-- ALWAYS use for observation data
{{ get_observations("'CLUSTER_ID1', 'CLUSTER_ID2'") }}

-- Example: Blood pressure readings (intermediate layer - ALL persons)
{{ get_observations("'SYSBP_COD', 'DIABP_COD', 'BP_COD'") }}

-- For comprehensive intermediate models:
FROM {{ get_observations("'CLUSTER_IDS'") }} obs
LEFT JOIN {{ ref('dim_person') }} p ON obs.person_id = p.person_id
-- Include ALL observations regardless of person status

-- For analytical models:
FROM {{ ref('int_blood_pressure_all') }} bp
INNER JOIN {{ ref('dim_person_active_patients') }} ap
    ON bp.person_id = ap.person_id
-- Filter to active patients for analysis
```

#### Medication Data

```sql
-- ALWAYS use for medication-related queries
{{ get_medication_orders() }}
-- Handles complex medication mapping and BNF code resolution
```

#### Date Filtering

```sql
-- Use consistent date filtering patterns
{{ filter_by_date(date_column='clinical_effective_date', 
                  from_date='2020-01-01') }}
```

#### Latest Events

```sql
-- For getting most recent clinical events
{{ get_latest_events(partition_by=['person_id'], 
                     order_by='clinical_effective_date') }}
```

## Naming Conventions

### Staging Models

**Pattern:** `stg_{source_schema}_{table_name}.sql`

```
stg_olids_observation.sql       # OLIDS observation data
stg_codesets_bnf_latest.sql     # BNF reference codes
stg_rulesets_bp_thresholds.sql  # Clinical thresholds
```

### Intermediate Models

**Pattern:** `int_{clinical_domain}_{descriptor}.sql`

```
int_blood_pressure_all.sql      # All BP readings consolidated
int_blood_pressure_latest.sql   # Most recent BP per person
int_medications_current.sql     # Active medications
int_encounters_primary_care.sql # GP encounters only
```

### Mart Models

**Pattern:** `fct_{entity}_{grain}.sql` or `dim_{entity}.sql`

```
fct_person_observations.sql     # Person-level observation facts
dim_clinical_codes.sql          # Clinical code dimension
fct_medication_adherence.sql    # Medication adherence metrics
```

## Clinical Data Patterns

### Concept Mapping Strategy

All clinical codes MUST use the standardised mapping approach:

```sql
-- Standard pattern for concept resolution
FROM {{ ref('stg_olids_observation') }} o
JOIN {{ ref('stg_codesets_mapped_concepts') }} mc
    ON o.observation_core_concept_id = mc.source_code_id
JOIN {{ ref('stg_codesets_combined_codesets') }} cc
    ON mc.concept_code = cc.code
WHERE cc.cluster_id IN ({{ cluster_ids }})
```

### Data Quality Validation

**Clinical Range Validation Examples:**

```sql
-- Blood Pressure ranges
systolic_value >= 40 AND systolic_value <= 350
diastolic_value >= 20 AND diastolic_value <= 200

-- Age validation
age_at_event >= 0 AND age_at_event <= 120

-- Date validation (no future dates for historical data)
clinical_effective_date <= CURRENT_DATE()
```

### Temporal Patterns

**Standard Date Fields:**

- `clinical_effective_date`: When the clinical event occurred
- `date_recorded`: When the record was created in the system
- `lds_datetime_data_acquired`: LDS processing timestamp

**Event Deduplication:**

```sql
-- Use window functions for latest events
ROW_NUMBER() OVER (
    PARTITION BY person_id, clinical_concept_id 
    ORDER BY clinical_effective_date DESC, date_recorded DESC
) as rn
```

## Testing Requirements

### Mandatory Tests for All Models

#### Staging Models

```yaml
tests:
  - all_source_columns_in_staging  # Custom test
  - not_null: [patient_id, clinical_effective_date]
  - no_future_dates: [clinical_effective_date]
```

#### Intermediate/Mart Models

```yaml
tests:
  - unique: [surrogate_key]
  - not_null: [person_id, clinical_effective_date]
  - cluster_ids_exist: [cluster_id]  # Custom test
  - relationships:
      to: ref('dim_person')
      field: person_id
```

### Custom Healthcare Tests

Located in `macros/testing/generic/`:

- `test_all_source_columns_in_staging`: Ensures staging completeness
- `test_no_future_dates`: Clinical date validation
- `test_bnf_codes_exist`: Medication code validation
- `test_cluster_ids_exist`: Concept mapping validation

## Documentation Standards

### Model Documentation

Every model MUST include:

```yaml
description: |
  Clinical purpose and scope of the model.
  Key transformations and business rules applied.
  Data quality considerations and validation rules.
  
columns:
  - name: person_id
    description: "Unique person identifier across the HEI system"
    tests: [not_null, unique]
```

### Inline Comments

```sql
-- Clinical business rule: BP readings outside normal ranges
-- Systolic: 40-350 mmHg, Diastolic: 20-200 mmHg
-- Source: NHS clinical guidelines
WHERE systolic_value BETWEEN 40 AND 350
```

### Table Comments (for important models)

```sql
{{
    config(
        post_hook=[
            "COMMENT ON TABLE {{ this }} IS 'Comprehensive clinical description...'"
        ]
    )
}}
```

## Performance Considerations

### Materialisation Strategy

- **Staging**: Views (always fresh, minimal compute)
- **Intermediate**: Tables (complex joins, better performance)
- **Marts**: Tables (analytical queries, end-user performance)

### Indexing Hints

```sql
-- Consider clustering for large tables
{{
    config(
        cluster_by=['person_id', 'clinical_effective_date']
    )
}}
```

## Migration Checklist

### For Each Legacy Dynamic Table:

- [ ] **Analyse** the existing dynamic table logic
- [ ] **Identify** which macros can replace custom SQL
- [ ] **Map** to appropriate dbt layer
- [ ] **Extract** staging models for new sources (if needed)
- [ ] **Create** intermediate model using standard patterns
- [ ] **Apply** clinical validation rules
- [ ] **Add** comprehensive tests
- [ ] **Document** clinical logic and business rules
- [ ] **Validate** results against legacy output
- [ ] **Performance test** with production data volumes

### Red Flags to Avoid:

❌ Custom patient linkage logic (use dimensions/macros)
❌ Hardcoded clinical thresholds (use reference tables)
❌ Bespoke concept mapping (use standard pattern)
❌ Missing data quality validation
❌ Unclear clinical documentation
❌ No testing coverage
❌ Non-standard naming conventions
❌ Including inactive/deceased patients without filtering

### Green Flags to Embrace:

✅ Macro usage for common patterns
✅ Direct dimension table usage for person/patient relationships
✅ Active patient filtering using is_active flag
✅ Comprehensive clinical validation
✅ Clear business rule documentation
✅ Consistent naming conventions
✅ Appropriate materialisation strategy
✅ Healthcare-specific testing
✅ Traceability to source systems

## Contact & Questions

When in doubt about migration patterns or clinical logic, refer back to this document. The patterns documented here represent proven approaches that maintain data quality, performance, and clinical accuracy.

---

*Last Updated: 2024*
*Version: 2.0*
