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

## Critical Macro Usage Patterns

### ⚠️ AVOID RECURSIVE CTE ISSUES

**❌ INCORRECT PATTERN (Causes recursive WITH errors):**
```sql
WITH base_orders AS (
    SELECT * FROM {{ get_medication_orders(bnf_code='0304') }}
)
```

**❌ INCORRECT PATTERN (Causes nested SELECT errors):**
```sql
FROM {{ get_observations("'AST_COD', 'ASTRES_COD'") }} obs
```

**✅ CORRECT PATTERN (Use subquery wrapper):**
```sql
-- For medication models
FROM ({{ get_medication_orders(bnf_code='0304') }}) base_orders

-- For diagnosis/observation models  
FROM ({{ get_observations("'AST_COD', 'ASTRES_COD'") }}) obs
```

### Medication Models - Correct Pattern

```sql
{{
    config(
        materialized='table',
        cluster_by=['person_id', 'order_date']
    )
}}

SELECT
    base_orders.person_id,
    base_orders.medication_order_id,
    base_orders.order_date,
    base_orders.order_medication_name,
    -- ... other base columns with base_orders. prefix
    
    -- Derived fields
    CASE 
        WHEN base_orders.statement_medication_name ILIKE '%IBUPROFEN%' THEN 'IBUPROFEN'
        ELSE 'OTHER'
    END AS medication_type
    
FROM ({{ get_medication_orders(bnf_code='1001') }}) base_orders
WHERE base_orders.bnf_code LIKE '100101%'
ORDER BY base_orders.person_id, base_orders.order_date DESC
```

### Diagnosis Models - Correct Pattern

```sql
{{
    config(
        materialized='table',
        cluster_by=['person_id', 'clinical_effective_date']
    )
}}

WITH base_observations AS (
    SELECT
        obs.observation_id,
        obs.person_id,
        obs.clinical_effective_date,
        obs.mapped_concept_code AS concept_code,
        obs.mapped_concept_display AS concept_display,
        obs.cluster_id AS source_cluster_id,
        
        -- Derived flags
        CASE WHEN obs.cluster_id = 'DM_COD' THEN TRUE ELSE FALSE END AS is_diabetes_code
        
    FROM ({{ get_observations("'DM_COD', 'DMRES_COD'") }}) obs
    WHERE obs.clinical_effective_date IS NOT NULL
)

-- Use conditional aggregation instead of FILTER clauses for Snowflake
SELECT 
    person_id,
    ARRAY_AGG(DISTINCT CASE WHEN is_diabetes_code THEN concept_code ELSE NULL END) AS diabetes_codes
FROM base_observations  
GROUP BY person_id
```

### Column Mapping for get_observations() Macro

**The `get_observations()` macro returns these columns:**
- `observation_id` ✅
- `person_id` ✅  
- `clinical_effective_date` ✅
- `mapped_concept_code` → alias as `concept_code`
- `mapped_concept_display` → alias as `concept_display`
- `cluster_id` → alias as `source_cluster_id`

**Always use proper aliasing:**
```sql
obs.mapped_concept_code AS concept_code,
obs.mapped_concept_display AS concept_display,
obs.cluster_id AS source_cluster_id
```

### Snowflake Compatibility Notes

**❌ FILTER clause not supported:**
```sql
ARRAY_AGG(value) FILTER (WHERE condition)  -- PostgreSQL syntax
```

**✅ Use conditional aggregation:**
```sql
ARRAY_AGG(CASE WHEN condition THEN value ELSE NULL END)  -- Snowflake compatible
```

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

## New Diagnosis Model Architecture (Improved SRP)

### Single Responsibility Principle for Diagnosis Models

**OLD Legacy Pattern** (violates SRP):
```
fct_person_dx_diabetes.sql → Does everything (data collection + QOF logic + register creation)
```

**NEW Improved Pattern** (follows SRP):
```
int_diabetes_diagnoses_all.sql → Data collection from QOF cluster IDs
fct_person_diabetes_register.sql → QOF register logic + criteria application
```

### Benefits of New Architecture

1. **Data Collection Layer** (`int_*_diagnoses_all.sql`):
   - Uses standardised `get_observations()` macro
   - Collects ALL diagnosis observations for flexibility
   - QOF cluster ID validation with `cluster_ids_exist` tests
   - Person-level aggregates for downstream use
   - Clinical flags and derived fields for context
   
2. **Business Logic Layer** (`fct_person_*_register.sql`):
   - Applies QOF-specific business rules
   - Age restrictions and eligibility criteria
   - Cross-model joins (spirometry, medications, etc.)
   - Register inclusion/exclusion logic
   - Final analytical model for end users

### QOF Diagnosis Model Pattern

**Template Structure for Diagnosis Intermediate Models:**

```sql
-- Data collection using our macro
FROM {{ get_observations("'CONDITION_COD', 'CONDITIONRES_COD'") }} obs

-- QOF-specific flags (observation-level only)
CASE WHEN obs.source_cluster_id = 'CONDITION_COD' THEN TRUE ELSE FALSE END AS is_diagnosis_code
CASE WHEN obs.source_cluster_id = 'CONDITIONRES_COD' THEN TRUE ELSE FALSE END AS is_resolved_code

-- NO person-level aggregates in intermediate layer for incremental refresh efficiency
-- Complex aggregations and QOF business logic applied in fact layer
```

**Key Architectural Decision:**
- **Intermediate models**: Simple, one row per observation, minimal derived fields
- **Fact models**: Complex aggregations, QOF business rules, person-level analysis
- **Benefits**: Simpler incremental refreshes, cleaner separation of concerns

**YAML Documentation Requirements:**
- `cluster_ids_exist` test with comma-separated cluster IDs
- Comprehensive QOF context in description
- Clinical purpose explanation
- Boolean flag validation
- Individual YAML files (not shared schema.yml)

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

### YAML Documentation & Testing Requirements

#### Individual Model YAML Files

**CRITICAL RULE: Each model MUST have its own individual YAML file**

- Use format: `{model_name}.yml` (e.g., `int_bmi_all.yml`)
- Include comprehensive column documentation
- Add appropriate data quality tests for clinical ranges
- Document business logic and clinical validation rules

#### ⚠️ AVOIDING DUPLICATE TESTS

**❌ INCORRECT - Causes duplicate test conflicts:**
```yaml
# int_diabetes_diagnoses_all.yml
models:
  - name: int_diabetes_diagnoses_all
    tests:  # ❌ Model-level tests block
      - cluster_ids_exist:
          cluster_ids: "DM_COD,DMRES_COD"
    columns:
      - name: person_id
        tests: [not_null]  # ❌ Will conflict with model-level tests
```

**✅ CORRECT - Use ONLY column-level tests:**
```yaml
# int_diabetes_diagnoses_all.yml  
models:
  - name: int_diabetes_diagnoses_all
    description: |
      All diabetes diagnosis observations from clinical records.
      Uses QOF diabetes cluster IDs: DM_COD, DMTYPE1_COD, DMTYPE2_COD, DMRES_COD
    # ✅ NO model-level tests block
    
    columns:
      - name: person_id
        description: "Unique person identifier"
        tests: [not_null]
        
      - name: concept_code
        description: "Clinical concept code"
        tests: 
          - not_null
          - cluster_ids_exist:
              cluster_ids: "DM_COD,DMTYPE1_COD,DMTYPE2_COD,DMRES_COD"
```

#### Mandatory Tests for All Models

#### Staging Models

```yaml
# Column-level tests only
columns:
  - name: patient_id
    tests: [not_null]
  - name: clinical_effective_date  
    tests: 
      - not_null
      - no_future_dates  # Custom generic test
```

#### Intermediate/Mart Models

```yaml
# Column-level tests only
columns:
  - name: person_id
    tests: 
      - not_null
      - relationships:
          to: ref('dim_person')
          field: person_id
          
  - name: clinical_effective_date
    tests: 
      - not_null
      - no_future_dates  # Custom generic test
    
        - name: concept_code  # For observation models
        tests:
          - test_cluster_ids_exist:
              cluster_ids: "AST_COD,ASTRES_COD"  # Comma-separated
              
      - name: bnf_code  # For medication models  
        tests:
          - test_bnf_codes_exist:
              bnf_codes: "0304,1001"  # Comma-separated
          
  - name: is_diabetes_code  # Boolean flags
    tests:
      - accepted_values:
          values: [true, false]
          
        - name: systolic_value  # Clinical measurements
        tests:
          - accepted_range:  # dbt built-in range validation
              min_value: 40
              max_value: 350
              severity: warn
              
      - name: diastolic_value  # Clinical measurements
        tests:
          - accepted_range:  # dbt built-in range validation
              min_value: 20
              max_value: 200
              severity: warn
```

#### Code Validation Tests (CRITICAL)

**For Clinical Observation Models:**
```yaml
tests:
  - test_cluster_ids_exist:
      cluster_ids: "SYSBP_COD,DIABP_COD,BP_COD"  # Comma-separated list
```

**For Medication Models:**
```yaml
tests:
  - test_bnf_codes_exist:
      bnf_codes: "0601,0212"  # Comma-separated BNF codes  
```

**Why These Tests Matter:**
- Validates that our filters are actually finding data
- Prevents silent failures where models run but return no results
- Ensures cluster IDs and BNF codes exist in the mapping tables
- Critical for medication safety and clinical accuracy

### Custom Healthcare Tests

Located in `macros/testing/generic/`:

- `test_no_future_dates`: Clinical date validation - ensures dates are not in the future
- `test_cluster_ids_exist`: Concept mapping validation - ensures cluster IDs used in filters exist in codesets
- `test_bnf_codes_exist`: Medication code validation - ensures BNF codes used in filters exist in codesets  
- `test_all_source_columns_in_staging`: Ensures staging completeness (staging models only)

**✅ Use dbt built-in tests and our custom healthcare-specific tests:**

```yaml
# ✅ dbt built-in tests for clinical data
- test_no_future_dates      # Custom date validation
- accepted_range:           # dbt built-in range validation
    min_value: 40
    max_value: 350
- test_cluster_ids_exist:   # Healthcare-specific validation
    cluster_ids: "AST_COD,ASTRES_COD"
```

#### Usage Examples:

**Clinical Observation Models:**
```yaml
# Blood pressure model using multiple cluster IDs
tests:
  - test_cluster_ids_exist:
      cluster_ids: "SYSBP_COD,DIABP_COD,BP_COD"

# Single cluster ID models  
tests:
  - test_cluster_ids_exist:
      cluster_ids: "BMIVAL_COD"
```

**Medication Models:**
```yaml
# Single BNF chapter
tests:
  - test_bnf_codes_exist:
      bnf_codes: "0601"  # Diabetes medications

# Multiple BNF codes
tests:
  - test_bnf_codes_exist:
      bnf_codes: "100101,100302"  # NSAID codes
```

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
- [ ] **Create** individual YAML file with comprehensive documentation and tests
- [ ] **Add code validation tests** (cluster_ids_exist for observations, bnf_codes_exist for medications)
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
✅ Healthcare-specific testing (cluster_ids_exist, bnf_codes_exist)
✅ Individual YAML files with comprehensive documentation
✅ Code validation tests ensuring filters actually find data
✅ Traceability to source systems

## Migration Progress Tracking

### Phase 1: Core Intermediate Tables (Clinical Observations & Measurements)

Focus on building comprehensive intermediate models for all major clinical domains first, as these serve multiple downstream uses and are needed before condition registers can be built.

#### 1.1 Vital Signs & Basic Measurements ✅ **Priority: HIGH**
- [x] `intermediate_blood_pressure_all.sql` → `int_blood_pressure_all.sql` ✅ **COMPLETE**
- [x] `intermediate_blood_pressure_latest.sql` → `int_blood_pressure_latest.sql` ✅ **COMPLETE**
- [x] `intermediate_bmi_historic.sql` → `int_bmi_all.sql` ✅ **COMPLETE** (simple numeric BMI pattern)
- [x] `intermediate_bmi_latest.sql` → `int_bmi_latest.sql` ✅ **COMPLETE**
- [x] `intermediate_bmi_values.sql` → `int_bmi_qof.sql` ✅ **COMPLETE** (QOF obesity register rules)
- [ ] `intermediate_waist_circumference_all.sql` → `int_waist_circumference_all.sql`
- [ ] `intermediate_waist_circumference_latest.sql` → `int_waist_circumference_latest.sql`

#### 1.2 Laboratory Results ✅ **Priority: HIGH**
- [x] `intermediate_hba1c_all.sql` → `int_hba1c_all.sql` ✅ **COMPLETE**
- [x] `intermediate_hba1c_latest.sql` → `int_hba1c_latest.sql` ✅ **COMPLETE**
- [x] `intermediate_total_cholesterol_all.sql` → `int_cholesterol_all.sql` ✅ **COMPLETE**
- [x] `intermediate_total_cholesterol_latest.sql` → `int_cholesterol_latest.sql` ✅ **COMPLETE**
- [x] `intermediate_egfr_all.sql` → `int_egfr_all.sql` ✅ **COMPLETE**
- [x] `intermediate_egfr_latest.sql` → `int_egfr_latest.sql` ✅ **COMPLETE**
- [x] `intermediate_serum_creatinine_all.sql` → `int_creatinine_all.sql` ✅ **COMPLETE**
- [x] `intermediate_serum_creatinine_latest.sql` → `int_creatinine_latest.sql` ✅ **COMPLETE**
- [x] `intermediate_urine_acr_all.sql` → `int_urine_acr_all.sql` ✅ **COMPLETE**
- [x] `intermediate_urine_acr_latest.sql` → `int_urine_acr_latest.sql` ✅ **COMPLETE**

#### 1.1.1 Additional Vital Signs ✅ **Priority: HIGH**
- [x] `intermediate_waist_circumference_all.sql` → `int_waist_circumference_all.sql` ✅ **COMPLETE**
- [x] `intermediate_waist_circumference_latest.sql` → `int_waist_circumference_latest.sql` ✅ **COMPLETE**

#### 1.3 Risk Assessments & Scores ✅ **Priority: MEDIUM**
- [x] `intermediate_qrisk_all.sql` → `int_qrisk_all.sql` ✅ **COMPLETE**
- [x] `intermediate_qrisk_latest.sql` → `int_qrisk_latest.sql` ✅ **COMPLETE**
- [x] `intermediate_smoking_all.sql` → `int_smoking_status_all.sql` ✅ **COMPLETE**
- [x] `intermediate_smoking_latest.sql` → `int_smoking_status_latest.sql` ✅ **COMPLETE**

#### 1.4 Clinical Examinations ✅ **Priority: MEDIUM**
- [x] `intermediate_spirometry.sql` → `int_spirometry_all.sql` ✅ **COMPLETE**
- [x] `intermediate_foot_check_all.sql` → `int_foot_examination_all.sql` ✅ **COMPLETE**
- [x] `intermediate_foot_check_latest.sql` → `int_foot_examination_latest.sql` ✅ **COMPLETE**
- [x] `intermediate_retinal_screening_all.sql` → `int_retinal_screening_all.sql` ✅ **COMPLETE**
- [x] `intermediate_retinal_screening_latest.sql` → `int_retinal_screening_latest.sql` ✅ **COMPLETE**

#### 1.5 Health Checks & Assessments ✅ **Priority: MEDIUM**
- [x] `intermediate_nhs_health_check_all.sql` → `int_nhs_health_check_all.sql` ✅ **COMPLETE**
- [x] `intermediate_nhs_health_check_latest.sql` → `int_nhs_health_check_latest.sql` ✅ **COMPLETE**

#### 1.6 Demographics & Social Factors ✅ **Priority: MEDIUM**
- [x] `intermediate_ethnicity_all.sql` → `int_ethnicity_all.sql` ✅ **COMPLETE**
- [x] `intermediate_ethnicity_qof.sql` → `int_ethnicity_qof.sql` ✅ **COMPLETE**

### Phase 2: Medication Intermediate Tables

#### 2.1 Diabetes Medications ✅ **Priority: HIGH**
- [x] `intermediate_diabetes_orders_all.sql` → `int_diabetes_medications_all.sql` ✅ **COMPLETE**

#### 2.2 Cardiovascular Medications ✅ **Priority: HIGH**
- [x] `intermediate_statin_orders_all.sql` → `int_statin_medications_all.sql` ✅ **COMPLETE**
- [x] `intermediate_ace_inhibitor_orders_all.sql` → `int_ace_inhibitor_medications_all.sql` ✅ **COMPLETE**
- [x] `intermediate_arb_orders_all.sql` → `int_arb_medications_all.sql` ✅ **COMPLETE**
- [x] `intermediate_beta_blocker_orders_all.sql` → `int_beta_blocker_medications_all.sql` ✅ **COMPLETE**
- [x] `intermediate_diuretic_orders_all.sql` → `int_diuretic_medications_all.sql` ✅ **COMPLETE**
- [x] `intermediate_antiplatelet_orders_all.sql` → `int_antiplatelet_medications_all.sql` ✅ **COMPLETE**
- [x] `intermediate_oral_anticoagulant_orders_all.sql` → `int_anticoagulant_medications_all.sql` ✅ **COMPLETE**

#### 2.3 Respiratory Medications ✅ **Priority: MEDIUM**
- [x] `intermediate_inhaled_corticosteroid_orders_all.sql` → `int_inhaled_corticosteroid_medications_all.sql` ✅ **COMPLETE**
- [x] `intermediate_systemic_corticosteroid_orders_all.sql` → `int_systemic_corticosteroid_medications_all.sql` ✅ **COMPLETE**

#### 2.4 Other Medications ✅ **Priority: MEDIUM**
- [x] `intermediate_ppi_orders_all.sql` → `int_ppi_medications_all.sql` ✅ **COMPLETE**
- [x] `intermediate_nsaid_orders_all.sql` → `int_nsaid_medications_all.sql` ✅ **COMPLETE**
- [x] `intermediate_antidepressant_orders_all.sql` → `int_antidepressant_medications_all.sql` ✅ **COMPLETE**
- [x] `intermediate_cardiac_glycoside_orders_all.sql` → `int_cardiac_glycoside_medications_all.sql` ✅ **COMPLETE**
- [x] `intermediate_lithium_orders.sql` → `int_lithium_medications_all.sql` ✅ **COMPLETE**

#### 2.5 Specialist Medications ✅ **Priority: LOW** ✅ **COMPLETE**
- [x] `intermediate_valproate_orders_all.sql` → `int_valproate_medications_all.sql` ✅ **COMPLETE**
- [x] `intermediate_asthma_orders_12m.sql` → `int_asthma_medications_12m.sql` ✅ **COMPLETE**
- [x] `intermediate_epilepsy_orders_6m.sql` → `int_epilepsy_medications_6m.sql` ✅ **COMPLETE**
- [x] `intermediate_allergy_orders_all.sql` → `int_allergy_medications_all.sql` ✅ **COMPLETE**

### Phase 3: Clinical Condition Intermediate Tables

#### 3.1 Major Chronic Conditions ✅ **Priority: HIGH**
- [x] `intermediate_diabetes_diagnoses.sql` → `int_diabetes_diagnoses_all.sql` ✅ **COMPLETE** (QOF diabetes cluster IDs: DM_COD, DMTYPE1_COD, DMTYPE2_COD, DMRES_COD)
- [x] `intermediate_copd_diagnoses.sql` → `int_copd_diagnoses_all.sql` ✅ **COMPLETE** (QOF COPD cluster IDs: COPD_COD, COPDRES_COD with April 2023 spirometry rules)
- [x] `intermediate_hf_details.sql` → `int_heart_failure_diagnoses_all.sql` ✅ **COMPLETE** (QOF heart failure cluster IDs: HF_COD, HFRES_COD, HFLVSD_COD, REDEJCFRAC_COD)
- [x] `fct_person_dx_hypertension.sql` → `int_hypertension_diagnoses_all.sql` ✅ **COMPLETE** (QOF hypertension cluster IDs: HYP_COD, HYPRES_COD)
- [ ] `intermediate_depression_details.sql` → `int_depression_diagnoses_all.sql`
- [ ] `intermediate_mh_diagnoses.sql` → `int_mental_health_diagnoses_all.sql`
- [ ] `intermediate_cancer_details.sql` → `int_cancer_diagnoses_all.sql`

#### 3.2 Chronic Disease Complications ✅ **Priority: MEDIUM**
- [ ] `intermediate_osteoporosis_diagnoses.sql` → `int_osteoporosis_diagnoses_all.sql`
- [ ] `intermediate_fragility_fractures.sql` → `int_fragility_fractures_all.sql`
- [ ] `intermediate_ckd_lab_inference.sql` → `int_ckd_lab_inference_all.sql`
- [ ] `intermediate_ndh_diagnoses.sql` → `int_diabetic_retinopathy_diagnoses_all.sql`

#### 3.3 Other Clinical Conditions ✅ **Priority: LOW**
- [ ] `intermediate_ld_diagnoses_all.sql` → `int_learning_disability_diagnoses_all.sql`
- [ ] `intermediate_copd_unable_spirometry.sql` → `int_copd_unable_spirometry_all.sql`
- [ ] `intermediate_perm_absence_preg_risk.sql` → `int_pregnancy_absence_risk_all.sql`

### Phase 4: Fact Table Migration by Clinical Pattern

**Migration Strategy**: Migrate fact tables in logical order based on complexity patterns, from simple to complex.

#### 4.1 Pattern 1: Simple Register (Diagnosis Only) ✅ **Priority: HIGH**
*Logic: Simple presence of diagnosis code = on register. No resolution codes or ignored.*

**Simplicity Guidelines:**
- ❌ **No episode timing flags** (has_episode_last_12m, has_episode_last_24m) - keep models focused
- ❌ **No indexes** in config - let Snowflake handle optimization
- ✅ **Clean, minimal field set** - only essential register fields
- ✅ **Episode analysis** can be done separately if needed

- [x] `fct_person_dx_chd.sql` → `fct_person_chd_register.sql` ✅ **COMPLETE**
- [x] `fct_person_dx_pad.sql` → `fct_person_pad_register.sql` ✅ **COMPLETE**
- [x] `fct_person_dx_cancer.sql` → `fct_person_cancer_register.sql` ✅ **COMPLETE**
- [x] `fct_person_dx_ra.sql` → `fct_person_rheumatoid_arthritis_register.sql` ✅ **COMPLETE**
- [x] `fct_person_dx_stia.sql` → `fct_person_stroke_tia_register.sql` ✅ **COMPLETE**
- [x] `fct_person_dx_fhyp.sql` → `fct_person_familial_hypercholesterolaemia_register.sql` ✅ **COMPLETE**
- [x] `fct_person_dx_gestational_diabetes.sql` → `fct_person_gestational_diabetes_register.sql` ✅ **COMPLETE**
- [x] `fct_person_dx_dementia.sql` → `fct_person_dementia_register.sql` ✅ **COMPLETE** (moved from Pattern 2)
- [x] `fct_person_dx_ld.sql` → `fct_person_learning_disability_register.sql` ✅ **COMPLETE** (age ≥14 filter)
- [x] `fct_person_dx_nafld.sql` → `fct_person_nafld_register.sql` ✅ **COMPLETE** (moved from Pattern 6 - simple diagnosis with hardcoded codes)

#### 4.2 Pattern 2: Standard QOF Register (Diagnosis + Resolution) ✅ **Priority: HIGH**
*Logic: latest_diagnosis > latest_resolution OR no_resolution. Age restrictions, date thresholds, exclusion logic.*

- [x] `fct_person_dx_depression.sql` → `fct_person_depression_register.sql` ✅ **COMPLETE**
- [x] `fct_person_dx_smi.sql` → `fct_person_smi_register.sql` ✅ **COMPLETE**
- [x] `fct_person_dx_palliative_care.sql` → `fct_person_palliative_care_register.sql` ✅ **COMPLETE**
- [x] `fct_person_dx_af.sql` → `fct_person_atrial_fibrillation_register.sql` ✅ **COMPLETE** (moved from Pattern 3)
- [x] `fct_person_dx_ckd.sql` → `fct_person_ckd_register.sql` ✅ **COMPLETE** (moved from Pattern 5 - age ≥18 + diagnosis + resolution logic)

**Pattern Reclassifications:**
- `fct_person_dx_dementia.sql` → **Pattern 1** (simple diagnosis only, no resolution codes)
- `fct_person_dx_epilepsy.sql` → **Pattern 3** (external medication validation required)
- `fct_person_dx_ld.sql` → **Pattern 1 with age filter** (simple diagnosis + age ≥14, no resolution codes)
- `fct_person_dx_af.sql` → **Pattern 2** (simple diagnosis + resolution, no external validation)
- `fct_person_dx_ckd.sql` → **Pattern 2** (diagnosis + resolution, lab data enrichment only)

#### 4.3 Pattern 3: Complex QOF Register (External Validation) ✅ **Priority: MEDIUM**
*Logic: Diagnosis + additional validation requirements (medication, confirmation).*

- [x] `fct_person_dx_asthma.sql` → `fct_person_asthma_register.sql` ✅ **COMPLETE** (age ≥6 + active diagnosis + 12m medication)
- [x] `fct_person_dx_cyp_asthma.sql` → `fct_person_cyp_asthma_register.sql` ✅ **COMPLETE** (age <18 + active diagnosis + 12m medication)
- [x] `fct_person_dx_epilepsy.sql` → `fct_person_epilepsy_register.sql` ✅ **COMPLETE** (age ≥18 + active diagnosis + 6m medication)

**Pattern Reclassifications:**
- `fct_person_dx_af.sql` → **Pattern 2** → `fct_person_atrial_fibrillation_register.sql` ✅ **COMPLETE** (simple diagnosis + resolution, no medication validation)

#### 4.4 Pattern 4: Type Classification Register ✅ **Priority: MEDIUM**
*Logic: Multiple cluster types with hierarchy/precedence rules for type determination.*

- [x] `fct_person_dx_diabetes.sql` → `fct_person_diabetes_register.sql` ✅ **COMPLETE** (age ≥17 + type classification: Type 1 vs Type 2 vs Unknown)
- [x] `fct_person_dx_hf.sql` → `fct_person_heart_failure_register.sql` ✅ **COMPLETE** (general HF + LVSD/Reduced EF subtype registers)
- [x] `fct_person_dx_ndh.sql` → `fct_person_ndh_register.sql` ✅ **COMPLETE** (age ≥18 + NDH subtypes + diabetes exclusion logic)

#### 4.5 Pattern 5: Lab-Enhanced Register ✅ **DISCONTINUED**
*Pattern discontinued: Lab enhancement handled by separate intermediate tables and clinical measurement facts.*

**Design Decision**: Lab-enhanced registers would duplicate functionality already available in:
- Intermediate lab measurement tables (int_egfr_all, int_creatinine_all, int_urine_acr_all)
- Clinical measurement fact tables 
- Maintains clean separation of concerns between diagnosis registers and lab monitoring

**CKD Register Reclassification**: Moved to Pattern 2 (diagnosis + resolution logic only)

#### 4.6 Pattern 6: Complex Clinical Logic ✅ **Priority: MEDIUM**
*Logic: Multiple data sources with sophisticated clinical algorithms.*

- [x] `fct_person_dx_copd.sql` → `fct_person_copd_register.sql` ✅ **COMPLETE** (spirometry confirmation logic)
- [x] `fct_person_dx_hypertension.sql` → `fct_person_hypertension_register.sql` ✅ **COMPLETE** (diagnosis + resolution + BP staging with context-specific NICE thresholds)
- [x] `fct_person_dx_osteoporosis.sql` → `fct_person_osteoporosis_register.sql` ✅ **COMPLETE** (age 50-74 + fragility fracture + diagnosis + DXA confirmation)
- [x] `fct_person_dx_obesity.sql` → `fct_person_obesity_register.sql` ✅ **COMPLETE** (age ≥18 + ethnicity-specific BMI thresholds)

### Phase 5: Clinical Quality & Status Fact Tables

#### 5.1 Clinical Control & Quality Measures ✅ **Priority: HIGH** ✅ **COMPLETE**
- [x] `fct_person_bp_control_status.sql` → `fct_person_bp_control.sql` ✅ **COMPLETE**
- [x] `fct_person_diabetes_8_care_processes.sql` → `fct_person_diabetes_8_care_processes.sql` ✅ **COMPLETE**
- [x] `fct_person_diabetes_9_care_processes.sql` → `fct_person_diabetes_9_care_processes.sql` ✅ **COMPLETE**
- [x] `fct_person_diabetes_triple_target.sql` → `fct_person_diabetes_triple_target.sql` ✅ **COMPLETE**
- [x] `fct_person_diabetes_foot_check.sql` → `fct_person_diabetes_foot_check.sql` ✅ **COMPLETE**

#### 5.2 Patient Status & Demographics ✅ **Priority: HIGH**
- [x] `fct_person_smoking_status.sql` → `fct_person_smoking_status.sql` ✅ **COMPLETE** (using existing int_smoking_status_all/latest)
- [x] `fct_person_pregnant.sql` → `fct_person_pregnancy_status.sql` ✅ **COMPLETE** (created int_pregnancy_status_all + int_pregnancy_absence_risk_all)
- [x] `fct_person_nhs_health_check_status.sql` → `fct_person_nhs_health_check_status.sql` ✅ **COMPLETE**

#### 5.3 Clinical Safety ✅ **Priority: HIGH**
- [x] `fct_clinical_safety_on_valproate_and_pregnant.sql` → `fct_clinical_safety_valproate_pregnancy.sql` ✅ **COMPLETE** (created int_valproate_medications_6m_latest)

#### 5.4 Service Usage ✅ **Priority: MEDIUM**
- [ ] `fct_person_appointments_gp_12m.sql` → `fct_person_gp_appointments_12m.sql`
- [ ] `fct_organisation_active_patients.sql` → `fct_organisation_active_patients.sql`
- [ ] `fct_person_ltc_summary.sql` → `fct_person_ltc_summary.sql`

### Phase 6: Programme Dimensions

#### 6.1 NHS Health Checks ✅ **Priority: HIGH**
- [ ] `dim_prog_nhs_health_check_eligibility.sql` → `dim_prog_nhs_health_check_eligibility.sql`

#### 6.2 LTC/LCS Programmes ✅ **Priority: MEDIUM**
**Note**: These require most intermediate tables to be complete first
- [ ] `dim_prog_ltc_lcs_moc_base.sql` → `dim_prog_ltc_lcs_base.sql`
- [ ] `dim_prog_ltc_lcs_cf_summary.sql` → `dim_prog_ltc_lcs_summary.sql`
- [ ] `dim_prog_ltc_lcs_cf_exclusions.sql` → `dim_prog_ltc_lcs_exclusions.sql`

#### 6.3 LTC/LCS Condition-Specific Programmes ✅ **Priority: LOW**
- [ ] `dim_prog_ltc_lcs_cf_dm_61.sql` → `dim_prog_ltc_lcs_diabetes_61.sql`
- [ ] `dim_prog_ltc_lcs_cf_dm_62.sql` → `dim_prog_ltc_lcs_diabetes_62.sql`
- [ ] `dim_prog_ltc_lcs_cf_dm_63.sql` → `dim_prog_ltc_lcs_diabetes_63.sql`
- [ ] `dim_prog_ltc_lcs_cf_dm_64.sql` → `dim_prog_ltc_lcs_diabetes_64.sql`
- [ ] `dim_prog_ltc_lcs_cf_dm_65.sql` → `dim_prog_ltc_lcs_diabetes_65.sql`
- [ ] `dim_prog_ltc_lcs_cf_dm_66.sql` → `dim_prog_ltc_lcs_diabetes_66.sql`
- [ ] `dim_prog_ltc_lcs_cf_htn_61.sql` → `dim_prog_ltc_lcs_hypertension_61.sql`
- [ ] `dim_prog_ltc_lcs_cf_htn_62.sql` → `dim_prog_ltc_lcs_hypertension_62.sql`
- [ ] `dim_prog_ltc_lcs_cf_htn_63.sql` → `dim_prog_ltc_lcs_hypertension_63.sql`
- [ ] `dim_prog_ltc_lcs_cf_htn_65.sql` → `dim_prog_ltc_lcs_hypertension_65.sql`
- [ ] `dim_prog_ltc_lcs_cf_htn_66.sql` → `dim_prog_ltc_lcs_hypertension_66.sql`
- [ ] `dim_prog_ltc_lcs_cf_cvd_61.sql` → `dim_prog_ltc_lcs_cvd_61.sql`
- [ ] `dim_prog_ltc_lcs_cf_cvd_62.sql` → `dim_prog_ltc_lcs_cvd_62.sql`
- [ ] `dim_prog_ltc_lcs_cf_cvd_63.sql` → `dim_prog_ltc_lcs_cvd_63.sql`
- [ ] `dim_prog_ltc_lcs_cf_cvd_64.sql` → `dim_prog_ltc_lcs_cvd_64.sql`
- [ ] `dim_prog_ltc_lcs_cf_cvd_65.sql` → `dim_prog_ltc_lcs_cvd_65.sql`
- [ ] `dim_prog_ltc_lcs_cf_cvd_66.sql` → `dim_prog_ltc_lcs_cvd_66.sql`
- [ ] `dim_prog_ltc_lcs_cf_ckd_61.sql` → `dim_prog_ltc_lcs_ckd_61.sql`
- [ ] `dim_prog_ltc_lcs_cf_ckd_62.sql` → `dim_prog_ltc_lcs_ckd_62.sql`
- [ ] `dim_prog_ltc_lcs_cf_ckd_63.sql` → `dim_prog_ltc_lcs_ckd_63.sql`
- [ ] `dim_prog_ltc_lcs_cf_ckd_64.sql` → `dim_prog_ltc_lcs_ckd_64.sql`
- [ ] `dim_prog_ltc_lcs_cf_af_61.sql` → `dim_prog_ltc_lcs_atrial_fibrillation_61.sql`
- [ ] `dim_prog_ltc_lcs_cf_af_62.sql` → `dim_prog_ltc_lcs_atrial_fibrillation_62.sql`
- [ ] `dim_prog_ltc_lcs_cf_cyp_ast_61.sql` → `dim_prog_ltc_lcs_cyp_asthma_61.sql`

#### 6.4 Immunisation Programmes ✅ **Priority: MEDIUM**
- [ ] `dim_prog_imm_base_pop.sql` → `dim_prog_immunisation_base_population.sql`
- [ ] `dim_prog_imm_child_elig.sql` → `dim_prog_immunisation_child_eligibility.sql`
- [ ] `dim_prog_imm_child_vaccine.sql` → `dim_prog_immunisation_child_vaccine.sql`
- [ ] `dim_prog_imm_child_vaccine_temp.sql` → `dim_prog_immunisation_child_vaccine_temp.sql`

#### 6.5 Valproate Safety Programmes ✅ **Priority: LOW**
- [ ] `dim_prog_valproate_neurology.sql` → `dim_prog_valproate_neurology.sql`
- [ ] `dim_prog_valproate_psychiatry.sql` → `dim_prog_valproate_psychiatry.sql`
- [ ] `dim_prog_valproate_araf.sql` → `dim_prog_valproate_araf.sql`
- [ ] `dim_prog_valproate_araf_referral.sql` → `dim_prog_valproate_araf_referral.sql`
- [ ] `dim_prog_valproate_db_scope.sql` → `dim_prog_valproate_db_scope.sql`

### Phase 7: Remaining Complex Models

#### 7.1 LTC/LCS Intermediate Supporting Models ✅ **Priority: LOW**
**Note**: These support the LTC/LCS programme dimensions
- [ ] `intermediate_ltc_lcs_cf_base_population.sql` → `int_ltc_lcs_base_population.sql`
- [ ] `intermediate_ltc_lcs_cf_health_checks.sql` → `int_ltc_lcs_health_checks.sql`
- [ ] `intermediate_ltc_lcs_cf_cvd_base.sql` → `int_ltc_lcs_cvd_base.sql`
- [ ] `intermediate_ltc_lcs_cf_cvd_65_base.sql` → `int_ltc_lcs_cvd_65_base.sql`
- [ ] `intermediate_ltc_lcs_cf_cvd_66_base.sql` → `int_ltc_lcs_cvd_66_base.sql`
- [ ] `intermediate_ltc_lcs_cf_htn_base.sql` → `int_ltc_lcs_hypertension_base.sql`
- [ ] `intermediate_ltc_lcs_raw_data.sql` → `int_ltc_lcs_raw_data.sql`

### Migration Notes

#### Dependencies
- **`dim_person_condition_registers`** requires most clinical intermediate tables to be complete first
- **LTC/LCS programme dimensions** require comprehensive clinical and medication intermediate tables
- **Disease register fact tables** require their corresponding intermediate diagnosis and medication tables

#### Current State Summary
- ✅ **Staging Layer**: Complete for all source systems
- ✅ **Core Dimensions**: Complete for person/patient/practice relationships
- ✅ **Phase 1 Intermediate**: **100% COMPLETE!** All core clinical observations, laboratory results, risk assessments, clinical examinations, and health checks migrated
- ✅ **Phase 2.1-2.5**: **100% COMPLETE!** All medication intermediate tables migrated (diabetes, cardiovascular, respiratory, gastrointestinal, mental health, cardiac therapy, specialist medications)
- ✅ **Phase 3.1**: **100% COMPLETE!** All major chronic conditions diagnosis intermediate models (diabetes, COPD, heart failure, hypertension, CKD, depression, asthma, dementia, epilepsy, SMI, learning disability)
- ✅ **Phase 4.1**: **100% COMPLETE!** Pattern 1 registers (10/10 simple diagnosis-only registers including reclassified NAFLD)
- ✅ **Phase 4.2**: **100% COMPLETE!** Pattern 2 registers (5/5 diagnosis + resolution registers including CKD)
- ✅ **Phase 4.3**: **100% COMPLETE!** Pattern 3 registers (3/3 complex registers with external medication validation)
- ✅ **Phase 4.4**: **100% COMPLETE!** Pattern 4 registers (3/3 type classification registers with complex business logic)
- ❌ **Phase 4.5**: **DISCONTINUED** Pattern 5 (lab-enhanced registers - functionality handled separately)
- ✅ **Phase 4.6**: **100% COMPLETE!** Pattern 6 registers (4/4 complex clinical logic registers)
- 🎯 **MAJOR MILESTONE**: **ALL QOF REGISTER PATTERNS COMPLETE!** 25 total register fact tables implemented

#### Next Priority Actions
1. **🎯 Phase 5**: Clinical Quality & Status Fact Tables (diabetes care processes, BP control, clinical safety measures)
2. **Then**: Phase 6 - Programme Dimensions (NHS Health Checks, LTC/LCS programmes)
3. **Finally**: Phase 7 - Remaining Complex Models (LTC/LCS supporting models)

## Troubleshooting Common Issues

### 🔧 dbt Compilation Errors

#### "Recursive CTE" or "WITH clause" errors
**Symptoms:** Models fail with recursive CTE or WITH clause compilation errors
**Cause:** Using macros directly in WITH clauses
**Solution:** Use subquery pattern:
```sql
# ❌ Wrong: WITH base AS (SELECT * FROM {{ macro() }})
# ✅ Right: FROM ({{ macro() }}) base
```

#### "Invalid identifier" errors
**Symptoms:** Column not found errors during compilation
**Cause:** Incorrect column names from macro output
**Solution:** Use proper column mapping:
```sql
# For get_observations() macro:
obs.mapped_concept_code AS concept_code,
obs.mapped_concept_display AS concept_display,
obs.cluster_id AS source_cluster_id
```

#### "FILTER clause not supported" errors  
**Symptoms:** FILTER (WHERE ...) syntax fails in Snowflake
**Solution:** Use conditional aggregation:
```sql
# ❌ Wrong: ARRAY_AGG(value) FILTER (WHERE condition)
# ✅ Right: ARRAY_AGG(CASE WHEN condition THEN value ELSE NULL END)
```

### 🔧 YAML Test Errors

#### "Duplicate test" errors
**Symptoms:** Tests fail with duplicate test name conflicts
**Cause:** Both model-level and column-level tests defined
**Solution:** Use ONLY column-level tests - remove any `tests:` block under the model name

#### "cluster_ids_exist" test failures
**Symptoms:** Test fails saying cluster IDs don't exist
**Cause:** Incorrect cluster ID spelling or missing IDs in codesets
**Solution:** 
1. Check spelling of cluster IDs
2. Verify IDs exist in `stg_codesets_combined_codesets`
3. Use comma-separated format: `"AST_COD,ASTRES_COD"`

### 🔧 Performance Issues

#### Slow model compilation
**Symptoms:** Models take very long to compile/run
**Cause:** Inefficient macro usage or missing clustering
**Solution:**
1. Use subquery pattern for macros
2. Add appropriate clustering: `cluster_by=['person_id', 'clinical_effective_date']`
3. Avoid unnecessary joins in intermediate models

#### Memory errors
**Symptoms:** Out of memory errors during large aggregations
**Cause:** Complex aggregations without proper filtering
**Solution:**
1. Filter early in WHERE clauses
2. Use incremental materialization for large tables
3. Break complex logic into multiple steps

### 🔧 Data Quality Issues

#### Empty result sets
**Symptoms:** Models run successfully but return no data
**Cause:** Incorrect cluster IDs or date filtering
**Solution:**
1. Verify cluster IDs using `cluster_ids_exist` test
2. Check date filters aren't too restrictive
3. Validate source data exists for the time period

#### Unexpected NULL values
**Symptoms:** Expected data shows as NULL
**Cause:** Incorrect column mapping or joins
**Solution:**
1. Check column aliases match macro output
2. Use LEFT JOIN appropriately for optional data
3. Add NOT NULL tests to catch issues early

## Contact & Questions

When in doubt about migration patterns or clinical logic, refer back to this document. The patterns documented here represent proven approaches that maintain data quality, performance, and clinical accuracy.

**Key Resources:**
- **Macro Usage**: Always use subquery pattern to avoid recursive CTE issues
- **YAML Testing**: Use only column-level tests to avoid duplicates  
- **Column Mapping**: Use proper aliases for macro outputs
- **Snowflake Compatibility**: Use conditional aggregation instead of FILTER clauses
