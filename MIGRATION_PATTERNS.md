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

## Advanced Architecture Patterns

### Event-Based Clinical Measurement Consolidation

**Pattern**: Transform observation-level clinical measurements into event-based paired readings with clinical context intelligence.

**Use Case**: Critical for measurements requiring paired values (systolic/diastolic BP, height/weight, before/after readings) where clinical context affects interpretation.

#### Legacy vs Enhanced Architecture Comparison

**❌ Legacy Observation-Level Structure:**
```sql
-- One row per individual measurement observation
person_id | date       | measurement_type | value | context
12345     | 2024-01-15 | Systolic        | 140   | Clinic
12345     | 2024-01-15 | Diastolic       | 90    | Clinic
12345     | 2024-01-15 | Systolic        | 135   | Home
12345     | 2024-01-15 | Diastolic       | 85    | Home
```

**✅ Enhanced Event-Based Structure:**
```sql
-- One row per clinical event with paired values and context
person_id | date       | systolic | diastolic | is_home_bp | is_abpm_bp | context
12345     | 2024-01-15 | 140      | 90        | FALSE      | FALSE      | Clinic  
12345     | 2024-01-15 | 135      | 85        | TRUE       | FALSE      | Home
```

#### Implementation Pattern

**1. Event Consolidation Logic:**
```sql
WITH base_observations AS (
    SELECT 
        obs.person_id,
        obs.clinical_effective_date,
        obs.result_value,
        obs.cluster_id AS source_cluster_id,
        obs.mapped_concept_display AS concept_display
    FROM ({{ get_observations("'CLUSTER_IDS'") }}) obs
    WHERE obs.result_value IS NOT NULL
    AND obs.clinical_effective_date IS NOT NULL
    -- Apply clinical range validation
    AND obs.result_value BETWEEN min_plausible AND max_plausible
),

row_classification AS (
    SELECT *,
        -- Classify measurement type
        (source_cluster_id = 'SYSBP_COD' OR 
         (source_cluster_id = 'BP_COD' AND concept_display ILIKE '%systolic%')) AS is_systolic_row,
        (source_cluster_id = 'DIABP_COD' OR 
         (source_cluster_id = 'BP_COD' AND concept_display ILIKE '%diastolic%')) AS is_diastolic_row,
         
        -- Detect clinical context
        (source_cluster_id IN ('HOMEBP_COD', 'HOMEAMBBP_COD') OR 
         concept_display ILIKE '%home%') AS is_home_measurement,
        (source_cluster_id = 'ABPM_COD' OR 
         concept_display ILIKE '%ambulatory%') AS is_abpm_measurement
    FROM base_observations
),

event_consolidation AS (
    SELECT
        person_id,
        clinical_effective_date,
        
        -- Paired value consolidation
        MAX(CASE WHEN is_systolic_row THEN result_value END) AS systolic_value,
        MAX(CASE WHEN is_diastolic_row THEN result_value END) AS diastolic_value,
        
        -- Clinical context aggregation
        BOOLOR_AGG(is_home_measurement) AS is_home_bp_event,
        BOOLOR_AGG(is_abpm_measurement) AS is_abpm_bp_event,
        
        -- Rich traceability metadata
        ARRAY_AGG(DISTINCT observation_id) AS all_observation_ids,
        ARRAY_AGG(DISTINCT source_cluster_id) AS all_source_clusters,
        ARRAY_AGG(DISTINCT concept_display) AS all_concept_displays
        
    FROM row_classification
    GROUP BY person_id, clinical_effective_date
    
    -- Quality filters: Ensure paired readings for clinical validity
    HAVING systolic_value IS NOT NULL AND diastolic_value IS NOT NULL
)

SELECT *,
    -- Clinical context classification for downstream use
    CASE 
        WHEN is_abpm_bp_event THEN 'ABPM'
        WHEN is_home_bp_event THEN 'Home'
        ELSE 'Clinic'
    END AS measurement_context
FROM event_consolidation
```

**2. Data Quality Companion Pattern:**
```sql
-- Parallel DQ table captures filtered-out events for clinical governance
WITH problematic_events AS (
    -- Same base logic but WITHOUT quality filters
    -- Captures orphaned readings, out-of-range values, missing dates
    SELECT person_id, clinical_effective_date, 
           systolic_original, diastolic_original,
           
           -- DQ Flag definitions
           CASE WHEN systolic_original < 40 OR systolic_original > 350 
                THEN TRUE ELSE FALSE END AS is_sbp_out_of_range,
           CASE WHEN (systolic_original IS NOT NULL AND diastolic_original IS NULL) 
                  OR (systolic_original IS NULL AND diastolic_original IS NOT NULL) 
                THEN TRUE ELSE FALSE END AS is_orphaned_reading,
           -- ... other DQ flags
    FROM raw_event_aggregation
)

SELECT * FROM problematic_events 
WHERE is_sbp_out_of_range OR is_orphaned_reading OR /* other DQ issues */
```

#### Clinical Intelligence Integration

**3. Context-Specific Clinical Logic:**
```sql
-- NICE Guidelines: Different BP thresholds by measurement context
CASE 
    -- Severe hypertension (universal threshold)
    WHEN systolic_value >= 180 OR diastolic_value >= 120 
        THEN 'Severe HTN'
        
    -- Stage 2 Hypertension (context-specific)
    WHEN (measurement_context IN ('Home', 'ABPM') 
          AND (systolic_value >= 155 OR diastolic_value >= 95))
      OR (measurement_context = 'Clinic' 
          AND (systolic_value >= 160 OR diastolic_value >= 100))
        THEN 'Stage 2 HTN'
        
    -- Stage 1 Hypertension (context-specific)  
    WHEN (measurement_context IN ('Home', 'ABPM') 
          AND (systolic_value >= 135 OR diastolic_value >= 85))
      OR (measurement_context = 'Clinic' 
          AND (systolic_value >= 140 OR diastolic_value >= 90))
        THEN 'Stage 1 HTN'
        
    ELSE 'Normal / High Normal'
END AS clinical_stage
```

#### Benefits of Event-Based Pattern

**Clinical Advantages:**
- ✅ **Paired readings**: Systolic/diastolic values guaranteed to be from same clinical event
- ✅ **Context awareness**: Clinical interpretation varies by measurement setting (Home vs Clinic vs ABPM)
- ✅ **Quality assurance**: Orphaned or problematic readings tracked separately for clinical review
- ✅ **Clinical guidelines compliance**: Context-specific thresholds (NICE, AHA, etc.) properly implemented

**Analytical Advantages:**
- ✅ **Simplified downstream queries**: No complex joins needed to pair measurements
- ✅ **Event-based analysis**: Trends, patterns, and changes easier to track over time
- ✅ **Performance optimization**: Fewer rows, better clustering, faster aggregations
- ✅ **Rich metadata**: Full traceability to source observations for audit purposes

**Data Quality Advantages:**
- ✅ **Proactive issue detection**: DQ companion table identifies problems before clinical use
- ✅ **Clinical governance**: Problematic data flagged for clinical review processes
- ✅ **Validation alignment**: DQ logic mirrors main consolidation for consistency
- ✅ **Audit compliance**: Complete visibility into data transformations and quality issues

#### When to Apply This Pattern

**⚠️ IMPORTANT: Blood Pressure is Uniquely Complex**

The full event-based consolidation pattern above is specifically designed for **blood pressure measurements**, which are uniquely complex due to:
- **Mandatory paired readings** (systolic/diastolic must be from same clinical event)
- **Context-critical interpretation** (Home/ABPM vs Clinic thresholds differ significantly)
- **Safety-critical accuracy** (hypertension staging affects clinical decisions)

**✅ Apply FULL pattern for:**
- **Blood pressure only** (systolic/diastolic pairs with measurement context)
- Other truly paired measurements requiring context-specific clinical interpretation

**✅ Apply SIMPLIFIED pattern for most other clinical measurements:**
- Single-value observations (HbA1c, cholesterol, BMI, etc.)
- Use standard intermediate → latest pattern without complex event consolidation
- Focus on good result presentation and clinical interpretation enhancement

**❌ Don't over-engineer:**
- Simple single-value measurements (weight, temperature, glucose)
- Measurements without clinical context variation
- Non-safety-critical observations
- Reference data or categorical observations

#### Implementation Checklist

- [ ] **Identify pairing requirements**: What values must be measured together?
- [ ] **Map clinical contexts**: How does measurement setting affect interpretation?
- [ ] **Define quality criteria**: What makes a valid clinical event?
- [ ] **Design DQ companion**: How will problematic data be tracked?
- [ ] **Implement clinical logic**: What guidelines need context-specific rules?
- [ ] **Plan traceability**: What metadata is needed for clinical audit?
- [ ] **Test edge cases**: Orphaned readings, missing dates, out-of-range values
- [ ] **Validate clinical output**: Do staging rules match clinical guidelines?

This pattern transforms basic observation data into **clinically intelligent, analytically powerful, and quality-assured measurement events** that support sophisticated population health analysis and clinical decision-making.

### Standard Clinical Measurement Enhancement Pattern

**Pattern**: Enhance single-value clinical measurements with better result presentation and clinical interpretation without complex event consolidation.

**Use Case**: Most clinical observations (HbA1c, cholesterol, BMI, laboratory results) that don't require paired readings but benefit from improved clinical interpretation and result formatting.

#### Implementation Pattern for Standard Measurements

**Focus Areas for Enhancement:**
1. **Result Presentation**: Clear value formatting with appropriate units and clinical context
2. **Clinical Interpretation**: Thresholds, target ranges, and clinical significance flags
3. **Type Classification**: Handle different measurement types/units (e.g., HbA1c IFCC vs DCCT)
4. **Quality Flags**: Basic range validation and clinical plausibility checks

**Example: Enhanced HbA1c Pattern**
```sql
-- Enhanced intermediate with clinical intelligence
WITH base_observations AS (
    SELECT 
        obs.person_id,
        obs.clinical_effective_date,
        obs.result_value,
        obs.result_unit_display,
        obs.mapped_concept_display AS concept_display
    FROM ({{ get_observations("'HBA1C_COD'") }}) obs
    WHERE obs.result_value IS NOT NULL
    AND obs.clinical_effective_date IS NOT NULL
),

enhanced_results AS (
    SELECT *,
        -- Enhanced type detection
        CASE 
            WHEN result_unit_display ILIKE '%mmol/mol%' OR result_value > 20 THEN TRUE
            ELSE FALSE 
        END AS is_ifcc,
        
        CASE 
            WHEN result_unit_display ILIKE '%\%%' OR result_value <= 20 THEN TRUE
            ELSE FALSE 
        END AS is_dcct,
        
        -- Clinical range validation
        CASE 
            WHEN (result_value > 20 AND result_value BETWEEN 20 AND 200) 
              OR (result_value <= 20 AND result_value BETWEEN 3 AND 20)
            THEN TRUE ELSE FALSE 
        END AS is_plausible_value,
        
        -- Clinical interpretation flags
        CASE 
            WHEN (is_ifcc AND result_value < 42) OR (is_dcct AND result_value < 6.0)
            THEN 'Normal'
            WHEN (is_ifcc AND result_value BETWEEN 42 AND 47) OR (is_dcct AND result_value BETWEEN 6.0 AND 6.4)
            THEN 'Prediabetes'
            WHEN (is_ifcc AND result_value >= 48) OR (is_dcct AND result_value >= 6.5)
            THEN 'Diabetes'
            ELSE 'Unknown'
        END AS clinical_interpretation
        
    FROM base_observations
    WHERE is_plausible_value = TRUE
)

SELECT 
    person_id,
    clinical_effective_date,
    result_value AS hba1c_value,
    result_unit_display,
    is_ifcc,
    is_dcct,
    clinical_interpretation,
    
    -- Enhanced clinical context
    CASE 
        WHEN clinical_interpretation = 'Diabetes' THEN TRUE 
        ELSE FALSE 
    END AS indicates_diabetes,
    
    -- Metadata for traceability
    concept_display,
    observation_id
FROM enhanced_results
ORDER BY person_id, clinical_effective_date DESC
```

**Key Principles for Standard Measurements:**
- ✅ **Enhanced presentation**: Clear value formatting and clinical context
- ✅ **Type handling**: Proper detection of measurement units/types using both cluster IDs and actual unit display
- ✅ **Clinical intelligence**: Threshold interpretation and clinical flags
- ✅ **Quality validation**: Range checks and plausibility validation
- ✅ **Proper unit capture**: Use enhanced `get_observations()` macro with `result_unit_display` from source
- ❌ **Avoid over-complexity**: No event consolidation or complex context logic unless truly needed

#### Enhanced get_observations() Macro

The `get_observations()` macro has been enhanced to properly capture result unit display text from the source system:

```sql
-- Enhanced macro now includes result_unit_display
LEFT JOIN {{ ref('stg_olids_term_concept') }} unit_con
    ON o.result_value_unit_concept_id = unit_con.id

-- Returns result_unit_display field with actual units (e.g., 'mmol/mol', '%', 'mg/L')
```

**Benefits:**
- ✅ **Authentic units**: Uses actual unit text from source system instead of hardcoded values  
- ✅ **Enhanced type detection**: Can detect measurement types using both cluster ID and unit display
- ✅ **Better result presentation**: Combines value + unit for clear clinical display
- ✅ **Legacy compatibility**: Matches legacy approach for unit handling

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

### ✅ **LEGACY MIGRATION COMPLETE**

**Final Status Summary:**
- ✅ **Staging Layer**: Complete for all source systems
- ✅ **Core Dimensions**: Complete for person/patient/practice relationships  
- ✅ **All Intermediate Tables**: 100% complete - all clinical observations, laboratory results, risk assessments, medications
- ✅ **All QOF Register Patterns**: 100% complete - 25 total register fact tables implemented across all patterns
- ✅ **Clinical Quality & Status Tables**: 100% complete - diabetes care processes, BP control, clinical safety measures
- 🎯 **MAJOR MILESTONE**: **ALL LEGACY DYNAMIC TABLES MIGRATED!** 

**Project Status**: **175+ models with 100% PASS rate** - Legacy migration phase complete, now focusing on analytics enhancement.

---

## 🚀 **Analytics-Ready Enhancement Checklist**

**Goal**: Enhance intermediate models to match legacy analytics-ready patterns with better business logic, categorisation, and user-friendly field presentation.

### **Analytics-Ready Enhancement Patterns**

#### **🎯 Core Enhancement Features:**
1. **Legacy Structure Alignment**: `sk_patient_id`, `result_unit_display`, proper field naming
2. **Business Logic Flags**: Pre-calculated boolean indicators (`is_current_smoker`, `is_high_risk`, etc.)
3. **Clinical Categorisation**: User-friendly status fields (`smoking_status`, `risk_category`, etc.)
4. **Derived Analytics Fields**: Pre-calculated metrics, interpretations, clinical significance
5. **Enhanced Reference Integration**: Rich categorisation from reference tables
6. **Comprehensive Traceability**: Arrays of contributing codes, observation IDs, metadata

### **Phase 1: Core Measurement Tables** ✅ **Priority: HIGH**

#### 1.1 Laboratory Results ✅ **COMPLETE**
- [x] `int_urine_acr_all.sql` ✅ **ENHANCED** (sk_patient_id, result_unit_display, clinical categorisation)
- [x] `int_egfr_all.sql` ✅ **ENHANCED** (sk_patient_id, result_unit_display, CKD staging)  
- [x] `int_creatinine_all.sql` ✅ **ENHANCED** (sk_patient_id, result_unit_display, clinical ranges)
- [x] `int_cholesterol_all.sql` ✅ **ENHANCED** (sk_patient_id, result_unit_display, risk categories)

#### 1.2 Physical Measurements ✅ **COMPLETE**  
- [x] `int_bmi_all.sql` ✅ **ENHANCED** (sk_patient_id, result_unit_display, obesity classification)
- [x] `int_waist_circumference_all.sql` ✅ **ENHANCED** (sk_patient_id, result_unit_display, risk categorisation)

#### 1.3 Risk Scores ✅ **COMPLETE**
- [x] `int_qrisk_all.sql` ✅ **ENHANCED** (sk_patient_id, result_unit_display, CVD risk categories)  
- [x] `int_hba1c_all.sql` ✅ **ENHANCED** (advanced type detection, formatted display, clinical categories)

#### 1.4 Blood Pressure ✅ **COMPLETE** 
- [x] `int_blood_pressure_all.sql` ✅ **ENHANCED** (event-based consolidation, clinical context awareness)

### **Phase 2: Clinical Assessment Tables** ⚠️ **Priority: HIGH** ⚠️ **IN PROGRESS**

#### 2.1 Lifestyle & Behavioural Assessments
- [x] `int_smoking_status_all.sql` → **ENHANCED** ✅ **COMPLETE** (sk_patient_id, code_description, analytics flags, risk categorisation)
- [x] `int_ethnicity_all.sql` → **VALIDATED** ✅ **COMPLETE** (already has rich categorisation and legacy alignment)

#### 2.2 Clinical Examinations  
- [x] `int_spirometry_all.sql` → **ENHANCED** ✅ **COMPLETE** (sk_patient_id, code_description, enhanced COPD analytics, staging)
- [x] `int_foot_examination_all.sql` → **ENHANCED** ✅ **COMPLETE** (sk_patient_id, examination type classification, diabetes foot risk categories)
- [ ] `int_retinal_screening_all.sql` → **ENHANCE** ⚠️ **NEEDS**: screening result categorisation, diabetes eye risk
- [ ] `int_dxa_scans_all.sql` → **ENHANCE** ⚠️ **NEEDS**: bone density categorisation, fracture risk assessment

#### 2.3 Health Checks & Assessments
- [ ] `int_nhs_health_check_all.sql` → **ENHANCE** ⚠️ **NEEDS**: health check type classification, eligibility status
- [ ] `int_pregnancy_status_all.sql` → **VALIDATE** (check against legacy patterns)

### **Phase 3: Diagnosis Models Enhancement** ⚠️ **Priority: MEDIUM**

#### 3.1 Enhanced Diagnosis Categorisation
- [ ] `int_diabetes_diagnoses_all.sql` → **ENHANCE** ⚠️ **NEEDS**: diabetes type hierarchy, onset classification
- [ ] `int_heart_failure_diagnoses_all.sql` → **ENHANCE** ⚠️ **NEEDS**: HF type classification, severity staging
- [ ] `int_copd_diagnoses_all.sql` → **ENHANCE** ⚠️ **NEEDS**: COPD severity staging, spirometry integration
- [ ] `int_depression_diagnoses_all.sql` → **ENHANCE** ⚠️ **NEEDS**: depression severity, treatment status

#### 3.2 Cardiovascular Diagnoses
- [ ] `int_hypertension_diagnoses_all.sql` → **ENHANCE** ⚠️ **NEEDS**: HTN staging, resistant HTN flags
- [ ] `int_atrial_fibrillation_diagnoses_all.sql` → **ENHANCE** ⚠️ **NEEDS**: AF type classification, stroke risk
- [ ] `int_ckd_diagnoses_all.sql` → **ENHANCE** ⚠️ **NEEDS**: CKD stage integration, progression tracking

### **Phase 4: Medication Tables Enhancement** ⚠️ **Priority: LOW**

#### 4.1 Medication Analytics Enhancement
All medication intermediate tables could benefit from:
- [ ] Enhanced medication categorisation (brand vs generic, dosage classification)
- [ ] Therapeutic class hierarchies  
- [ ] Duration and adherence indicators
- [ ] Drug interaction flags

**Examples:**
- [ ] `int_diabetes_medications_all.sql` → Enhanced insulin vs non-insulin classification
- [ ] `int_statin_medications_all.sql` → Statin intensity classification
- [ ] `int_antidepressant_medications_all.sql` → Antidepressant class categorisation

### **Enhancement Implementation Checklist**

For each model enhancement, verify:

#### **✅ Legacy Structure Alignment**
- [ ] `sk_patient_id` field included (with appropriate NULL handling for dummy data)
- [ ] `result_unit_display` field for measurement tables
- [ ] Field naming matches legacy conventions
- [ ] Proper clustering and indexing

#### **✅ Business Logic Enhancement**  
- [ ] Boolean flags for key clinical indicators
- [ ] Risk categorisation fields
- [ ] Clinical interpretation fields
- [ ] Validation and quality flags

#### **✅ Analytics-Ready Features**
- [ ] Pre-calculated derived fields
- [ ] User-friendly categorisation  
- [ ] Clinical significance indicators
- [ ] Reference data integration

#### **✅ Documentation & Testing**
- [ ] YAML updated with new fields
- [ ] Tests appropriate for enhanced structure
- [ ] Clinical logic documented
- [ ] Legacy comparison validated

#### **✅ Traceability & Metadata**
- [ ] Source observation IDs preserved
- [ ] Contributing concept codes tracked
- [ ] Data lineage clear
- [ ] Audit-ready metadata

### **Next Steps**

1. **🎯 Start with Phase 2.1**: Smoking status and ethnicity validation
2. **Then Phase 2.2**: Clinical examinations (spirometry, foot exams, retinal screening)
3. **Then Phase 2.3**: Health checks and assessments
4. **Finally Phases 3-4**: Diagnosis and medication enhancements

**Success Criteria**: Each enhanced model should be more user-friendly, analytics-ready, and provide richer clinical context than the basic intermediate pattern while maintaining 100% data integrity.

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

## 🔧 **CRITICAL FIX: Duplicate Observations Issue Resolved**

### **Problem Identified**
User reported duplicate observations in intermediate tables (e.g., `int_bmi_all`, `int_egfr_all`) despite source data not containing duplicates.

### **Root Cause**
The `get_observations()` macro was creating duplicates due to **many-to-many relationships** in concept mapping:
- Multiple `concept_code` values mapping to the same `source_code_id` in `stg_codesets_mapped_concepts`
- Same clinical concept having multiple display terms/synonyms
- This is expected in healthcare data (multiple concept displays for same SNOMED concept)

### **Solution Applied**
Restructured the `get_observations()` macro to start with observations and use window functions to select the best concept match per observation:

```sql
-- Enhanced macro now starts with observations and ranks concept matches
SELECT o.id AS observation_id, ...
FROM {{ ref('stg_olids_observation') }} o
JOIN (
    -- Get the best concept match per observation
    SELECT mc.source_code_id, ...,
           ROW_NUMBER() OVER (
               PARTITION BY mc.source_code_id 
               ORDER BY mc.code_description, mc.concept_code
           ) AS concept_rank
    FROM {{ ref('stg_codesets_mapped_concepts') }} mc
    WHERE cc.cluster_id IN ({{ cluster_ids }})
) best_match ON o.observation_core_concept_id = best_match.source_code_id
    AND best_match.concept_rank = 1
```

### **Benefits of This Approach**
- ✅ **Observation-centric design** - starts with observations, avoids cartesian product
- ✅ **Deterministic concept selection** - always picks the same concept display per observation
- ✅ **Better performance** - no GROUP BY needed, cleaner execution plan
- ✅ **More maintainable** - clearer logic flow from observations to concepts

### **Impact**
- ✅ **All intermediate tables now deduplicated**
- ✅ **Data integrity maintained** - correct observations preserved
- ✅ **Performance improvement** - fewer duplicate rows, cleaner joins
- ✅ **Downstream models now accurate** - no artificially inflated counts

This fix applies to **ALL models using `get_observations()`** including measurement, diagnosis, and examination models.

## 🚨 **CRITICAL ARCHITECTURAL FIX: Observation vs Person Grain Issue**

### **Problem Identified**
Many intermediate diagnosis tables were incorrectly mixing **observation-level data** with **person-level aggregates**, causing:
- ✅ **Multiple rows per person in fact tables** (violating fact table grain)
- ✅ **Duplicated aggregate data** across every observation row
- ✅ **Performance issues** from unnecessary data duplication
- ✅ **Confusing data model** with unclear grain

### **Root Cause**
**Mixed Grain Architecture**: Intermediate tables like `int_diabetes_diagnoses_all` were:
1. Starting with observations (one row per observation)
2. Calculating person-level aggregates
3. Joining aggregates back to **every observation row**
4. Fact tables then had multiple rows per person

### **Solution: Clean Separation of Concerns**

#### **✅ NEW ARCHITECTURE PATTERN:**

**Intermediate Tables**: Pure observation-level data
```sql
-- CORRECT: One row per observation, no person-level aggregates
SELECT
    obs.observation_id,
    obs.person_id,
    obs.clinical_effective_date,
    obs.concept_code,
    obs.concept_display,
    obs.source_cluster_id,
    
    -- Individual observation flags only
    CASE WHEN obs.cluster_id = 'DM_COD' THEN TRUE ELSE FALSE END AS is_general_diabetes_code,
    -- ... other observation-level flags
    
FROM ({{ get_observations("'DM_COD', 'DMRES_COD'") }}) obs
-- NO person-level aggregation, NO JOINs back to observations
```

**Fact Tables**: Handle all person-level aggregation
```sql
-- CORRECT: Fact table calculates person-level aggregates
WITH person_aggregates AS (
    SELECT
        person_id,
        MIN(CASE WHEN is_general_diabetes_code THEN clinical_effective_date END) AS earliest_diabetes_date,
        MAX(CASE WHEN is_general_diabetes_code THEN clinical_effective_date END) AS latest_diabetes_date,
        -- ... other person-level aggregations
    FROM {{ ref('int_diabetes_diagnoses_all') }}
    GROUP BY person_id  -- ✅ CRITICAL: GROUP BY ensures one row per person
)

SELECT person_id, earliest_diabetes_date, latest_diabetes_date, ...
FROM person_aggregates
-- ✅ Result: One row per person in fact table
```

### **Implementation Checklist**

#### **🎯 Phase 1: Fix All Diagnosis Intermediate Tables**

**Pattern to Look For:**
```sql
-- ❌ INCORRECT PATTERN (causes multiple rows per person in facts)
WITH person_aggregates AS (
    SELECT person_id, MIN(date) as earliest_date, ...
    FROM base_observations 
    GROUP BY person_id
)
SELECT 
    bo.observation_id,
    bo.person_id,
    pa.earliest_date,  -- ❌ Person aggregate attached to every observation
    ...
FROM base_observations bo
LEFT JOIN person_aggregates pa ON bo.person_id = pa.person_id
-- ❌ Results in multiple rows per person with duplicated aggregates
```

**Fix Pattern:**
```sql
-- ✅ CORRECT PATTERN (observation-level only)
SELECT
    obs.observation_id,
    obs.person_id,
    obs.clinical_effective_date,
    -- Only observation-level fields, NO person aggregates
FROM ({{ get_observations(...) }}) obs
-- ✅ Clean observation-level data
```

#### **Models to Fix - Intermediate Layer:**

- [ ] `int_asthma_diagnoses_all.sql` → **FIX NEEDED** ⚠️
- [ ] `int_atrial_fibrillation_diagnoses_all.sql` → **FIX NEEDED** ⚠️  
- [ ] `int_cancer_diagnoses_all.sql` → **FIX NEEDED** ⚠️
- [ ] `int_chd_diagnoses_all.sql` → **FIX NEEDED** ⚠️
- [ ] `int_ckd_diagnoses_all.sql` → **FIX NEEDED** ⚠️
- [ ] `int_copd_diagnoses_all.sql` → **FIX NEEDED** ⚠️
- [ ] `int_cyp_asthma_diagnoses_all.sql` → **FIX NEEDED** ⚠️
- [ ] `int_dementia_diagnoses_all.sql` → **FIX NEEDED** ⚠️
- [x] `int_diabetes_diagnoses_all.sql` → **FIXED** ✅
- [ ] `int_epilepsy_diagnoses_all.sql` → **FIX NEEDED** ⚠️
- [ ] `int_familial_hypercholesterolaemia_diagnoses_all.sql` → **FIX NEEDED** ⚠️
- [ ] `int_gestational_diabetes_diagnoses_all.sql` → **FIX NEEDED** ⚠️
- [ ] `int_heart_failure_diagnoses_all.sql` → **FIX NEEDED** ⚠️
- [x] `int_hypertension_diagnoses_all.sql` → **FIXED** ✅
- [ ] `int_learning_disability_diagnoses_all.sql` → **FIX NEEDED** ⚠️
- [ ] `int_nafld_diagnoses_all.sql` → **FIX NEEDED** ⚠️
- [ ] `int_ndh_diagnoses_all.sql` → **FIX NEEDED** ⚠️
- [ ] `int_obesity_diagnoses_all.sql` → **FIX NEEDED** ⚠️
- [ ] `int_osteoporosis_diagnoses_all.sql` → **FIX NEEDED** ⚠️
- [ ] `int_pad_diagnoses_all.sql` → **FIX NEEDED** ⚠️
- [ ] `int_palliative_care_diagnoses_all.sql` → **FIX NEEDED** ⚠️
- [ ] `int_rheumatoid_arthritis_diagnoses_all.sql` → **FIX NEEDED** ⚠️
- [ ] `int_smi_diagnoses_all.sql` → **FIX NEEDED** ⚠️
- [ ] `int_stroke_tia_diagnoses_all.sql` → **FIX NEEDED** ⚠️

#### **🎯 Phase 2: Fix All Register Fact Tables**

**Pattern to Look For:**
```sql
-- ❌ INCORRECT: Using intermediate table without GROUP BY
SELECT person_id, earliest_diabetes_date, ...
FROM {{ ref('int_diabetes_diagnoses_all') }}
-- ❌ Results in multiple rows per person because intermediate has observation grain
```

**Fix Pattern:**
```sql
-- ✅ CORRECT: Proper person-level aggregation in fact table
WITH person_aggregates AS (
    SELECT 
        person_id,
        MIN(CASE WHEN condition THEN clinical_effective_date END) AS earliest_date,
        MAX(CASE WHEN condition THEN clinical_effective_date END) AS latest_date,
        ...
    FROM {{ ref('int_condition_diagnoses_all') }}
    GROUP BY person_id  -- ✅ CRITICAL
)
SELECT person_id, earliest_date, latest_date, ...
FROM person_aggregates
-- ✅ One row per person guaranteed
```

#### **Models to Fix - Fact Layer:**

- [ ] `fct_person_asthma_register.sql` → **FIX NEEDED** ⚠️
- [ ] `fct_person_atrial_fibrillation_register.sql` → **FIX NEEDED** ⚠️
- [ ] `fct_person_cancer_register.sql` → **FIX NEEDED** ⚠️
- [ ] `fct_person_chd_register.sql` → **FIX NEEDED** ⚠️
- [ ] `fct_person_ckd_register.sql` → **FIX NEEDED** ⚠️
- [ ] `fct_person_copd_register.sql` → **FIX NEEDED** ⚠️
- [ ] `fct_person_cyp_asthma_register.sql` → **FIX NEEDED** ⚠️
- [ ] `fct_person_dementia_register.sql` → **FIX NEEDED** ⚠️
- [ ] `fct_person_depression_register.sql` → **FIX NEEDED** ⚠️
- [x] `fct_person_diabetes_register.sql` → **FIXED** ✅
- [ ] `fct_person_epilepsy_register.sql` → **FIX NEEDED** ⚠️
- [ ] `fct_person_familial_hypercholesterolaemia_register.sql` → **FIX NEEDED** ⚠️
- [ ] `fct_person_gestational_diabetes_register.sql` → **FIX NEEDED** ⚠️
- [ ] `fct_person_heart_failure_register.sql` → **FIX NEEDED** ⚠️
- [ ] `fct_person_hypertension_register.sql` → **FIX NEEDED** ⚠️
- [ ] `fct_person_learning_disability_register.sql` → **FIX NEEDED** ⚠️
- [ ] `fct_person_nafld_register.sql` → **FIX NEEDED** ⚠️
- [ ] `fct_person_ndh_register.sql` → **FIX NEEDED** ⚠️
- [ ] `fct_person_obesity_register.sql` → **FIX NEEDED** ⚠️
- [ ] `fct_person_osteoporosis_register.sql` → **FIX NEEDED** ⚠️
- [ ] `fct_person_pad_register.sql` → **FIX NEEDED** ⚠️
- [ ] `fct_person_palliative_care_register.sql` → **FIX NEEDED** ⚠️
- [ ] `fct_person_rheumatoid_arthritis_register.sql` → **FIX NEEDED** ⚠️
- [ ] `fct_person_smi_register.sql` → **FIX NEEDED** ⚠️
- [ ] `fct_person_stroke_tia_register.sql` → **FIX NEEDED** ⚠️

#### **🎯 Phase 3: Fix Care Process Tables**

**Pattern to Check:**
- All care process tables should use register fact tables, not intermediate tables
- Each care process table should be exactly one row per person

#### **Models to Verify:**
- [x] `fct_person_diabetes_8_care_processes.sql` → **FIXED** ✅
- [ ] `fct_person_diabetes_9_care_processes.sql` → **VERIFY** ⚠️
- [ ] All other condition-specific care process tables → **VERIFY** ⚠️

### **Quality Assurance Checklist**

For each fixed model, verify:

#### **✅ Intermediate Tables:**
- [ ] **Pure observation grain**: One row per observation/diagnosis
- [ ] **No person-level aggregates**: No MIN/MAX/COUNT across observations
- [ ] **No JOINs back to observations**: Clean, simple SELECT from get_observations()
- [ ] **Only observation-level flags**: is_type1_code, is_resolved_code, etc.

#### **✅ Fact Tables:**
- [ ] **One row per person**: Final result set has unique person_id
- [ ] **Proper aggregation**: Uses GROUP BY person_id in CTEs
- [ ] **Register logic applied**: Age restrictions, resolution status, etc.
- [ ] **Clean dependencies**: Only uses dimensions and intermediate tables

#### **✅ Testing:**
- [ ] **Row count validation**: Fact table row count ≤ person count in intermediate
- [ ] **Uniqueness test**: No duplicate person_id in fact tables
- [ ] **Logical validation**: Register inclusion criteria work correctly

### **Implementation Priority**

1. **HIGH PRIORITY**: Common conditions (diabetes ✅, hypertension, asthma, COPD)
2. **MEDIUM PRIORITY**: Cardiovascular conditions (CHD, AF, stroke)
3. **LOW PRIORITY**: Specialty conditions (learning disability, NAFLD, etc.)

### **Expected Benefits**

After completing this fix:
- ✅ **All fact tables**: Exactly one row per person
- ✅ **Performance improvement**: No duplicate aggregates, cleaner queries
- ✅ **Data integrity**: Clear grain separation, no mixed responsibilities
- ✅ **Maintainability**: Easier to understand and modify logic
- ✅ **Register accuracy**: Proper QOF register logic without data grain confusion
