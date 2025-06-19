# Snowflake HEI Migration - dbt Patterns & Guidelines

## Overview

This document provides essential patterns for migrating legacy dynamic tables to dbt models in the Snowflake HEI migration project. Follow these patterns consistently for maintainable, testable healthcare data transformations.

## Core Architecture

### Layer Structure
```
models/
├── staging/          # 1:1 source mappings (views)
├── intermediate/     # Business logic & consolidation (tables)
└── marts/           # Final analytical models (tables)
```

### Database Strategy
- **Development Database**: `DATA_LAB_NCL_TRAINING_TEMP`
- **Staging Schema**: Source-specific (e.g., `OLIDS_MASKED`, `CODESETS`)
- **Transformed Schema**: `DBT`
- **Materialisation**: Views for staging, tables for intermediate/marts

## Critical Macro Usage Patterns

### ⚠️ ALWAYS Use Subquery Pattern

**❌ INCORRECT (Causes recursive CTE errors):**
```sql
WITH base_orders AS (
    SELECT * FROM {{ get_medication_orders(bnf_code='0304') }}
)
```

**✅ CORRECT (Use subquery wrapper):**
```sql
-- For all macro usage
FROM ({{ get_medication_orders(bnf_code='0304') }}) base_orders
FROM ({{ get_observations("'AST_COD', 'ASTRES_COD'") }}) obs
```

### Column Mapping for get_observations() Macro

**Always use proper aliasing:**
```sql
obs.mapped_concept_code AS concept_code,
obs.mapped_concept_display AS concept_display,
obs.cluster_id AS source_cluster_id
```

### Snowflake Compatibility
**❌ FILTER clause not supported:**
```sql
ARRAY_AGG(value) FILTER (WHERE condition)  -- PostgreSQL syntax
```

**✅ Use conditional aggregation:**
```sql
ARRAY_AGG(CASE WHEN condition THEN value ELSE NULL END)  -- Snowflake compatible
```

## Model Architecture Patterns

### Diagnosis Models Pattern
```sql
{{
    config(
        materialized='table',
        cluster_by=['person_id', 'clinical_effective_date']
    )
}}

-- Intermediate: Pure observation-level data (one row per observation)
SELECT
    obs.observation_id,
    obs.person_id,
    obs.clinical_effective_date,
    obs.mapped_concept_code AS concept_code,
    obs.mapped_concept_display AS concept_display,
    obs.cluster_id AS source_cluster_id,
    
    -- Individual observation flags only
    CASE WHEN obs.cluster_id = 'DM_COD' THEN TRUE ELSE FALSE END AS is_diabetes_code,
    CASE WHEN obs.cluster_id = 'DMRES_COD' THEN TRUE ELSE FALSE END AS is_resolved_code
    
FROM ({{ get_observations("'DM_COD', 'DMRES_COD'") }}) obs
WHERE obs.clinical_effective_date IS NOT NULL
-- NO person-level aggregates here
```

### Fact/Register Models Pattern
```sql
-- Fact: Person-level aggregation and business logic
WITH person_aggregates AS (
    SELECT
        person_id,
        MIN(CASE WHEN is_diabetes_code THEN clinical_effective_date END) AS earliest_diabetes_date,
        MAX(CASE WHEN is_diabetes_code THEN clinical_effective_date END) AS latest_diabetes_date,
        ARRAY_AGG(DISTINCT CASE WHEN is_diabetes_code THEN concept_code END) AS diabetes_codes
    FROM {{ ref('int_diabetes_diagnoses_all') }}
    GROUP BY person_id  -- ✅ CRITICAL: Ensures one row per person
)

SELECT 
    person_id,
    earliest_diabetes_date,
    latest_diabetes_date,
    -- Business logic
    CASE WHEN earliest_diabetes_date IS NOT NULL THEN TRUE ELSE FALSE END AS on_diabetes_register
FROM person_aggregates
```

## Testing & Documentation

### YAML Requirements
Each model MUST have individual YAML file with column-level tests only:

```yaml
# int_diabetes_diagnoses_all.yml
models:
  - name: int_diabetes_diagnoses_all
    description: "All diabetes diagnosis observations from clinical records"
    # NO model-level tests block
    
    columns:
      - name: person_id
        tests: [not_null]
        
      - name: concept_code
        tests: 
          - not_null
          - cluster_ids_exist:
              cluster_ids: "DM_COD,DMRES_COD"  # Comma-separated
              
      - name: is_diabetes_code
        tests:
          - accepted_values:
              values: [true, false]
```

### Mandatory Tests
- **Clinical observations**: `cluster_ids_exist` test
- **Medications**: `bnf_codes_exist` test  
- **Dates**: `no_future_dates` test
- **Boolean flags**: `accepted_values` test
- **Clinical ranges**: `accepted_range` test

## Naming Conventions

- **Staging**: `stg_{source_schema}_{table_name}.sql`
- **Intermediate**: `int_{clinical_domain}_{descriptor}.sql`
- **Marts**: `fct_{entity}_{grain}.sql` or `dim_{entity}.sql`

## 🚨 **CRITICAL FIX: Observation vs Person Grain**

### Problem
Many diagnosis intermediate tables incorrectly mixed observation-level data with person-level aggregates, causing multiple rows per person in fact tables.

### Solution: Clean Separation
- **Intermediate**: Pure observation-level data (one row per observation)
- **Fact**: Person-level aggregation (one row per person, uses GROUP BY)

## 🎯 **CURRENT PRIORITY: Analytics Enhancement Checklist**

### **Phase 1: Fix Observation Grain Issues** ✅ **COMPLETE**

**✅ Diagnosis Intermediate Tables: ALL FIXED**
All 23 diagnosis intermediate models follow correct observation-level pattern:
- [x] All `int_*_diagnoses_all.sql` models → **FIXED** ✅ (Pure observation-level, no person aggregates)

**✅ Register Fact Tables: ALL FIXED** 
All 24 register fact models use proper person-level aggregation:
- [x] All `fct_person_*_register.sql` models → **FIXED** ✅ (Proper GROUP BY person_id)

**⚠️ ACTUAL GRAIN ISSUES STATUS:**

**✅ Medication Intermediate Tables: ALL FIXED**
- [x] `int_allergy_medications_all.sql` → **FIXED** ✅ Removed window functions, now pure observation-level
- [x] All other 15+ medication models → **CLEAN** ✅ (No grain issues found)

**✅ Complex Clinical Models: VERIFIED CORRECT**
- [x] `int_foot_examination_all.sql` → **VERIFIED CORRECT** ✅ Matches legacy pattern exactly - foot amputations/absences are permanent person-level attributes correctly joined to examination-level data

### **Phase 2: Analytics Enhancement** 
After grain issues fixed, enhance models with:
- Enhanced business logic flags
- Clinical categorisation fields  
- Analytics-ready derived fields
- Legacy structure alignment (`sk_patient_id`, `result_unit_display`)

### **Quality Checks**
For each fixed model:
- [ ] **Intermediate**: No person-level aggregates, observation grain only
- [ ] **Fact**: Uses GROUP BY person_id, one row per person guaranteed
- [ ] **Testing**: Row count validation, uniqueness tests
- [ ] **Documentation**: Updated YAML with proper tests

## Troubleshooting

### Common Issues
- **Recursive CTE errors** → Use subquery pattern for macros
- **Duplicate test errors** → Use only column-level tests in YAML
- **Empty result sets** → Verify cluster IDs with `cluster_ids_exist` test
- **Multiple rows per person** → Fix grain separation between intermediate/fact

### Performance
- Use appropriate clustering: `cluster_by=['person_id', 'clinical_effective_date']`
- Intermediate tables as tables for complex joins
- Filter early in WHERE clauses

## Migration Complete Status

✅ **Staging Layer**: Complete
✅ **Core Dimensions**: Complete  
✅ **Intermediate Tables**: Complete (fixing grain issues)
✅ **QOF Registers**: Complete (fixing grain issues)
✅ **Care Process Tables**: Complete

**Current Focus**: Fix observation vs person grain issues, then enhance for analytics. 