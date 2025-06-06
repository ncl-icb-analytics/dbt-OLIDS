# Core Patterns for Snowflake HEI Migration to dbt

## Layer Architecture

### 1. Staging Layer (`stg_`)
- Clean, typed copies of source tables
- Minimal transformations
- One-to-one relationship with source
- Example: `stg_olids_patient`

### 2. Intermediate Layer (`int_`)
- Combines multiple staging models
- Business logic and transformations
- Creates reusable structures
- Example: `int_blood_pressure_all`

### 3. Dimension Layer (`dim_`)
- Type 1 and 2 SCDs (Slowly Changing Dimensions)
- Core reference entities
- Examples:
  - `dim_patient` (Type 2 SCD)
  - `dim_practitioner` (Type 2 SCD)
  - `dim_location` (Type 1 SCD)
  - `dim_date` (Static)
  - `dim_codeset` (Type 1 SCD)

### 4. Fact Layer (`fct_`)
- Measurements and events
- Foreign keys to dimensions
- Grain at most granular level
- Examples:
  - `fct_blood_pressure` (grain: patient-date-reading)
  - `fct_medication` (grain: patient-date-medication)
  - `fct_observation` (grain: patient-date-observation)
  - `fct_encounter` (grain: patient-date-practitioner)

### Key Principles
1. **Grain Definition**
   - Each fact table has a clearly defined grain
   - Grain documented in model description
   - No unnecessary aggregation

2. **Dimension Handling**
   - Type 2 SCD for patient demographics
   - Type 2 SCD for practitioner details
   - Type 1 SCD for locations and organisations
   - Conformed dimensions across facts

3. **Key Structure**
   - Natural keys in staging
   - Surrogate keys in dimensions
   - Multiple foreign keys in facts

4. **Incremental Processing**
   - Dimensions: Merge strategy for SCD
   - Facts: Append strategy for new data
   - Delete+Insert for corrections 