# Flu Eligibility Rules - Simplified Implementation

## Overview

This document describes the simplified approach to flu vaccination eligibility rules. The new system replaces complex macros with clear, understandable SQL models that are easy to maintain and debug.

## Key Improvements

### 1. **Clear Terminology**
- `rule_group_id` → `eligibility_reason` (what makes them eligible)
- `cluster_id` → `code_set_name` (clearer naming for clinical codes)  
- `date_qualifier` → `timing_rule` (when the code matters)
- Descriptive rule names instead of generic types

### 2. **Single Configuration Point**
All campaign dates and parameters are defined in one place:
```sql
-- Usage: {{ flu_campaign_config('flu_2024_25') }}
-- Returns all campaign dates, lookback periods, age ranges, etc.
```

### 3. **Individual Rule Models**
All 19 flu eligibility rules now have clear, individual models:

**Age-Based Rules (3):**
- `int_flu_over_65.sql` - Simple age threshold (65+)
- `int_flu_children_preschool.sql` - Campaign-specific birth date range (typically 2-3 years)
- `int_flu_children_school_age.sql` - Campaign-specific birth date range (typically 4-16 years)

**Simple Clinical Rules (7):**
- `int_flu_chronic_heart_disease.sql` - CHD diagnosis
- `int_flu_chronic_liver_disease.sql` - CLD diagnosis  
- `int_flu_chronic_neurological_disease.sql` - CNS diagnosis
- `int_flu_asplenia.sql` - Asplenia diagnosis
- `int_flu_learning_disability.sql` - Learning disability
- `int_flu_household_immunocompromised.sql` - Household contact
- `int_flu_asthma_admission.sql` - Asthma hospital admission

**Combination Rules (4):**
- `int_flu_active_asthma_management.sql` - Diagnosis + (medication OR admission)
- `int_flu_immunosuppression.sql` - Multiple immunosuppression sources
- `int_flu_health_social_care_worker.sql` - Multiple worker categories
- `int_flu_chronic_respiratory_disease.sql` - Uses asthma + respiratory codes

**Hierarchical Rules (4):**
- `int_flu_chronic_kidney_disease.sql` - CKD staging logic
- `int_flu_severe_obesity.sql` - BMI values + diagnosis codes
- `int_flu_pregnancy.sql` - Complex pregnancy/delivery logic
- `int_flu_homeless.sql` - Latest residential status
- `int_flu_long_term_residential_care.sql` - Latest residential status

**Exclusion Rules (2):**
- `int_flu_diabetes.sql` - Diabetes with resolution exclusion
- `int_flu_carer.sql` - Carer status with eligibility exclusions

**Vaccination Tracking (3):**
- `int_flu_vaccination_given.sql` - Flu vaccine administered
- `int_flu_vaccination_declined.sql` - Vaccination declined
- `int_flu_laiv_vaccination.sql` - LAIV (nasal spray) vaccine

### 4. **Direct Use of Core Macros**
Instead of wrapper macros, models use `get_observations()` and `get_medication_orders()` directly:
```sql
-- Clear and direct
FROM ({{ get_observations('AST_COD', 'UKHSA_FLU') }})

-- Instead of confusing wrapper
FROM ({{ get_flu_observations_for_rule_group(campaign_id, 'AST_GROUP') }})
```

## Rule Implementation Examples

### Simple Rule: Chronic Heart Disease
```sql
-- Step 1: Find people with CHD diagnosis
people_with_chd_diagnosis AS (
    SELECT 
        person_id,
        MIN(clinical_effective_date) AS first_chd_date
    FROM ({{ get_observations('CHD_COD', 'UKHSA_FLU') }})
    WHERE clinical_effective_date IS NOT NULL
        AND clinical_effective_date <= CURRENT_DATE
    GROUP BY person_id
),

-- Step 2: Apply age restrictions and format output
-- (Clear age logic: 6 months to under 65 years)
```

### Combination Rule: Active Asthma
```sql
-- Step 1: Must have asthma diagnosis
people_with_asthma_diagnosis AS (...)

-- Step 2: Must have recent medication OR admission
people_with_active_asthma_evidence AS (
    -- Recent prescriptions
    SELECT person_id, 'Recent prescription' AS evidence_type, date
    FROM people_with_recent_prescriptions
    UNION ALL
    -- Recent administrations  
    SELECT person_id, 'Recent medication', date
    FROM people_with_recent_medications
    UNION ALL
    -- Hospital admissions (any time)
    SELECT person_id, 'Hospital admission', date
    FROM people_with_admissions
)

-- Step 3: Combine diagnosis AND evidence requirements
-- (Clear business logic: diagnosis + (medication OR admission))
```

### Hierarchical Rule: CKD Staging
```sql
-- Step 1: Direct CKD diagnosis (always eligible)
people_with_ckd_diagnosis AS (...)

-- Step 2: Stage-based logic
people_eligible_via_ckd_stages AS (
    SELECT person_id, qualifying_date
    FROM people_with_severe_ckd severe
    LEFT JOIN people_with_any_stage_ckd any_stage
        ON severe.person_id = any_stage.person_id
    WHERE severe.latest_severe_stage_date >= 
          COALESCE(any_stage.latest_any_stage_date, severe.latest_severe_stage_date)
)

-- Step 3: Combine all eligibility paths
-- (Clear hierarchy: direct diagnosis OR recent severe stage)
```

### Exclusion Rule: Diabetes
```sql
-- Step 1: Always eligible - Addison's disease
people_with_addisons AS (...)

-- Step 2: Conditional eligibility - Active diabetes
people_with_active_diabetes AS (
    SELECT person_id, latest_diabetes_date
    FROM people_with_diabetes_codes diab
    LEFT JOIN people_with_diabetes_resolved_codes resolved
        ON diab.person_id = resolved.person_id
    WHERE resolved.latest_resolved_date IS NULL 
       OR diab.latest_diabetes_date > resolved.latest_resolved_date
)

-- Step 3: Combine eligibility paths
-- (Clear exclusion: diabetes unless more recently resolved)
```

## Business Rule Validation

Each rule model includes clear business logic validation:

### Asthma Rule Validation
- ✅ Requires asthma diagnosis (AST_COD)
- ✅ AND requires evidence of active management:
  - Recent medication (ASTRX_COD or ASTMED_COD) since lookback date, OR
  - Hospital admission (ASTADM_COD) any time in history
- ✅ Age restrictions: 6 months to under 65 years

### CKD Rule Validation  
- ✅ Direct CKD diagnosis (CKD_COD) always qualifies
- ✅ OR stage 3-5 code (CKD35_COD) more recent than any-stage (CKD15_COD)
- ✅ Age restrictions: 6 months to under 65 years

### Diabetes Rule Validation
- ✅ Addison's disease (ADDIS_COD) always qualifies
- ✅ OR diabetes (DIAB_COD) without more recent resolution (DMRES_COD)
- ✅ Age restrictions: 6 months to under 65 years

## Data Model Architecture

### Separation of Concerns

The new architecture cleanly separates two distinct business concepts:

**`fct_flu_eligibility`** - **WHO SHOULD** be vaccinated
- Contains people eligible for flu vaccination based on clinical/demographic criteria
- Used for planning, targeting, and invitation lists
- Includes age-based, clinical condition, combination, hierarchical, and exclusion rules

**`fct_flu_status`** - **WHO HAS BEEN** vaccinated  
- Contains vaccination outcomes and tracking information
- Used for monitoring uptake, coverage analysis, and reporting
- Includes vaccination given, declined, and LAIV administration

This separation provides:
- ✅ **Clear business logic**: Eligibility vs outcomes are distinct concepts
- ✅ **Better reporting**: Can analyse eligible vs vaccinated populations separately
- ✅ **Cleaner queries**: No need to filter vaccination tracking from eligibility data
- ✅ **Performance**: Smaller, focused tables for specific use cases

## File Structure

```
models/
├── intermediate/programme/flu/
│   ├── int_flu_over_65.sql                    # Age-based rule
│   ├── int_flu_children_preschool.sql         # Age-based rule (campaign-specific)
│   ├── int_flu_children_school_age.sql        # Age-based rule (campaign-specific)
│   ├── int_flu_chronic_heart_disease.sql      # Simple clinical rule
│   ├── int_flu_active_asthma_management.sql   # Combination rule
│   ├── int_flu_chronic_kidney_disease.sql     # Hierarchical rule
│   ├── int_flu_diabetes.sql                   # Exclusion rule
│   ├── int_flu_vaccination_given.sql          # Vaccination tracking
│   ├── int_flu_vaccination_declined.sql       # Vaccination tracking
│   └── int_flu_laiv_vaccination.sql           # Vaccination tracking
├── marts/programme/flu/
│   ├── fct_flu_eligibility.sql                # Eligibility fact table
│   ├── fct_flu_status.sql                     # Vaccination status fact table
│   └── fct_flu_eligibility_comparison.sql     # Simple comparison summary
└── macros/flu/
    └── flu_campaign_config.sql                # Multi-campaign configuration
```

## Migration Benefits

### ✅ **Clarity**
- Business logic is explicit in SQL, not hidden in macros
- Step-by-step comments explain what each CTE does
- Clear rule names describe eligibility criteria

### ✅ **Maintainability** 
- Each rule in its own model file
- Single configuration point for all dates
- Easy to modify individual rules without affecting others

### ✅ **Testability**
- Each intermediate model can be tested independently
- Clear input/output for each rule
- Business logic validation built into SQL

### ✅ **Performance**
- Simpler SQL generates more efficient execution plans
- No complex macro expansion at compile time
- Direct use of optimized core macros

### ✅ **Debuggability**
- Clear data lineage through dbt DAG
- Easy to run individual rule models for testing
- Explicit business logic instead of macro black boxes

## Adding New Rules

To add a new eligibility rule:

1. **Create the rule model**: `int_flu_[rule_name].sql`
2. **Use clear step-by-step logic**: Document each CTE's purpose
3. **Use campaign config**: Get dates from `flu_campaign_config()`
4. **Use core macros**: Call `get_observations()` and `get_medication_orders()` directly
5. **Add to fact model**: Union the new rule in `fct_flu_eligibility_simplified.sql`

## Multi-Campaign Support

### Running Models for Different Campaign Years

All flu models support multiple campaign years through the `flu_current_campaign` variable set in `dbt_project.yml`:

**To change campaign year:**
1. **Edit `dbt_project.yml`:**
```yaml
vars:
  flu_current_campaign: "flu_2023_24"     # Change this value
  flu_previous_campaign: "flu_2022_23"    # For comparison
```

2. **Run models normally:**
```bash
# No vars needed - uses dbt_project.yml setting
dbt run -s +fct_flu_eligibility+
dbt run -s +fct_flu_status+
```

**Available campaign values:**
- `"flu_2023_24"` - 2023-24 Campaign
- `"flu_2024_25"` - 2024-25 Campaign (default)
- `"flu_2025_26"` - 2025-26 Campaign

### Comparing Across Campaign Years

To compare eligibility across multiple years:

1. **Run models for each campaign year:**
```bash
# Edit dbt_project.yml: flu_current_campaign: "flu_2023_24"
dbt run -s +fct_flu_eligibility+

# Edit dbt_project.yml: flu_current_campaign: "flu_2024_25"  
dbt run -s +fct_flu_eligibility+
```

2. **Query results from different campaigns:**
```sql
-- Union results from different campaign years
SELECT * FROM fct_flu_eligibility 
WHERE campaign_id IN ('flu_2023_24', 'flu_2024_25')
ORDER BY campaign_id, person_id, rule_group_id
```

**Tip:** Use the `flu_previous_campaign` variable in comparison queries:
```sql
-- Compare current vs previous campaign (using dbt_project.yml variables)
SELECT * FROM fct_flu_eligibility 
WHERE campaign_id IN ('{{ var("flu_current_campaign") }}', '{{ var("flu_previous_campaign") }}')
```

### Available Campaign Years

| Campaign ID | Campaign Name | Child Preschool Ages | Child School Ages |
|-------------|----------------|---------------------|-------------------|
| `flu_2023_24` | 2023-24 Campaign | Born Sep 2019 - Aug 2021 | Born Sep 2007 - Aug 2019 |
| `flu_2024_25` | 2024-25 Campaign | Born Sep 2020 - Aug 2022 | Born Sep 2008 - Aug 2020 |
| `flu_2025_26` | 2025-26 Campaign | Born Sep 2021 - Aug 2023 | Born Sep 2009 - Aug 2021 |

### Age-Agnostic Model Names

Child models use standardised names that work across all campaign years:
- `int_flu_children_preschool` - Instead of age-specific "children_2_3"
- `int_flu_children_school_age` - Instead of age-specific "children_4_16"

This allows for:
- ✅ **Consistent comparisons** across campaign years
- ✅ **Flexible age ranges** that can change year-to-year
- ✅ **Clear business logic** that doesn't become outdated

## Configuration Management

All campaign-specific parameters are centralised in `flu_campaign_config.sql`:
- Campaign dates (start, reference, end)
- Medication lookback periods
- Child age group birth ranges (campaign-specific)
- Vaccination tracking dates

To add a new campaign year, add a new condition to the macro with campaign-specific dates.

## Testing Approach

Each rule model should be tested for:
- **Business logic accuracy**: Does it implement the correct clinical criteria?
- **Age restrictions**: Are the age ranges applied correctly?
- **Data quality**: Are null dates and invalid codes handled properly?
- **Performance**: Does the SQL execute efficiently at scale?