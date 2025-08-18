# Analysis Examples

Three analysis files demonstrate how to use `person_month_analysis_base` for population health analytics.

## The Model

`person_month_analysis_base` is an incremental table containing person-month grain data for all registered patients. It combines active registrations, temporal demographics, condition flags, and practice information with pre-calculated date dimensions.

Built from:
- `fct_person_condition_episodes` - SCD2 condition history built on the same intermediates that power QOF and other registers, using diagnosis/resolved codes only
- `dim_person_demographics_historical` - SCD2 demographics  
- `fct_person_practice_registrations` - SCD2 practice registrations
- `int_date_spine` - date dimensions

**Note**: Condition flags use diagnosis/resolved codes only, without additional QOF requirements (e.g., COPD spirometry, asthma prescriptions). Prevalence will be higher than QOF registers.

## Core Patterns

```sql
-- 1. Practice registrations (from fct_person_practice_registrations)
SELECT practice_name, practice_borough, COUNT(DISTINCT person_id) as list_size
FROM {{ ref('person_month_analysis_base') }}
WHERE analysis_month = (SELECT MAX(analysis_month) FROM {{ ref('person_month_analysis_base') }})
GROUP BY ALL

-- 2. Demographics (from dim_person_demographics_historical)
SELECT age_band_nhs, sex, COUNT(DISTINCT person_id) as population
FROM {{ ref('person_month_analysis_base') }}
WHERE analysis_month = (SELECT MAX(analysis_month) FROM {{ ref('person_month_analysis_base') }})
GROUP BY ALL

-- 3. Conditions (from fct_person_condition_episodes)
SELECT COUNT(DISTINCT CASE WHEN has_dm THEN person_id END) as diabetes_count
FROM {{ ref('person_month_analysis_base') }}
WHERE analysis_month = (SELECT MAX(analysis_month) FROM {{ ref('person_month_analysis_base') }})

-- 4. Date dimensions (from int_date_spine)
SELECT financial_year, COUNT(DISTINCT person_id) as population
FROM {{ ref('person_month_analysis_base') }}
GROUP BY financial_year
ORDER BY financial_year

-- 5. Combined: CKD prevalence by neighbourhood and ethnicity (uses all four sources)
SELECT 
    practice_neighbourhood, -- registrations                                         
    ethnicity_category, -- demographics                                             
    COUNT(DISTINCT person_id) as population,
    COUNT(DISTINCT CASE WHEN has_ckd THEN person_id END) as ckd_cases, -- conditions
    ROUND(100 * COUNT(DISTINCT CASE WHEN has_ckd THEN person_id END) / 
          COUNT(DISTINCT person_id), 1) as ckd_prevalence_pct
FROM {{ ref('person_month_analysis_base') }}
WHERE financial_year = '2025/26' -- date spine                                   
GROUP BY ALL
HAVING COUNT(DISTINCT CASE WHEN has_ckd THEN person_id END) > 5  -- Small number suppression
ORDER BY practice_neighbourhood, ckd_prevalence_pct DESC
```

## Usage

Compile with `dbt compile --s analysis.*` then execute with Snowflake extension or copy SQL to Snowsight.