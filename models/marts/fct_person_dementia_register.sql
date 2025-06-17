{{
    config(
        materialized='table',
        cluster_by=['person_id']
    )
}}

/*
**Dementia Register - QOF Mental Health Quality Measures**

Pattern 1: Simple Register (Diagnosis Only)

Business Logic:
- Presence of dementia diagnosis (DEM_COD) = on register
- No resolution codes (dementia is permanent condition)
- No age restrictions for dementia register
- Based on legacy fct_person_dx_dementia.sql

QOF Context:
Used for dementia quality measures including:
- Dementia care pathway monitoring
- Cognitive health assessment support
- Memory service referral tracking
- Early detection and ongoing care

Matches legacy business logic and field structure with simplification.
*/

WITH dementia_diagnoses AS (
    
    SELECT
        dem.person_id,
        dem.earliest_dementia_date,
        dem.latest_dementia_date,
        dem.all_dementia_concept_codes,
        dem.all_dementia_concept_displays
        
    FROM {{ ref('int_dementia_diagnoses_all') }} dem
    WHERE dem.has_dementia_diagnosis = TRUE
)

-- Final selection with person demographics
SELECT
    dd.person_id,
    age.age,
    TRUE AS is_on_dementia_register,
    dd.earliest_dementia_date AS earliest_dementia_diagnosis_date,
    dd.latest_dementia_date AS latest_dementia_diagnosis_date,
    dd.all_dementia_concept_codes,
    dd.all_dementia_concept_displays

FROM dementia_diagnoses dd
INNER JOIN {{ ref('dim_person') }} p
    ON dd.person_id = p.person_id
INNER JOIN {{ ref('dim_person_age') }} age
    ON dd.person_id = age.person_id 