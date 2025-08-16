{{
    config(
        materialized='table')
}}

/*
CHD Register - QOF Cardiovascular Disease Quality Measures
Tracks all patients with coronary heart disease diagnoses.

Business Logic:
- Presence of CHD diagnosis = on register (lifelong condition)
- No resolution codes (CHD is permanent)
- No age restrictions

Clinical Context:
- Used for secondary prevention and cardiovascular risk management

QOF Business Rules:
1. Any CHD diagnosis code qualifies for register inclusion
2. CHD is considered a permanent condition - no resolution
3. Used for secondary prevention medication monitoring
4. Cardiovascular risk management

*/

WITH base_diagnoses AS (
    SELECT
        person_id,

        -- Person-level aggregation from observation-level data
        MIN(clinical_effective_date) AS earliest_diagnosis_date,
        MAX(clinical_effective_date) AS latest_diagnosis_date,
        COUNT(DISTINCT clinical_effective_date) AS total_chd_episodes,

        -- Traceability arrays
        ARRAY_AGG(DISTINCT concept_code) AS all_chd_concept_codes,
        ARRAY_AGG(DISTINCT concept_display) AS all_chd_concept_displays

    FROM {{ ref('int_chd_diagnoses_all') }}
    WHERE is_diagnosis_code = TRUE
    GROUP BY person_id
),

-- Add person demographics matching legacy structure
final AS (
    SELECT
        bd.person_id,
        age.age,

        -- Register flag (always true for simple register pattern)
        TRUE AS is_on_register,

        -- Diagnosis dates
        bd.earliest_diagnosis_date,
        bd.latest_diagnosis_date,

        -- Code arrays for traceability
        bd.all_chd_concept_codes,
        bd.all_chd_concept_displays

    FROM base_diagnoses AS bd
    LEFT JOIN {{ ref('dim_person') }} AS p ON bd.person_id = p.person_id
    LEFT JOIN {{ ref('dim_person_age') }} AS age ON bd.person_id = age.person_id
)

SELECT * FROM final
