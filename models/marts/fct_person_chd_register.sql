{{
    config(
        materialized='table'
    )
}}

/*
CHD Register - QOF Cardiovascular Disease Quality Measures
Tracks all patients with coronary heart disease diagnoses.

Simple Register Pattern:
- Presence of CHD diagnosis = on register (lifelong condition)
- No resolution codes (CHD is permanent)
- No age restrictions
- Important for secondary prevention monitoring

QOF Business Rules:
1. Any CHD diagnosis code qualifies for register inclusion
2. CHD is considered a permanent condition - no resolution
3. Used for secondary prevention medication monitoring
4. Cardiovascular risk management

Matches legacy fct_person_dx_chd business logic and field structure.
*/

WITH base_diagnoses AS (
    SELECT 
        person_id,
        has_chd_diagnosis,
        earliest_chd_date,
        latest_chd_date,
        total_chd_episodes,
        has_episode_last_12m,
        has_episode_last_24m,
        all_chd_concept_codes,
        all_chd_concept_displays
    FROM {{ ref('int_chd_diagnoses_all') }}
),

-- Add person demographics matching legacy structure
final AS (
    SELECT
        bd.person_id,
        p.sk_patient_id,
        p.age_years AS age,
        
        -- Register flag (always true for simple register pattern)
        TRUE AS is_on_chd_register,
        
        -- Episode timing flags
        bd.has_episode_last_24m,
        bd.has_episode_last_12m,
        
        -- Diagnosis dates
        bd.earliest_chd_date,
        bd.latest_chd_date,
        
        -- Code arrays for traceability  
        bd.all_chd_concept_codes,
        bd.all_chd_concept_displays
        
    FROM base_diagnoses bd
    LEFT JOIN {{ ref('dim_person') }} p
        ON bd.person_id = p.person_id
)

SELECT * FROM final 