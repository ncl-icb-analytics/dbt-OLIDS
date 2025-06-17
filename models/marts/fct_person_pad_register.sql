{{
    config(
        materialized='table'
    )
}}

/*
PAD Register - QOF Cardiovascular Disease Quality Measures
Tracks all patients with peripheral arterial disease diagnoses.

Simple Register Pattern:
- Presence of PAD diagnosis = on register (lifelong condition)
- No resolution codes (PAD is permanent)
- No age restrictions
- Important for secondary prevention monitoring

QOF Business Rules:
1. Any PAD diagnosis code qualifies for register inclusion
2. PAD is considered a permanent condition - no resolution
3. Used for secondary prevention medication monitoring
4. Cardiovascular risk management

Matches legacy fct_person_dx_pad business logic and field structure.
*/

WITH base_diagnoses AS (
    SELECT 
        person_id,
        earliest_pad_date,
        latest_pad_date,
        total_pad_episodes,
        all_pad_concept_codes,
        all_pad_concept_displays
    FROM {{ ref('int_pad_diagnoses_all') }}
),

-- Add person demographics matching legacy structure
final AS (
    SELECT
        bd.person_id,
        age.age,
        
        -- Register flag (always true for simple register pattern)
        TRUE AS is_on_pad_register,
        
        -- Diagnosis dates
        bd.earliest_pad_date,
        bd.latest_pad_date,
        
        -- Code arrays for traceability  
        bd.all_pad_concept_codes,
        bd.all_pad_concept_displays
        
    FROM base_diagnoses bd
    LEFT JOIN {{ ref('dim_person') }} p ON bd.person_id = p.person_id
    LEFT JOIN {{ ref('dim_person_age') }} age ON bd.person_id = age.person_id
)

SELECT * FROM final 