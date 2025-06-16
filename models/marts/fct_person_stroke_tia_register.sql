{{
    config(
        materialized='table'
    )
}}

/*
Stroke/TIA Register - QOF Cardiovascular Disease Quality Measures
Tracks all patients with stroke and transient ischaemic attack diagnoses.

Simple Register Pattern:
- Presence of stroke or TIA diagnosis = on register (lifelong condition)
- No resolution codes (stroke/TIA are permanent)
- No age restrictions
- Important for secondary prevention monitoring

QOF Business Rules:
1. Any stroke or TIA diagnosis code qualifies for register inclusion
2. Stroke/TIA are considered permanent conditions - no resolution
3. Used for secondary prevention medication monitoring
4. Cardiovascular risk management and anticoagulation decisions

Matches legacy fct_person_dx_stia business logic and field structure.
*/

WITH base_diagnoses AS (
    SELECT 
        person_id,
        has_stroke_tia_diagnosis,
        earliest_stroke_tia_date,
        latest_stroke_tia_date,
        total_stroke_tia_episodes,
        all_stroke_tia_concept_codes,
        all_stroke_tia_concept_displays
    FROM {{ ref('int_stroke_tia_diagnoses_all') }}
),

-- Add person demographics matching legacy structure
final AS (
    SELECT
        bd.person_id,
        p.sk_patient_id,
        p.age_years AS age,
        
        -- Register flag (always true for simple register pattern)
        TRUE AS is_on_stia_register,
        
        -- Diagnosis dates
        bd.earliest_stroke_tia_date,
        bd.latest_stroke_tia_date,
        
        -- Code arrays for traceability  
        bd.all_stroke_tia_concept_codes,
        bd.all_stroke_tia_concept_displays
        
    FROM base_diagnoses bd
    LEFT JOIN {{ ref('dim_person') }} p
        ON bd.person_id = p.person_id
)

SELECT * FROM final 