{{
    config(
        materialized='table'
    )
}}

/*
Rheumatoid Arthritis Register - QOF Quality Measures
Tracks all patients aged 16+ with rheumatoid arthritis diagnoses.

Simple Register Pattern with Age Filter:
- Presence of RA diagnosis for age ≥16 = on register
- No resolution codes (RA is permanent)
- Age restriction: patients must be 16+ years
- Important for DMARDs monitoring

QOF Business Rules:
1. RA diagnosis (RARTH_COD) for patients aged 16+ qualifies for register
2. RA is considered a permanent condition - no resolution
3. Used for DMARDs therapy monitoring and joint health assessment
4. Supports rheumatology care quality measures

Matches legacy fct_person_dx_ra business logic and field structure.
*/

WITH base_diagnoses AS (
    SELECT 
        person_id,
        earliest_ra_date,
        latest_ra_date,
        total_ra_episodes,
        all_ra_concept_codes,
        all_ra_concept_displays
    FROM {{ ref('int_rheumatoid_arthritis_diagnoses_all') }}
),

-- Add person demographics and apply age filter
final AS (
    SELECT
        bd.person_id,
        age.age,
        
        -- Register flag (always true after age filtering)
        TRUE AS is_on_ra_register,
        
        -- Diagnosis dates
        bd.earliest_ra_date AS earliest_ra_diagnosis_date,
        bd.latest_ra_date AS latest_ra_diagnosis_date,
        
        -- Code arrays for traceability  
        bd.all_ra_concept_codes,
        bd.all_ra_concept_displays
        
    FROM base_diagnoses bd
    LEFT JOIN {{ ref('dim_person') }} p ON bd.person_id = p.person_id
    LEFT JOIN {{ ref('dim_person_age') }} age ON bd.person_id = age.person_id
    
    -- Apply QOF age filter: patients aged 16 or over
    WHERE age.age >= 16
)

SELECT * FROM final 