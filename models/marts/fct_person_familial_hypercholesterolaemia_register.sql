{{
    config(
        materialized='table'
    )
}}

/*
Familial Hypercholesterolaemia Register - QOF Quality Measures
Tracks all patients with familial hypercholesterolaemia diagnoses.

Simple Register Pattern:
- Presence of FH diagnosis = on register (lifelong genetic condition)
- No resolution codes (FH is permanent genetic condition)
- No age restrictions
- Important for cascade screening and high-intensity statin therapy

QOF Business Rules:
1. Any FH diagnosis code qualifies for register inclusion
2. FH is considered a permanent genetic condition - no resolution
3. Used for cascade family screening programmes
4. High-intensity statin therapy monitoring

Matches legacy fct_person_dx_fhyp business logic and field structure.
*/

WITH base_diagnoses AS (
    SELECT 
        person_id,
        has_fh_diagnosis,
        earliest_fh_date,
        latest_fh_date,
        total_fh_episodes,
        all_fh_concept_codes,
        all_fh_concept_displays
    FROM {{ ref('int_familial_hypercholesterolaemia_diagnoses_all') }}
),

-- Add person demographics matching legacy structure
final AS (
    SELECT
        bd.person_id,
        p.sk_patient_id,
        p.age_years AS age,
        
        -- Register flag (always true for simple register pattern)
        TRUE AS is_on_fhyp_register,
        
        -- Diagnosis dates
        bd.earliest_fh_date AS earliest_fhyp_date,
        bd.latest_fh_date AS latest_fhyp_date,
        
        -- Code arrays for traceability  
        bd.all_fh_concept_codes AS all_fhyp_concept_codes,
        bd.all_fh_concept_displays AS all_fhyp_concept_displays
        
    FROM base_diagnoses bd
    LEFT JOIN {{ ref('dim_person') }} p
        ON bd.person_id = p.person_id
)

SELECT * FROM final 