{{
    config(
        materialized='table'
    )
}}

/*
Cancer Register - QOF Quality Measures
Tracks all patients with cancer diagnoses on/after April 1, 2003.

Simple Register Pattern with Date Filter:
- Presence of cancer diagnosis on/after 1 April 2003 = on register
- No resolution codes (cancer is permanent)
- No age restrictions
- Excludes non-melanotic skin cancers (handled in cluster definition)

QOF Business Rules:
1. Cancer diagnosis (CAN_COD) on/after 1 April 2003 qualifies for register
2. Cancer is considered a permanent condition - no resolution
3. Used for survivorship care and follow-up monitoring
4. Supports cancer care quality measures

Note: Legacy includes episode timing flags, but keeping this model simple 
per architectural guidance. Episode analysis can be done separately if needed.

Matches legacy fct_person_dx_cancer business logic and field structure.
*/

WITH base_diagnoses AS (
    SELECT 
        person_id,
        has_cancer_diagnosis,
        earliest_cancer_date,
        latest_cancer_date,
        total_cancer_episodes,
        all_cancer_concept_codes,
        all_cancer_concept_displays
    FROM {{ ref('int_cancer_diagnoses_all') }}
    
    -- Apply QOF date filter: cancer diagnosis on/after 1 April 2003
    WHERE latest_cancer_date >= DATE '2003-04-01'
),

-- Add person demographics matching legacy structure
final AS (
    SELECT
        bd.person_id,
        p.sk_patient_id,
        p.age_years AS age,
        
        -- Register flag (always true after date filtering)
        TRUE AS is_on_cancer_register,
        
        -- Diagnosis dates
        bd.earliest_cancer_date,
        bd.latest_cancer_date,
        
        -- Code arrays for traceability  
        bd.all_cancer_concept_codes,
        bd.all_cancer_concept_displays
        
    FROM base_diagnoses bd
    LEFT JOIN {{ ref('dim_person') }} p
        ON bd.person_id = p.person_id
)

SELECT * FROM final 