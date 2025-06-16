{{
    config(
        materialized='table',
        indexes=[
            {'columns': ['person_id'], 'unique': false},
            {'columns': ['person_id', 'has_nafld_diagnosis'], 'unique': false}
        ]
    )
}}

WITH nafld_diagnoses AS (
    -- Get all NAFLD diagnoses per person
    SELECT 
        person_id,
        MIN(clinical_effective_date) AS earliest_nafld_date,
        MAX(clinical_effective_date) AS latest_nafld_date,
        COUNT(*) AS total_nafld_diagnoses,
        
        -- Collect all concept codes and displays
        ARRAY_AGG(DISTINCT concept_code) AS all_nafld_concept_codes,
        ARRAY_AGG(DISTINCT concept_display) AS all_nafld_concept_displays
        
    FROM {{ ref('int_nafld_diagnoses_all') }}
    GROUP BY person_id
),

person_demographics AS (
    -- Get person demographics for context
    SELECT 
        p.person_id,
        p.sk_patient_id,
        age.age,
        prac.practice_code
    FROM {{ ref('dim_person') }} p
    JOIN {{ ref('dim_person_age') }} age ON p.person_id = age.person_id
    LEFT JOIN {{ ref('dim_patient_practice') }} prac ON p.sk_patient_id = prac.sk_patient_id
)

SELECT
    demo.person_id,
    demo.sk_patient_id,
    demo.age,
    demo.practice_code,
    
    -- NAFLD diagnosis flags
    CASE WHEN nafld.person_id IS NOT NULL THEN TRUE ELSE FALSE END AS has_nafld_diagnosis,
    
    -- NAFLD diagnosis details
    nafld.earliest_nafld_date,
    nafld.latest_nafld_date,
    COALESCE(nafld.total_nafld_diagnoses, 0) AS total_nafld_diagnoses,
    
    -- Years since first NAFLD diagnosis
    CASE 
        WHEN nafld.earliest_nafld_date IS NOT NULL 
        THEN DATEDIFF(year, nafld.earliest_nafld_date, CURRENT_DATE())
        ELSE NULL 
    END AS years_since_first_nafld_diagnosis,
    
    -- Days since latest NAFLD diagnosis
    CASE 
        WHEN nafld.latest_nafld_date IS NOT NULL 
        THEN DATEDIFF(day, nafld.latest_nafld_date, CURRENT_DATE())
        ELSE NULL 
    END AS days_since_latest_nafld_diagnosis,
    
    -- Concept arrays
    nafld.all_nafld_concept_codes,
    nafld.all_nafld_concept_displays

FROM person_demographics demo
LEFT JOIN nafld_diagnoses nafld ON demo.person_id = nafld.person_id
WHERE nafld.person_id IS NOT NULL -- Only include people with NAFLD diagnoses

ORDER BY demo.person_id 