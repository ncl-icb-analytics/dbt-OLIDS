{{
    config(
        materialized='table',
        cluster_by=['person_id']
    )
}}

/*
**Palliative Care Register - QOF End-of-Life Quality Measures**

Pattern 2: Standard QOF Register (Diagnosis + Exclusion + Date Filter)

Business Logic:
- Palliative care code (PALCARE_COD) on/after 1 April 2008 (QOF date threshold)
- NOT marked as "no longer indicated" (PALCARENI_COD) after latest palliative care code
- No age restrictions for palliative care register
- Based on legacy fct_person_dx_palliative_care.sql

QOF Context:
Used for palliative care quality measures including:
- End-of-life care coordination and monitoring
- Palliative care pathway assessment
- Appropriate care targeting
- Quality of life monitoring

Matches legacy business logic and field structure with simplification.
*/

WITH palliative_care_observations AS (
    
    SELECT
        pc.person_id,
        pc.clinical_effective_date,
        pc.concept_code,
        pc.concept_display,
        pc.source_cluster_id,
        pc.is_palliative_care_code,
        pc.is_palliative_care_not_indicated_code,
        
        -- Extract palliative care and exclusion dates
        CASE WHEN pc.is_palliative_care_code AND pc.clinical_effective_date >= DATE '2008-04-01' 
             THEN pc.clinical_effective_date END AS palliative_care_date,
        CASE WHEN pc.is_palliative_care_not_indicated_code 
             THEN pc.clinical_effective_date END AS no_longer_indicated_date
        
    FROM {{ ref('int_palliative_care_diagnoses_all') }} pc
    WHERE pc.clinical_effective_date IS NOT NULL
),

person_aggregates AS (
    
    SELECT
        person_id,
        
        -- Palliative care dates (on/after 1 April 2008)
        MIN(palliative_care_date) AS earliest_palliative_care_date,
        MAX(palliative_care_date) AS latest_palliative_care_date,
        
        -- Exclusion dates ("no longer indicated")
        MIN(no_longer_indicated_date) AS earliest_no_longer_indicated_date,
        
        -- Concept arrays for traceability
        ARRAY_AGG(DISTINCT CASE WHEN is_palliative_care_code THEN concept_code END) 
            FILTER (WHERE is_palliative_care_code) AS all_palliative_care_concept_codes,
        ARRAY_AGG(DISTINCT CASE WHEN is_palliative_care_code THEN concept_display END) 
            FILTER (WHERE is_palliative_care_code) AS all_palliative_care_concept_displays,
        ARRAY_AGG(DISTINCT CASE WHEN is_palliative_care_not_indicated_code THEN concept_code END) 
            FILTER (WHERE is_palliative_care_not_indicated_code) AS all_no_longer_indicated_concept_codes,
        ARRAY_AGG(DISTINCT CASE WHEN is_palliative_care_not_indicated_code THEN concept_display END) 
            FILTER (WHERE is_palliative_care_not_indicated_code) AS all_no_longer_indicated_concept_displays
        
    FROM palliative_care_observations
    GROUP BY person_id
),

register_logic AS (
    
    SELECT
        pa.*,
        p.sk_patient_id,
        age.age,
        
        -- QOF Register Logic: Palliative care after April 2008 + not marked as no longer indicated
        (
            pa.latest_palliative_care_date IS NOT NULL
            AND (
                pa.earliest_no_longer_indicated_date IS NULL
                OR pa.earliest_no_longer_indicated_date <= pa.latest_palliative_care_date
            )
        ) AS is_on_palliative_care_register
        
    FROM person_aggregates pa
    INNER JOIN {{ ref('dim_person') }} p
        ON pa.person_id = p.person_id
    INNER JOIN {{ ref('dim_person_age') }} age
        ON pa.person_id = age.person_id
)

-- Final selection: Only include patients on the palliative care register
SELECT
    rl.person_id,
    rl.sk_patient_id,
    rl.age,
    rl.is_on_palliative_care_register,
    rl.earliest_palliative_care_date,
    rl.latest_palliative_care_date,
    rl.earliest_no_longer_indicated_date,
    rl.all_palliative_care_concept_codes,
    rl.all_palliative_care_concept_displays,
    rl.all_no_longer_indicated_concept_codes,
    rl.all_no_longer_indicated_concept_displays

FROM register_logic rl
WHERE rl.is_on_palliative_care_register = TRUE 