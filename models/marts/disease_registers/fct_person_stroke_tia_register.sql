{{
    config(
        materialized='table',
        cluster_by=['person_id'],
        pre_hook="DROP TABLE IF EXISTS {{ this }}"
    )
}}

/*
Stroke and TIA register fact table - one row per person.
Applies QOF stroke register inclusion criteria with resolution logic.

Clinical Purpose:
- QOF stroke register for secondary prevention measures
- Cardiovascular risk management post-stroke
- Stroke care pathway monitoring

QOF Register Criteria:
- Person has stroke or TIA diagnosis code (STIA_COD)
- Not resolved/removed by resolution codes (STIARES_COD)
- No age restrictions
- Lifelong condition register for secondary prevention

Includes only active patients as per QOF population requirements.
This table provides one row per person for analytical use.
*/

WITH stroke_tia_diagnoses AS (
    SELECT
        person_id,
        
        -- Register inclusion dates  
        MIN(CASE WHEN is_stroke_tia_diagnosis_code THEN clinical_effective_date END) AS earliest_stroke_tia_date,
        MAX(CASE WHEN is_stroke_tia_diagnosis_code THEN clinical_effective_date END) AS latest_stroke_tia_date,
        
        -- Resolution dates
        MIN(CASE WHEN is_stroke_tia_resolved_code THEN clinical_effective_date END) AS earliest_resolution_date,
        MAX(CASE WHEN is_stroke_tia_resolved_code THEN clinical_effective_date END) AS latest_resolution_date,
        
        -- Episode counts
        COUNT(CASE WHEN is_stroke_tia_diagnosis_code THEN 1 END) AS total_stroke_tia_episodes,
        COUNT(CASE WHEN is_stroke_tia_resolved_code THEN 1 END) AS total_resolution_codes,
        
        -- Concept code arrays for traceability
        ARRAY_AGG(DISTINCT CASE WHEN is_stroke_tia_diagnosis_code THEN concept_code END) 
            AS stroke_tia_diagnosis_codes,
        ARRAY_AGG(DISTINCT CASE WHEN is_stroke_tia_resolved_code THEN concept_code END) 
            AS stroke_tia_resolution_codes,
        ARRAY_AGG(DISTINCT CASE WHEN is_stroke_tia_diagnosis_code THEN concept_display END) 
            AS stroke_tia_diagnosis_displays
            
    FROM {{ ref('int_stroke_tia_diagnoses_all') }}
    GROUP BY person_id
),

register_inclusion AS (
    SELECT
        std.*,
        
        -- QOF register logic: Include if has diagnosis and not resolved
        CASE 
            WHEN earliest_stroke_tia_date IS NOT NULL 
                 AND (earliest_resolution_date IS NULL 
                      OR earliest_resolution_date > latest_stroke_tia_date)
            THEN TRUE 
            ELSE FALSE 
        END AS is_on_stroke_tia_register,
        
        -- Clinical interpretation
        CASE 
            WHEN earliest_stroke_tia_date IS NOT NULL 
                 AND earliest_resolution_date IS NULL
            THEN 'Active stroke/TIA - never resolved'
            WHEN earliest_stroke_tia_date IS NOT NULL 
                 AND earliest_resolution_date > latest_stroke_tia_date
            THEN 'Active stroke/TIA - resolved before last diagnosis'
            WHEN earliest_stroke_tia_date IS NOT NULL 
                 AND earliest_resolution_date <= latest_stroke_tia_date  
            THEN 'Resolved stroke/TIA'
            ELSE 'No stroke/TIA diagnosis'
        END AS stroke_tia_status,
        
        -- Days calculations
        CASE 
            WHEN earliest_stroke_tia_date IS NOT NULL 
            THEN DATEDIFF(day, earliest_stroke_tia_date, CURRENT_DATE()) 
        END AS days_since_first_stroke_tia,
        
        CASE 
            WHEN latest_stroke_tia_date IS NOT NULL 
            THEN DATEDIFF(day, latest_stroke_tia_date, CURRENT_DATE()) 
        END AS days_since_latest_stroke_tia
        
    FROM stroke_tia_diagnoses std
)

SELECT
    ri.person_id,
    ri.is_on_stroke_tia_register,
    ri.stroke_tia_status,
    ri.earliest_stroke_tia_date,
    ri.latest_stroke_tia_date,
    ri.earliest_resolution_date,
    ri.latest_resolution_date,
    ri.total_stroke_tia_episodes,
    ri.total_resolution_codes,
    ri.days_since_first_stroke_tia,
    ri.days_since_latest_stroke_tia,
    ri.stroke_tia_diagnosis_codes,
    ri.stroke_tia_resolution_codes,
    ri.stroke_tia_diagnosis_displays
    
FROM register_inclusion ri
INNER JOIN {{ ref('dim_person_active_patients') }} ap
    ON ri.person_id = ap.person_id
WHERE ri.is_on_stroke_tia_register = TRUE

ORDER BY ri.earliest_stroke_tia_date DESC, ri.person_id 