{{
    config(
        materialized='table',
        cluster_by=['person_id'],
        pre_hook="DROP TABLE IF EXISTS {{ this }}"
    )
}}

/*
Peripheral Arterial Disease (PAD) register fact table - one row per person.
Applies QOF PAD register inclusion criteria.

Clinical Purpose:
- QOF PAD register for cardiovascular disease management
- Cardiovascular risk stratification and monitoring
- Secondary prevention pathway identification

QOF Register Criteria (Simple Pattern):
- Any PAD diagnosis code (PAD_COD)
- No age restrictions
- No resolution codes (simple diagnosis-based register)
- Lifelong condition register for cardiovascular secondary prevention

Includes only active patients as per QOF population requirements.
This table provides one row per person for analytical use.
*/

WITH pad_diagnoses AS (
    SELECT
        person_id,
        
        -- Register inclusion dates  
        MIN(CASE WHEN is_pad_diagnosis_code THEN clinical_effective_date END) AS earliest_diagnosis_date,
        MAX(CASE WHEN is_pad_diagnosis_code THEN clinical_effective_date END) AS latest_diagnosis_date,
        
        -- Episode counts
        COUNT(CASE WHEN is_pad_diagnosis_code THEN 1 END) AS total_pad_episodes,
        
        -- Concept code arrays for traceability
        ARRAY_AGG(DISTINCT CASE WHEN is_pad_diagnosis_code THEN concept_code END) 
            AS pad_diagnosis_codes,
        ARRAY_AGG(DISTINCT CASE WHEN is_pad_diagnosis_code THEN concept_display END) 
            AS pad_diagnosis_displays,
        
        -- Latest observation details
        ARRAY_AGG(DISTINCT observation_id) AS all_observation_ids
            
    FROM {{ ref('int_pad_diagnoses_all') }}
    GROUP BY person_id
),

register_inclusion AS (
    SELECT
        pd.*,
        
        -- Simple register logic: Include if has diagnosis
        CASE 
            WHEN earliest_diagnosis_date IS NOT NULL 
            THEN TRUE 
            ELSE FALSE 
        END AS is_on_pad_register,
        
        -- Clinical interpretation
        CASE 
            WHEN earliest_diagnosis_date IS NOT NULL 
            THEN 'Active PAD diagnosis'
            ELSE 'No PAD diagnosis'
        END AS pad_status,
        
        -- Days calculations
        CASE 
            WHEN earliest_diagnosis_date IS NOT NULL 
            THEN DATEDIFF(day, earliest_diagnosis_date, CURRENT_DATE()) 
        END AS days_since_first_pad,
        
        CASE 
            WHEN latest_diagnosis_date IS NOT NULL 
            THEN DATEDIFF(day, latest_diagnosis_date, CURRENT_DATE()) 
        END AS days_since_latest_pad
        
    FROM pad_diagnoses pd
)

SELECT
    ri.person_id,
    ri.is_on_pad_register,
    ri.pad_status,
    ri.earliest_diagnosis_date,
    ri.latest_diagnosis_date,
    ri.total_pad_episodes,
    ri.days_since_first_pad,
    ri.days_since_latest_pad,
    ri.pad_diagnosis_codes,
    ri.pad_diagnosis_displays,
    ri.all_observation_ids
    
FROM register_inclusion ri
INNER JOIN {{ ref('dim_person_active_patients') }} ap
    ON ri.person_id = ap.person_id
WHERE ri.is_on_pad_register = TRUE

ORDER BY ri.earliest_diagnosis_date DESC, ri.person_id 