{{
    config(
        materialized='table',
        cluster_by=['person_id'],
        pre_hook="DROP TABLE IF EXISTS {{ this }}"
    )
}}

/*
Non-Alcoholic Fatty Liver Disease (NAFLD) register fact table - one row per person.
Applies NAFLD register inclusion criteria.

Clinical Purpose:
- NAFLD diagnosis tracking and monitoring
- Liver health assessment
- Potential QOF register development

Register Criteria (Simple Pattern):
- Any NAFLD diagnosis code (hardcoded SNOMED concepts)
- No age restrictions
- No resolution codes (simple diagnosis-based register)

⚠️ TODO: Update with proper cluster ID once NAFLD_COD becomes available in codesets.

Includes only active patients as per standard population requirements.
This table provides one row per person for analytical use.
*/

WITH nafld_diagnoses AS (
    SELECT
        person_id,
        
        -- Register inclusion dates  
        MIN(CASE WHEN is_nafld_diagnosis_code THEN clinical_effective_date END) AS earliest_nafld_date,
        MAX(CASE WHEN is_nafld_diagnosis_code THEN clinical_effective_date END) AS latest_nafld_date,
        
        -- Episode counts
        COUNT(CASE WHEN is_nafld_diagnosis_code THEN 1 END) AS total_nafld_episodes,
        
        -- Concept code arrays for traceability
        ARRAY_AGG(DISTINCT CASE WHEN is_nafld_diagnosis_code THEN concept_code END) 
            AS nafld_diagnosis_codes,
        ARRAY_AGG(DISTINCT CASE WHEN is_nafld_diagnosis_code THEN concept_display END) 
            AS nafld_diagnosis_displays
            
    FROM {{ ref('int_nafld_diagnoses_all') }}
    GROUP BY person_id
),

register_inclusion AS (
    SELECT
        nd.*,
        
        -- Simple register logic: Include if has diagnosis
        CASE 
            WHEN earliest_nafld_date IS NOT NULL 
            THEN TRUE 
            ELSE FALSE 
        END AS is_on_nafld_register,
        
        -- Clinical interpretation
        CASE 
            WHEN earliest_nafld_date IS NOT NULL 
            THEN 'Active NAFLD diagnosis'
            ELSE 'No NAFLD diagnosis'
        END AS nafld_status,
        
        -- Days calculations
        CASE 
            WHEN earliest_nafld_date IS NOT NULL 
            THEN DATEDIFF(day, earliest_nafld_date, CURRENT_DATE()) 
        END AS days_since_first_nafld,
        
        CASE 
            WHEN latest_nafld_date IS NOT NULL 
            THEN DATEDIFF(day, latest_nafld_date, CURRENT_DATE()) 
        END AS days_since_latest_nafld
        
    FROM nafld_diagnoses nd
)

SELECT
    ri.person_id,
    ri.is_on_nafld_register,
    ri.nafld_status,
    ri.earliest_nafld_date,
    ri.latest_nafld_date,
    ri.total_nafld_episodes,
    ri.days_since_first_nafld,
    ri.days_since_latest_nafld,
    ri.nafld_diagnosis_codes,
    ri.nafld_diagnosis_displays
    
FROM register_inclusion ri
INNER JOIN {{ ref('dim_person_active_patients') }} ap
    ON ri.person_id = ap.person_id
WHERE ri.is_on_nafld_register = TRUE

ORDER BY ri.earliest_nafld_date DESC, ri.person_id 