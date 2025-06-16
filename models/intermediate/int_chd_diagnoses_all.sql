{{
    config(
        materialized='table',
        cluster_by=['person_id', 'clinical_effective_date']
    )
}}

/*
All coronary heart disease (CHD) diagnosis observations from clinical records.
Uses QOF CHD cluster ID:
- CHD_COD: Coronary heart disease diagnoses

Clinical Purpose:
- QOF CHD register data collection
- Cardiovascular risk assessment
- Coronary disease management monitoring
- Secondary prevention planning

Key QOF Requirements:
- Register inclusion: Presence of CHD diagnosis code (CHD_COD)
- No age restrictions for CHD register
- No resolution codes - CHD is considered permanent condition
- Important for cardiovascular disease secondary prevention

Note: CHD does not have resolved codes as it is considered a permanent condition.
The register is based purely on the presence of diagnostic codes.

Includes ALL persons following intermediate layer principles.
Use as input for fct_person_chd_register.sql which applies QOF business rules.
*/

WITH base_observations AS (
    
    SELECT
        obs.observation_id,
        obs.person_id,
        obs.clinical_effective_date,
        obs.concept_code,
        obs.concept_display,
        obs.source_cluster_id,
        
        -- Flag CHD diagnosis codes following QOF definitions
        CASE WHEN obs.source_cluster_id = 'CHD_COD' THEN TRUE ELSE FALSE END AS is_chd_diagnosis_code
        
    FROM {{ get_observations("'CHD_COD'") }} obs
    WHERE obs.clinical_effective_date IS NOT NULL
),

person_diagnosis_aggregates AS (
    
    SELECT
        person_id,
        
        -- CHD diagnosis dates (CHD_COD)
        MIN(clinical_effective_date) AS earliest_chd_diagnosis_date,
        MAX(clinical_effective_date) AS latest_chd_diagnosis_date,
        COUNT(*) AS total_chd_diagnoses,
        
        -- Arrays for traceability
        ARRAY_AGG(DISTINCT concept_code) 
            WITHIN GROUP (ORDER BY concept_code) 
            AS all_chd_concept_codes,
        ARRAY_AGG(DISTINCT concept_display) 
            WITHIN GROUP (ORDER BY concept_display) 
            AS all_chd_concept_displays
            
    FROM base_observations
    GROUP BY person_id
),

final_with_derived_fields AS (
    
    SELECT
        bo.person_id,
        bo.observation_id,
        bo.clinical_effective_date,
        bo.concept_code,
        bo.concept_display,
        bo.source_cluster_id,
        bo.is_chd_diagnosis_code,
        
        -- Person-level aggregates
        pda.earliest_chd_diagnosis_date,
        pda.latest_chd_diagnosis_date,
        pda.total_chd_diagnoses,
        
        -- Classification of this specific observation
        CASE 
            WHEN bo.is_chd_diagnosis_code THEN 'CHD Diagnosis'
            ELSE 'Unknown'
        END AS chd_observation_type,
        
        -- QOF context fields (always true since we only have diagnosis codes)
        TRUE AS has_chd_diagnosis,
        
        -- Clinical flags for care planning
        CASE 
            WHEN pda.latest_chd_diagnosis_date >= CURRENT_DATE - INTERVAL '12 months' THEN TRUE
            ELSE FALSE
        END AS has_recent_chd_diagnosis,
        CASE 
            WHEN pda.latest_chd_diagnosis_date >= CURRENT_DATE - INTERVAL '24 months' THEN TRUE
            ELSE FALSE
        END AS has_chd_diagnosis_last_24m,
        
        -- Disease management indicators
        CASE WHEN pda.total_chd_diagnoses = 1 THEN TRUE ELSE FALSE END AS is_single_chd_diagnosis,
        CASE WHEN pda.total_chd_diagnoses > 1 THEN TRUE ELSE FALSE END AS has_multiple_chd_diagnoses,
        
        -- Care planning fields
        CASE 
            WHEN pda.earliest_chd_diagnosis_date IS NOT NULL 
            THEN CURRENT_DATE - pda.earliest_chd_diagnosis_date
            ELSE NULL
        END AS days_since_first_chd_diagnosis,
        
        CASE 
            WHEN pda.earliest_chd_diagnosis_date >= CURRENT_DATE - INTERVAL '1 year' THEN TRUE
            ELSE FALSE
        END AS is_newly_diagnosed_chd,
        CASE 
            WHEN pda.earliest_chd_diagnosis_date < CURRENT_DATE - INTERVAL '1 year' THEN TRUE
            ELSE FALSE
        END AS is_established_chd,
        CASE 
            WHEN pda.earliest_chd_diagnosis_date < CURRENT_DATE - INTERVAL '5 years' THEN TRUE
            ELSE FALSE
        END AS is_long_term_chd,
        
        -- Secondary prevention indicators
        CASE 
            WHEN pda.total_chd_diagnoses > 1 
                AND (pda.latest_chd_diagnosis_date - pda.earliest_chd_diagnosis_date) > INTERVAL '6 months'
            THEN TRUE
            ELSE FALSE
        END AS has_chd_progression_codes,
        
        -- Arrays for complete traceability
        pda.all_chd_concept_codes,
        pda.all_chd_concept_displays
        
    FROM base_observations bo
    LEFT JOIN person_diagnosis_aggregates pda
        ON bo.person_id = pda.person_id
)

SELECT * FROM final_with_derived_fields
ORDER BY person_id, clinical_effective_date 