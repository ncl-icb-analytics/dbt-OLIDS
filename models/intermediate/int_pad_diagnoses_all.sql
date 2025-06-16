{{
    config(
        materialized='table',
        cluster_by=['person_id', 'clinical_effective_date']
    )
}}

/*
All peripheral arterial disease (PAD) diagnosis observations from clinical records.
Uses QOF PAD cluster ID:
- PAD_COD: Peripheral arterial disease diagnoses

Clinical Purpose:
- QOF PAD register data collection
- Peripheral vascular disease monitoring
- Cardiovascular risk assessment
- Limb preservation planning

Key QOF Requirements:
- Register inclusion: Presence of PAD diagnosis code (PAD_COD)
- No age restrictions for PAD register
- No resolution codes - PAD is considered permanent condition
- Important for cardiovascular disease management

Note: PAD does not have resolved codes as it is considered a permanent condition.
The register is based purely on the presence of diagnostic codes.

Includes ALL persons following intermediate layer principles.
Use as input for fct_person_pad_register.sql which applies QOF business rules.
*/

WITH base_observations AS (
    
    SELECT
        obs.observation_id,
        obs.person_id,
        obs.clinical_effective_date,
        obs.concept_code,
        obs.concept_display,
        obs.source_cluster_id,
        
        -- Flag PAD diagnosis codes following QOF definitions
        CASE WHEN obs.source_cluster_id = 'PAD_COD' THEN TRUE ELSE FALSE END AS is_pad_diagnosis_code
        
    FROM {{ get_observations("'PAD_COD'") }} obs
    WHERE obs.clinical_effective_date IS NOT NULL
),

person_diagnosis_aggregates AS (
    
    SELECT
        person_id,
        
        -- PAD diagnosis dates (PAD_COD)
        MIN(clinical_effective_date) AS earliest_pad_diagnosis_date,
        MAX(clinical_effective_date) AS latest_pad_diagnosis_date,
        COUNT(*) AS total_pad_diagnoses,
        
        -- Arrays for traceability
        ARRAY_AGG(DISTINCT concept_code) 
            WITHIN GROUP (ORDER BY concept_code) 
            AS all_pad_concept_codes,
        ARRAY_AGG(DISTINCT concept_display) 
            WITHIN GROUP (ORDER BY concept_display) 
            AS all_pad_concept_displays
            
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
        bo.is_pad_diagnosis_code,
        
        -- Person-level aggregates
        pda.earliest_pad_diagnosis_date,
        pda.latest_pad_diagnosis_date,
        pda.total_pad_diagnoses,
        
        -- Classification of this specific observation
        CASE 
            WHEN bo.is_pad_diagnosis_code THEN 'PAD Diagnosis'
            ELSE 'Unknown'
        END AS pad_observation_type,
        
        -- QOF context fields (always true since we only have diagnosis codes)
        TRUE AS has_pad_diagnosis,
        
        -- Clinical flags for care planning
        CASE 
            WHEN pda.latest_pad_diagnosis_date >= CURRENT_DATE - INTERVAL '12 months' THEN TRUE
            ELSE FALSE
        END AS has_recent_pad_diagnosis,
        CASE 
            WHEN pda.latest_pad_diagnosis_date >= CURRENT_DATE - INTERVAL '24 months' THEN TRUE
            ELSE FALSE
        END AS has_pad_diagnosis_last_24m,
        
        -- Disease management indicators
        CASE WHEN pda.total_pad_diagnoses = 1 THEN TRUE ELSE FALSE END AS is_single_pad_diagnosis,
        CASE WHEN pda.total_pad_diagnoses > 1 THEN TRUE ELSE FALSE END AS has_multiple_pad_diagnoses,
        
        -- Vascular care planning fields
        CASE 
            WHEN pda.earliest_pad_diagnosis_date IS NOT NULL 
            THEN CURRENT_DATE - pda.earliest_pad_diagnosis_date
            ELSE NULL
        END AS days_since_first_pad_diagnosis,
        
        CASE 
            WHEN pda.earliest_pad_diagnosis_date >= CURRENT_DATE - INTERVAL '1 year' THEN TRUE
            ELSE FALSE
        END AS is_newly_diagnosed_pad,
        CASE 
            WHEN pda.earliest_pad_diagnosis_date < CURRENT_DATE - INTERVAL '1 year' THEN TRUE
            ELSE FALSE
        END AS is_established_pad,
        CASE 
            WHEN pda.earliest_pad_diagnosis_date < CURRENT_DATE - INTERVAL '5 years' THEN TRUE
            ELSE FALSE
        END AS is_long_term_pad,
        
        -- Cardiovascular risk indicators
        CASE 
            WHEN pda.total_pad_diagnoses > 1 
                AND (pda.latest_pad_diagnosis_date - pda.earliest_pad_diagnosis_date) > INTERVAL '6 months'
            THEN TRUE
            ELSE FALSE
        END AS has_pad_progression_codes,
        
        -- Arrays for complete traceability
        pda.all_pad_concept_codes,
        pda.all_pad_concept_displays
        
    FROM base_observations bo
    LEFT JOIN person_diagnosis_aggregates pda
        ON bo.person_id = pda.person_id
)

SELECT * FROM final_with_derived_fields
ORDER BY person_id, clinical_effective_date 