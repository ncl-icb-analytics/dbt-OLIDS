{{
    config(
        materialized='table',
        cluster_by=['person_id', 'clinical_effective_date']
    )
}}

/*
All learning disability diagnosis observations from clinical records.
Uses QOF learning disability cluster ID:
- LD_DIAGNOSIS_COD: Learning disability diagnoses

Clinical Purpose:
- QOF learning disability register data collection (age ≥14)
- Learning disability care pathway monitoring
- Special needs service planning
- Health equity monitoring

Key QOF Requirements:
- Register inclusion: Age ≥14 and presence of learning disability diagnosis (LD_DIAGNOSIS_COD)
- No resolution codes - learning disability is considered permanent condition
- Age threshold of 14 years for register inclusion
- No upper age limit

Note: Learning disability does not have resolved codes as it is considered a permanent condition.
The register is based purely on the presence of diagnostic codes and age criteria.

Includes ALL persons following intermediate layer principles.
Use as input for fct_person_learning_disability_register.sql which applies QOF business rules and age filtering.
*/

WITH base_observations AS (
    
    SELECT
        obs.observation_id,
        obs.person_id,
        obs.clinical_effective_date,
        obs.mapped_concept_code AS concept_code,
        obs.mapped_concept_display AS concept_display,
        obs.cluster_id AS source_cluster_id,
        
        -- Flag learning disability codes following QOF definitions
        CASE WHEN obs.cluster_id = 'LD_DIAGNOSIS_COD' THEN TRUE ELSE FALSE END AS is_learning_disability_diagnosis_code
        
    FROM ({{ get_observations("'LD_DIAGNOSIS_COD'") }}) obs
    WHERE obs.clinical_effective_date IS NOT NULL
),

person_diagnosis_aggregates AS (
    
    SELECT
        person_id,
        
        -- Learning disability diagnosis dates (LD_DIAGNOSIS_COD)
        MIN(clinical_effective_date) AS earliest_ld_diagnosis_date,
        MAX(clinical_effective_date) AS latest_ld_diagnosis_date,
        COUNT(*) AS total_ld_diagnoses,
        
        -- Arrays for traceability
        ARRAY_AGG(DISTINCT concept_code) 
            WITHIN GROUP (ORDER BY concept_code) 
            AS all_ld_concept_codes,
        ARRAY_AGG(DISTINCT concept_display) 
            WITHIN GROUP (ORDER BY concept_display) 
            AS all_ld_concept_displays
            
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
        bo.is_learning_disability_diagnosis_code,
        
        -- Person-level aggregates
        pda.earliest_ld_diagnosis_date,
        pda.latest_ld_diagnosis_date,
        pda.total_ld_diagnoses,
        
        -- Classification of this specific observation
        CASE 
            WHEN bo.is_learning_disability_diagnosis_code THEN 'Learning Disability Diagnosis'
            ELSE 'Unknown'
        END AS ld_observation_type,
        
        -- QOF context fields (always true since we only have diagnosis codes)
        TRUE AS has_learning_disability_diagnosis,
        
        -- Clinical flags for care planning
        CASE 
            WHEN pda.latest_ld_diagnosis_date >= CURRENT_DATE - INTERVAL '12 months' THEN TRUE
            ELSE FALSE
        END AS has_recent_ld_diagnosis,
        CASE 
            WHEN pda.latest_ld_diagnosis_date >= CURRENT_DATE - INTERVAL '24 months' THEN TRUE
            ELSE FALSE
        END AS has_ld_diagnosis_last_24m,
        
        -- Disease management indicators
        CASE WHEN pda.total_ld_diagnoses = 1 THEN TRUE ELSE FALSE END AS is_single_ld_diagnosis,
        CASE WHEN pda.total_ld_diagnoses > 1 THEN TRUE ELSE FALSE END AS has_multiple_ld_diagnoses,
        
        -- Care planning fields
        CASE 
            WHEN pda.earliest_ld_diagnosis_date IS NOT NULL 
            THEN CURRENT_DATE - pda.earliest_ld_diagnosis_date
            ELSE NULL
        END AS days_since_first_ld_diagnosis,
        
        CASE 
            WHEN pda.earliest_ld_diagnosis_date >= CURRENT_DATE - INTERVAL '2 years' THEN TRUE
            ELSE FALSE
        END AS is_newly_diagnosed_ld,
        CASE 
            WHEN pda.earliest_ld_diagnosis_date < CURRENT_DATE - INTERVAL '2 years' THEN TRUE
            ELSE FALSE
        END AS is_established_ld,
        CASE 
            WHEN pda.earliest_ld_diagnosis_date < CURRENT_DATE - INTERVAL '5 years' THEN TRUE
            ELSE FALSE
        END AS is_long_term_ld,
        
        -- Service planning indicators
        CASE 
            WHEN pda.total_ld_diagnoses > 1 
                AND (pda.latest_ld_diagnosis_date - pda.earliest_ld_diagnosis_date) > INTERVAL '6 months'
            THEN TRUE
            ELSE FALSE
        END AS has_ld_progression_codes,
        
        -- Arrays for complete traceability
        pda.all_ld_concept_codes,
        pda.all_ld_concept_displays
        
    FROM base_observations bo
    LEFT JOIN person_diagnosis_aggregates pda
        ON bo.person_id = pda.person_id
)

SELECT * FROM final_with_derived_fields
ORDER BY person_id, clinical_effective_date 