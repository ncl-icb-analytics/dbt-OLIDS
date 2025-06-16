{{
    config(
        materialized='table',
        cluster_by=['person_id', 'clinical_effective_date']
    )
}}

/*
All atrial fibrillation diagnosis observations from clinical records.
Uses QOF atrial fibrillation cluster IDs:
- AFIB_COD: Atrial fibrillation diagnoses
- AFIBRES_COD: Atrial fibrillation resolved/remission codes

Clinical Purpose:
- QOF atrial fibrillation register data collection
- Arrhythmia management monitoring
- Anticoagulation therapy planning
- Stroke prevention assessment

Key QOF Requirements:
- Register inclusion: Atrial fibrillation diagnosis (AFIB_COD) not in remission (latest AFIB_COD > latest AFIBRES_COD)
- No age restrictions for AF register
- Resolution status tracking important for anticoagulation decisions
- Critical for stroke prevention planning

Includes ALL persons following intermediate layer principles.
Use as input for fct_person_atrial_fibrillation_register.sql which applies QOF business rules.
*/

WITH base_observations AS (
    
    SELECT
        obs.observation_id,
        obs.person_id,
        obs.clinical_effective_date,
        obs.concept_code,
        obs.concept_display,
        obs.source_cluster_id,
        
        -- Flag different types of atrial fibrillation codes following QOF definitions
        CASE WHEN obs.source_cluster_id = 'AFIB_COD' THEN TRUE ELSE FALSE END AS is_af_diagnosis_code,
        CASE WHEN obs.source_cluster_id = 'AFIBRES_COD' THEN TRUE ELSE FALSE END AS is_af_resolved_code
        
    FROM {{ get_observations("'AFIB_COD', 'AFIBRES_COD'") }} obs
    WHERE obs.clinical_effective_date IS NOT NULL
),

person_diagnosis_aggregates AS (
    
    SELECT
        person_id,
        
        -- AF diagnosis dates (AFIB_COD)
        MIN(CASE WHEN is_af_diagnosis_code THEN clinical_effective_date END) AS earliest_af_diagnosis_date,
        MAX(CASE WHEN is_af_diagnosis_code THEN clinical_effective_date END) AS latest_af_diagnosis_date,
        COUNT(CASE WHEN is_af_diagnosis_code THEN 1 END) AS total_af_diagnoses,
        
        -- AF resolved dates (AFIBRES_COD)
        MIN(CASE WHEN is_af_resolved_code THEN clinical_effective_date END) AS earliest_af_resolved_date,
        MAX(CASE WHEN is_af_resolved_code THEN clinical_effective_date END) AS latest_af_resolved_date,
        COUNT(CASE WHEN is_af_resolved_code THEN 1 END) AS total_af_resolved,
        
        -- Arrays for traceability
        ARRAY_AGG(DISTINCT concept_code) 
            WITHIN GROUP (ORDER BY concept_code) 
            AS all_af_concept_codes,
        ARRAY_AGG(DISTINCT concept_display) 
            WITHIN GROUP (ORDER BY concept_display) 
            AS all_af_concept_displays
            
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
        bo.is_af_diagnosis_code,
        bo.is_af_resolved_code,
        
        -- Person-level aggregates
        pda.earliest_af_diagnosis_date,
        pda.latest_af_diagnosis_date,
        pda.total_af_diagnoses,
        pda.earliest_af_resolved_date,
        pda.latest_af_resolved_date,
        pda.total_af_resolved,
        
        -- Classification of this specific observation
        CASE 
            WHEN bo.is_af_diagnosis_code THEN 'AF Diagnosis'
            WHEN bo.is_af_resolved_code THEN 'AF Resolved'
            ELSE 'Unknown'
        END AS af_observation_type,
        
        -- QOF resolution logic (critical for register inclusion)
        CASE 
            WHEN pda.latest_af_diagnosis_date IS NOT NULL 
                AND (pda.latest_af_resolved_date IS NULL 
                     OR pda.latest_af_diagnosis_date > pda.latest_af_resolved_date)
            THEN TRUE
            ELSE FALSE
        END AS has_active_af_diagnosis,
        
        -- AF status categorisation
        CASE 
            WHEN pda.latest_af_diagnosis_date IS NULL THEN 'No AF Diagnosis'
            WHEN pda.latest_af_resolved_date IS NULL THEN 'Active AF'
            WHEN pda.latest_af_diagnosis_date > pda.latest_af_resolved_date THEN 'Active AF'
            WHEN pda.latest_af_resolved_date >= pda.latest_af_diagnosis_date THEN 'Resolved AF'
            ELSE 'Unknown AF Status'
        END AS af_status,
        
        -- Clinical flags for care planning
        CASE 
            WHEN pda.latest_af_diagnosis_date >= CURRENT_DATE - INTERVAL '12 months' THEN TRUE
            ELSE FALSE
        END AS has_recent_af_diagnosis,
        CASE 
            WHEN pda.latest_af_diagnosis_date >= CURRENT_DATE - INTERVAL '24 months' THEN TRUE
            ELSE FALSE
        END AS has_af_diagnosis_last_24m,
        
        -- Resolution status indicators
        CASE WHEN pda.total_af_resolved > 0 THEN TRUE ELSE FALSE END AS has_af_resolved_codes,
        CASE WHEN pda.total_af_diagnoses = 1 THEN TRUE ELSE FALSE END AS is_single_af_diagnosis,
        CASE WHEN pda.total_af_diagnoses > 1 THEN TRUE ELSE FALSE END AS has_multiple_af_diagnoses,
        
        -- Anticoagulation planning fields
        CASE 
            WHEN pda.earliest_af_diagnosis_date IS NOT NULL 
            THEN CURRENT_DATE - pda.earliest_af_diagnosis_date
            ELSE NULL
        END AS days_since_first_af_diagnosis,
        
        CASE 
            WHEN pda.earliest_af_diagnosis_date >= CURRENT_DATE - INTERVAL '1 year' THEN TRUE
            ELSE FALSE
        END AS is_newly_diagnosed_af,
        CASE 
            WHEN pda.earliest_af_diagnosis_date < CURRENT_DATE - INTERVAL '1 year' THEN TRUE
            ELSE FALSE
        END AS is_established_af,
        CASE 
            WHEN pda.earliest_af_diagnosis_date < CURRENT_DATE - INTERVAL '5 years' THEN TRUE
            ELSE FALSE
        END AS is_long_term_af,
        
        -- QOF temporal indicators for stroke prevention
        CASE 
            WHEN pda.latest_af_resolved_date IS NOT NULL 
                AND pda.latest_af_resolved_date >= CURRENT_DATE - INTERVAL '12 months'
            THEN TRUE
            ELSE FALSE
        END AS has_recent_af_resolution,
        
        -- Disease progression indicators
        CASE 
            WHEN pda.total_af_diagnoses > 1 
                AND (pda.latest_af_diagnosis_date - pda.earliest_af_diagnosis_date) > INTERVAL '6 months'
            THEN TRUE
            ELSE FALSE
        END AS has_af_recurrence_codes,
        
        -- Arrays for complete traceability
        pda.all_af_concept_codes,
        pda.all_af_concept_displays
        
    FROM base_observations bo
    LEFT JOIN person_diagnosis_aggregates pda
        ON bo.person_id = pda.person_id
)

SELECT * FROM final_with_derived_fields
ORDER BY person_id, clinical_effective_date 