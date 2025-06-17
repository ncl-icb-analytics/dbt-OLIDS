{{
    config(
        materialized='table',
        cluster_by=['person_id', 'clinical_effective_date']
    )
}}

/*
All serious mental illness diagnosis observations from clinical records.
Uses QOF mental health cluster IDs:
- MH_COD: Mental health diagnoses
- MHREM_COD: Mental health remission codes

Clinical Purpose:
- QOF SMI register data collection (mental health diagnosis not in remission OR recent lithium therapy)
- Mental health care pathway monitoring
- Treatment response tracking
- Lithium therapy integration support

Key QOF Requirements:
- Register inclusion: Mental health diagnosis (MH_COD) not in remission (latest MH_COD > latest MHREM_COD)
- OR recent lithium therapy (handled separately in medication models)
- No age restrictions for SMI register
- Requires integration with lithium medication orders for complete register

Note: This model provides diagnosis codes only. Lithium therapy integration is handled 
in the corresponding fact table which joins to int_lithium_medications_all.

Includes ALL persons following intermediate layer principles.
Use as input for fct_person_smi_register.sql which applies QOF business rules and medication integration.
*/

WITH base_observations AS (
    
    SELECT
        obs.observation_id,
        obs.person_id,
        obs.clinical_effective_date,
        obs.mapped_concept_code AS concept_code,
        obs.mapped_concept_display AS concept_display,
        obs.cluster_id AS source_cluster_id,
        
        -- Flag different types of mental health codes following QOF definitions
        CASE WHEN obs.cluster_id AS source_cluster_id = 'MH_COD' THEN TRUE ELSE FALSE END AS is_mental_health_diagnosis_code,
        CASE WHEN obs.cluster_id AS source_cluster_id = 'MHREM_COD' THEN TRUE ELSE FALSE END AS is_mental_health_remission_code
        
    FROM ({{ get_observations("'MH_COD', 'MHREM_COD'") }}) obs
    WHERE obs.clinical_effective_date IS NOT NULL
),

person_diagnosis_aggregates AS (
    
    SELECT
        person_id,
        
        -- Mental health diagnosis dates (MH_COD)
        MIN(CASE WHEN is_mental_health_diagnosis_code THEN clinical_effective_date END) AS earliest_mh_diagnosis_date,
        MAX(CASE WHEN is_mental_health_diagnosis_code THEN clinical_effective_date END) AS latest_mh_diagnosis_date,
        COUNT(CASE WHEN is_mental_health_diagnosis_code THEN 1 END) AS total_mh_diagnoses,
        
        -- Mental health remission dates (MHREM_COD)
        MIN(CASE WHEN is_mental_health_remission_code THEN clinical_effective_date END) AS earliest_remission_date,
        MAX(CASE WHEN is_mental_health_remission_code THEN clinical_effective_date END) AS latest_remission_date,
        COUNT(CASE WHEN is_mental_health_remission_code THEN 1 END) AS total_remission_codes,
        
        -- Arrays for traceability
        ARRAY_AGG(DISTINCT CASE WHEN is_mental_health_diagnosis_code THEN concept_code END) 
            WITHIN GROUP (ORDER BY CASE WHEN is_mental_health_diagnosis_code THEN concept_code END) 
            AS all_mh_diagnosis_concept_codes,
        ARRAY_AGG(DISTINCT CASE WHEN is_mental_health_diagnosis_code THEN concept_display END) 
            WITHIN GROUP (ORDER BY CASE WHEN is_mental_health_diagnosis_code THEN concept_display END) 
            AS all_mh_diagnosis_concept_displays,
        ARRAY_AGG(DISTINCT CASE WHEN is_mental_health_remission_code THEN concept_code END) 
            WITHIN GROUP (ORDER BY CASE WHEN is_mental_health_remission_code THEN concept_code END) 
            AS all_remission_concept_codes,
        ARRAY_AGG(DISTINCT CASE WHEN is_mental_health_remission_code THEN concept_display END) 
            WITHIN GROUP (ORDER BY CASE WHEN is_mental_health_remission_code THEN concept_display END) 
            AS all_remission_concept_displays
            
    FROM base_observations
    GROUP BY person_id
),

final_with_derived_fields AS (
    
    SELECT
        bo.person_id,
        bo.observation_id,
        bo.clinical_effective_date,
        bo.mapped_concept_code AS concept_code,
        bo.mapped_concept_display AS concept_display,
        bo.cluster_id AS source_cluster_id,
        bo.is_mental_health_diagnosis_code,
        bo.is_mental_health_remission_code,
        
        -- Person-level aggregates from window function
        pda.earliest_mh_diagnosis_date,
        pda.latest_mh_diagnosis_date,
        pda.earliest_remission_date,
        pda.latest_remission_date,
        pda.total_mh_diagnoses,
        pda.total_remission_codes,
        
        -- QOF-critical derived fields
        CASE 
            WHEN pda.latest_remission_date IS NULL THEN FALSE
            WHEN pda.latest_mh_diagnosis_date IS NULL THEN TRUE  -- Only remission codes, so technically in remission
            WHEN pda.latest_remission_date > pda.latest_mh_diagnosis_date THEN TRUE
            ELSE FALSE
        END AS is_mental_health_currently_in_remission,
        
        -- Classification of this specific observation
        CASE 
            WHEN bo.is_mental_health_diagnosis_code THEN 'Mental Health Diagnosis'
            WHEN bo.is_mental_health_remission_code THEN 'Mental Health Remission'
            ELSE 'Unknown'
        END AS mental_health_observation_type,
        
        -- QOF context fields
        pda.latest_mh_diagnosis_date IS NOT NULL AS has_mental_health_diagnosis,
        pda.latest_remission_date IS NOT NULL AS has_mental_health_remission_code,
        
        -- Clinical flags for care planning
        CASE 
            WHEN pda.latest_mh_diagnosis_date >= CURRENT_DATE - INTERVAL '12 months' THEN TRUE
            ELSE FALSE
        END AS has_recent_mh_diagnosis,
        CASE 
            WHEN pda.latest_mh_diagnosis_date >= CURRENT_DATE - INTERVAL '24 months' THEN TRUE
            ELSE FALSE
        END AS has_mh_diagnosis_last_24m,
        CASE 
            WHEN pda.latest_remission_date >= CURRENT_DATE - INTERVAL '12 months' THEN TRUE
            ELSE FALSE
        END AS has_recent_remission_code,
        
        -- Disease management indicators
        CASE WHEN pda.total_mh_diagnoses = 1 THEN TRUE ELSE FALSE END AS is_single_mh_diagnosis,
        CASE WHEN pda.total_mh_diagnoses > 1 THEN TRUE ELSE FALSE END AS has_multiple_mh_diagnoses,
        CASE WHEN pda.total_remission_codes > 0 THEN TRUE ELSE FALSE END AS has_any_remission_codes,
        
        -- Care planning fields
        CASE 
            WHEN pda.earliest_mh_diagnosis_date IS NOT NULL 
            THEN CURRENT_DATE - pda.earliest_mh_diagnosis_date
            ELSE NULL
        END AS days_since_first_mh_diagnosis,
        
        CASE 
            WHEN pda.earliest_mh_diagnosis_date >= CURRENT_DATE - INTERVAL '2 years' THEN TRUE
            ELSE FALSE
        END AS is_newly_diagnosed_mh,
        CASE 
            WHEN pda.earliest_mh_diagnosis_date < CURRENT_DATE - INTERVAL '2 years' THEN TRUE
            ELSE FALSE
        END AS is_established_mh,
        
        -- Complex care indicators
        CASE 
            WHEN pda.total_mh_diagnoses > 1 
                AND (pda.latest_mh_diagnosis_date - pda.earliest_mh_diagnosis_date) > INTERVAL '6 months'
            THEN TRUE
            ELSE FALSE
        END AS has_long_term_mh_management,
        
        -- Arrays for complete traceability
        pda.all_mh_diagnosis_concept_codes,
        pda.all_mh_diagnosis_concept_displays,
        pda.all_remission_concept_codes,
        pda.all_remission_concept_displays
        
    FROM base_observations bo
    LEFT JOIN person_diagnosis_aggregates pda
        ON bo.person_id = pda.person_id
)

SELECT * FROM final_with_derived_fields
ORDER BY person_id, clinical_effective_date 