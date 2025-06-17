{{
    config(
        materialized='table',
        cluster_by=['person_id', 'clinical_effective_date']
    )
}}

/*
All epilepsy diagnosis observations from clinical records.
Uses QOF epilepsy cluster IDs:
- EPIL_COD: Epilepsy diagnoses
- EPILRES_COD: Epilepsy resolved/remission codes

Clinical Purpose:
- QOF epilepsy register data collection (aged 18+, active epilepsy diagnosis + recent medication)
- Epilepsy care pathway monitoring
- Seizure management support
- Medication therapy integration for neurological conditions

Key QOF Requirements:
- Register inclusion: Age ≥18, active epilepsy diagnosis (latest EPIL_COD > latest EPILRES_COD)
- AND recent epilepsy medication order (within last 6 months)
- Neurological condition with specific medication monitoring requirements

Note: This model provides diagnosis codes only. Medication integration is handled 
in the corresponding fact table which joins to intermediate_epilepsy_orders_6m.

Includes ALL persons (active, inactive, deceased) following intermediate layer principles.
Use this model as input for fct_person_epilepsy_register.sql which applies QOF business rules and medication integration.
*/

WITH base_observations AS (
    
    SELECT
        obs.observation_id,
        obs.person_id,
        obs.clinical_effective_date,
        obs.mapped_concept_code AS concept_code,
        obs.mapped_concept_display AS concept_display,
        obs.cluster_id AS source_cluster_id,
        
        -- Flag different types of epilepsy codes following QOF definitions
        CASE WHEN obs.cluster_id = 'EPIL_COD' THEN TRUE ELSE FALSE END AS is_epilepsy_diagnosis_code,
        CASE WHEN obs.cluster_id = 'EPILRES_COD' THEN TRUE ELSE FALSE END AS is_epilepsy_resolved_code
        
    FROM ({{ get_observations("'EPIL_COD', 'EPILRES_COD'") }}) obs
    WHERE obs.clinical_effective_date IS NOT NULL
),

person_aggregates AS (
    -- Calculate person-level epilepsy date aggregates for context
    SELECT
        person_id,
        
        -- Epilepsy diagnosis dates
        MIN(CASE WHEN is_epilepsy_diagnosis_code THEN clinical_effective_date END) AS earliest_epilepsy_date,
        MAX(CASE WHEN is_epilepsy_diagnosis_code THEN clinical_effective_date END) AS latest_epilepsy_date,
        
        -- Resolution dates
        MIN(CASE WHEN is_epilepsy_resolved_code THEN clinical_effective_date END) AS earliest_resolved_date,
        MAX(CASE WHEN is_epilepsy_resolved_code THEN clinical_effective_date END) AS latest_resolved_date,
        
        -- Concept code arrays for traceability
        ARRAY_AGG(DISTINCT CASE WHEN is_epilepsy_diagnosis_code THEN concept_code ELSE NULL END) AS all_epilepsy_concept_codes,
        ARRAY_AGG(DISTINCT CASE WHEN is_epilepsy_diagnosis_code THEN concept_display ELSE NULL END) AS all_epilepsy_concept_displays,
        ARRAY_AGG(DISTINCT CASE WHEN is_epilepsy_resolved_code THEN concept_code ELSE NULL END) AS all_resolved_concept_codes,
        ARRAY_AGG(DISTINCT CASE WHEN is_epilepsy_resolved_code THEN concept_display ELSE NULL END) AS all_resolved_concept_displays
            
    FROM base_observations
    GROUP BY person_id
)

SELECT 
    bo.person_id,
    bo.observation_id,
    bo.clinical_effective_date,
            bo.concept_code,
        bo.concept_display,
        bo.source_cluster_id,
    
    -- Epilepsy type flags
    bo.is_epilepsy_diagnosis_code,
    bo.is_epilepsy_resolved_code,
    
    -- Person-level aggregate context (for downstream QOF logic)
    pa.earliest_epilepsy_date,
    pa.latest_epilepsy_date,
    pa.earliest_resolved_date,
    pa.latest_resolved_date,
    
    -- QOF-specific derived fields
    CASE 
        WHEN pa.latest_resolved_date IS NULL THEN FALSE
        WHEN pa.latest_epilepsy_date > pa.latest_resolved_date THEN FALSE
        ELSE TRUE
    END AS is_epilepsy_currently_resolved,
    
    -- Epilepsy observation type determination
    CASE
        WHEN bo.is_epilepsy_diagnosis_code THEN 'Epilepsy Diagnosis'
        WHEN bo.is_epilepsy_resolved_code THEN 'Epilepsy Resolved'
        ELSE 'Unknown'
    END AS epilepsy_observation_type,
    
    -- QOF register eligibility context (basic - needs age ≥18 filter and medication integration in fact layer)
    CASE
        WHEN pa.latest_epilepsy_date IS NOT NULL 
             AND (pa.latest_resolved_date IS NULL OR pa.latest_epilepsy_date > pa.latest_resolved_date)
        THEN TRUE
        ELSE FALSE
    END AS has_active_epilepsy_diagnosis,
    
    -- Clinical context flags for epilepsy management
    CASE
        WHEN pa.latest_epilepsy_date IS NOT NULL AND pa.latest_epilepsy_date >= CURRENT_DATE - INTERVAL '12 months'
        THEN TRUE
        ELSE FALSE
    END AS has_recent_epilepsy_diagnosis,
    
    CASE
        WHEN pa.latest_epilepsy_date IS NOT NULL AND pa.latest_epilepsy_date >= CURRENT_DATE - INTERVAL '24 months'
        THEN TRUE
        ELSE FALSE
    END AS has_epilepsy_diagnosis_last_24m,
    
    -- Neurological condition management indicators
    CASE
        WHEN pa.latest_epilepsy_date IS NOT NULL AND pa.latest_epilepsy_date >= CURRENT_DATE - INTERVAL '6 months'
        THEN TRUE
        ELSE FALSE
    END AS has_epilepsy_diagnosis_last_6m,
    
    -- Disease pattern indicators
    CASE
        WHEN pa.earliest_epilepsy_date IS NOT NULL 
             AND pa.latest_epilepsy_date IS NOT NULL
             AND pa.latest_epilepsy_date = pa.earliest_epilepsy_date
        THEN TRUE
        ELSE FALSE
    END AS is_single_epilepsy_diagnosis,
    
    CASE
        WHEN pa.earliest_epilepsy_date IS NOT NULL 
             AND pa.latest_epilepsy_date IS NOT NULL
             AND pa.latest_epilepsy_date > pa.earliest_epilepsy_date
        THEN TRUE
        ELSE FALSE
    END AS has_multiple_epilepsy_diagnoses,
    
    -- Time since diagnosis calculation (for medication monitoring)
    CASE
        WHEN pa.earliest_epilepsy_date IS NOT NULL
        THEN DATEDIFF('day', pa.earliest_epilepsy_date, CURRENT_DATE)
        ELSE NULL
    END AS days_since_first_diagnosis,
    
    -- Clinical management context
    CASE
        WHEN pa.earliest_epilepsy_date IS NOT NULL AND pa.earliest_epilepsy_date >= CURRENT_DATE - INTERVAL '2 years'
        THEN TRUE
        ELSE FALSE
    END AS is_newly_diagnosed_epilepsy,
    
    CASE
        WHEN pa.earliest_epilepsy_date IS NOT NULL AND pa.earliest_epilepsy_date < CURRENT_DATE - INTERVAL '2 years'
        THEN TRUE
        ELSE FALSE
    END AS is_established_epilepsy,
    
    -- Resolution pattern analysis
    CASE
        WHEN pa.latest_resolved_date IS NOT NULL AND pa.latest_resolved_date >= CURRENT_DATE - INTERVAL '12 months'
        THEN TRUE
        ELSE FALSE
    END AS has_recent_resolution_code,
    
    -- Seizure control indicators (based on diagnostic patterns)
    CASE
        WHEN pa.latest_epilepsy_date IS NOT NULL 
             AND pa.earliest_epilepsy_date IS NOT NULL
             AND DATEDIFF('month', pa.earliest_epilepsy_date, pa.latest_epilepsy_date) > 12
        THEN TRUE
        ELSE FALSE
    END AS has_long_term_epilepsy_management,
    
    -- Traceability arrays
    pa.all_epilepsy_concept_codes,
    pa.all_epilepsy_concept_displays,
    pa.all_resolved_concept_codes,
    pa.all_resolved_concept_displays

FROM base_observations bo
LEFT JOIN person_aggregates pa 
    ON bo.person_id = pa.person_id

ORDER BY person_id, clinical_effective_date, observation_id 