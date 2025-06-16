{{
    config(
        materialized='table',
        cluster_by=['person_id', 'clinical_effective_date']
    )
}}

/*
All dementia diagnosis observations from clinical records.
Uses QOF dementia cluster IDs:
- DEM_COD: Dementia diagnoses

Clinical Purpose:
- QOF dementia register data collection (presence of DEM_COD)
- Dementia care pathway monitoring
- Cognitive health assessment support
- Memory service referral tracking

Key QOF Requirements:
- Register inclusion: Presence of any dementia diagnosis code (DEM_COD)
- No age restrictions for dementia register
- No resolution codes - dementia is considered permanent condition
- Focus on early detection and ongoing care

Note: Dementia does not have resolved codes as it is considered a permanent condition.
The register is based purely on the presence of diagnostic codes.

Includes ALL persons (active, inactive, deceased) following intermediate layer principles.
Use this model as input for fct_person_dementia_register.sql which applies QOF business rules.
*/

WITH base_observations AS (
    
    SELECT
        obs.observation_id,
        obs.person_id,
        obs.clinical_effective_date,
        obs.concept_code,
        obs.concept_display,
        obs.source_cluster_id,
        
        -- Flag dementia diagnosis codes following QOF definitions
        CASE WHEN obs.source_cluster_id = 'DEM_COD' THEN TRUE ELSE FALSE END AS is_dementia_diagnosis_code
        
    FROM {{ get_observations("'DEM_COD'") }} obs
    WHERE obs.clinical_effective_date IS NOT NULL
),

person_aggregates AS (
    -- Calculate person-level dementia date aggregates for context
    SELECT
        person_id,
        
        -- Dementia diagnosis dates
        MIN(CASE WHEN is_dementia_diagnosis_code THEN clinical_effective_date END) AS earliest_dementia_date,
        MAX(CASE WHEN is_dementia_diagnosis_code THEN clinical_effective_date END) AS latest_dementia_date,
        
        -- Count of dementia diagnoses (may indicate progression or confirmation)
        COUNT(CASE WHEN is_dementia_diagnosis_code THEN 1 END) AS total_dementia_diagnoses,
        
        -- Concept code arrays for traceability
        ARRAY_AGG(DISTINCT CASE WHEN is_dementia_diagnosis_code THEN concept_code END) 
            FILTER (WHERE is_dementia_diagnosis_code) AS all_dementia_concept_codes,
        ARRAY_AGG(DISTINCT CASE WHEN is_dementia_diagnosis_code THEN concept_display END) 
            FILTER (WHERE is_dementia_diagnosis_code) AS all_dementia_concept_displays
            
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
    
    -- Dementia type flags
    bo.is_dementia_diagnosis_code,
    
    -- Person-level aggregate context (for downstream QOF logic)
    pa.earliest_dementia_date,
    pa.latest_dementia_date,
    pa.total_dementia_diagnoses,
    
    -- Dementia observation type determination
    CASE
        WHEN bo.is_dementia_diagnosis_code THEN 'Dementia Diagnosis'
        ELSE 'Unknown'
    END AS dementia_observation_type,
    
    -- QOF register eligibility context (basic - dementia is permanent condition)
    CASE
        WHEN pa.latest_dementia_date IS NOT NULL 
        THEN TRUE
        ELSE FALSE
    END AS has_dementia_diagnosis,
    
    -- Clinical context flags for dementia management
    CASE
        WHEN pa.latest_dementia_date IS NOT NULL AND pa.latest_dementia_date >= CURRENT_DATE - INTERVAL '12 months'
        THEN TRUE
        ELSE FALSE
    END AS has_recent_dementia_diagnosis,
    
    CASE
        WHEN pa.latest_dementia_date IS NOT NULL AND pa.latest_dementia_date >= CURRENT_DATE - INTERVAL '24 months'
        THEN TRUE
        ELSE FALSE
    END AS has_dementia_diagnosis_last_24m,
    
    -- Disease progression indicators
    CASE
        WHEN pa.total_dementia_diagnoses = 1
        THEN TRUE
        ELSE FALSE
    END AS is_single_dementia_diagnosis,
    
    CASE
        WHEN pa.total_dementia_diagnoses > 1
        THEN TRUE
        ELSE FALSE
    END AS has_multiple_dementia_diagnoses,
    
    -- Time since diagnosis calculation (for care planning)
    CASE
        WHEN pa.earliest_dementia_date IS NOT NULL
        THEN DATEDIFF('day', pa.earliest_dementia_date, CURRENT_DATE)
        ELSE NULL
    END AS days_since_first_diagnosis,
    
    -- Clinical staging context (based on diagnostic timing)
    CASE
        WHEN pa.earliest_dementia_date IS NOT NULL AND pa.earliest_dementia_date >= CURRENT_DATE - INTERVAL '1 year'
        THEN TRUE
        ELSE FALSE
    END AS is_newly_diagnosed_dementia,
    
    CASE
        WHEN pa.earliest_dementia_date IS NOT NULL AND pa.earliest_dementia_date < CURRENT_DATE - INTERVAL '1 year'
        THEN TRUE
        ELSE FALSE
    END AS is_established_dementia,
    
    CASE
        WHEN pa.earliest_dementia_date IS NOT NULL AND pa.earliest_dementia_date < CURRENT_DATE - INTERVAL '5 years'
        THEN TRUE
        ELSE FALSE
    END AS is_long_term_dementia,
    
    -- Care pathway indicators
    CASE
        WHEN pa.latest_dementia_date IS NOT NULL 
             AND pa.earliest_dementia_date IS NOT NULL
             AND DATEDIFF('month', pa.earliest_dementia_date, pa.latest_dementia_date) > 6
        THEN TRUE
        ELSE FALSE
    END AS has_dementia_progression_codes,
    
    -- Traceability arrays
    pa.all_dementia_concept_codes,
    pa.all_dementia_concept_displays

FROM base_observations bo
LEFT JOIN person_aggregates pa 
    ON bo.person_id = pa.person_id

ORDER BY person_id, clinical_effective_date, observation_id 