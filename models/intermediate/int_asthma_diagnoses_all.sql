{{
    config(
        materialized='table',
        cluster_by=['person_id', 'clinical_effective_date']
    )
}}

/*
All asthma diagnosis observations from clinical records.
Uses QOF asthma cluster IDs:
- AST_COD: Asthma diagnoses
- ASTRES_COD: Asthma resolved/remission codes

Clinical Purpose:
- QOF asthma register data collection (aged 6+, active asthma diagnosis + recent medication)
- Asthma care pathway monitoring
- Resolution status tracking
- Medication therapy integration support

Key QOF Requirements:
- Register inclusion: Age ≥6, active asthma diagnosis (latest AST_COD > latest ASTRES_COD)
- AND recent asthma medication order (within last 12 months)
- Pediatric and adult asthma management differentiation

Note: This model provides diagnosis codes only. Medication integration is handled 
in the corresponding fact table which joins to intermediate_asthma_orders_12m.

Includes ALL persons (active, inactive, deceased) following intermediate layer principles.
Use this model as input for fct_person_asthma_register.sql which applies QOF business rules and medication integration.
*/

WITH base_observations AS (
    
    SELECT
        obs.observation_id,
        obs.person_id,
        obs.clinical_effective_date,
        obs.mapped_concept_code AS concept_code,
        obs.mapped_concept_display AS concept_display,
        obs.cluster_id AS source_cluster_id,
        
        -- Flag different types of asthma codes following QOF definitions
        CASE WHEN obs.cluster_id = 'AST_COD' THEN TRUE ELSE FALSE END AS is_asthma_diagnosis_code,
        CASE WHEN obs.cluster_id = 'ASTRES_COD' THEN TRUE ELSE FALSE END AS is_asthma_resolved_code
        
    FROM ({{ get_observations("'AST_COD', 'ASTRES_COD'") }}) obs
    WHERE obs.clinical_effective_date IS NOT NULL
),

person_aggregates AS (
    -- Calculate person-level asthma date aggregates for context
    SELECT
        person_id,
        
        -- Asthma diagnosis dates
        MIN(CASE WHEN is_asthma_diagnosis_code THEN clinical_effective_date END) AS earliest_asthma_date,
        MAX(CASE WHEN is_asthma_diagnosis_code THEN clinical_effective_date END) AS latest_asthma_date,
        
        -- Resolution dates
        MIN(CASE WHEN is_asthma_resolved_code THEN clinical_effective_date END) AS earliest_resolved_date,
        MAX(CASE WHEN is_asthma_resolved_code THEN clinical_effective_date END) AS latest_resolved_date,
        
        -- Concept code arrays for traceability (using conditional aggregation for Snowflake compatibility)
        ARRAY_AGG(DISTINCT CASE WHEN is_asthma_diagnosis_code THEN concept_code ELSE NULL END) AS all_asthma_concept_codes,
        ARRAY_AGG(DISTINCT CASE WHEN is_asthma_diagnosis_code THEN concept_display ELSE NULL END) AS all_asthma_concept_displays,
        ARRAY_AGG(DISTINCT CASE WHEN is_asthma_resolved_code THEN concept_code ELSE NULL END) AS all_resolved_concept_codes,
        ARRAY_AGG(DISTINCT CASE WHEN is_asthma_resolved_code THEN concept_display ELSE NULL END) AS all_resolved_concept_displays
            
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
    
    -- Asthma type flags
    bo.is_asthma_diagnosis_code,
    bo.is_asthma_resolved_code,
    
    -- Person-level aggregate context (for downstream QOF logic)
    pa.earliest_asthma_date,
    pa.latest_asthma_date,
    pa.earliest_resolved_date,
    pa.latest_resolved_date,
    
    -- QOF-specific derived fields
    CASE 
        WHEN pa.latest_resolved_date IS NULL THEN FALSE
        WHEN pa.latest_asthma_date > pa.latest_resolved_date THEN FALSE
        ELSE TRUE
    END AS is_asthma_currently_resolved,
    
    -- Asthma observation type determination
    CASE
        WHEN bo.is_asthma_diagnosis_code THEN 'Asthma Diagnosis'
        WHEN bo.is_asthma_resolved_code THEN 'Asthma Resolved'
        ELSE 'Unknown'
    END AS asthma_observation_type,
    
    -- QOF register eligibility context (basic - needs age ≥6 filter and medication integration in fact layer)
    CASE
        WHEN pa.latest_asthma_date IS NOT NULL 
             AND (pa.latest_resolved_date IS NULL OR pa.latest_asthma_date > pa.latest_resolved_date)
        THEN TRUE
        ELSE FALSE
    END AS has_active_asthma_diagnosis,
    
    -- Clinical context flags for asthma management
    CASE
        WHEN pa.latest_asthma_date IS NOT NULL AND pa.latest_asthma_date >= CURRENT_DATE - INTERVAL '12 months'
        THEN TRUE
        ELSE FALSE
    END AS has_recent_asthma_diagnosis,
    
    CASE
        WHEN pa.latest_asthma_date IS NOT NULL AND pa.latest_asthma_date >= CURRENT_DATE - INTERVAL '24 months'
        THEN TRUE
        ELSE FALSE
    END AS has_asthma_diagnosis_last_24m,
    
    -- Age group context for downstream pediatric vs adult logic (needs age dimension join in fact layer)
    CASE
        WHEN pa.latest_asthma_date IS NOT NULL AND pa.latest_asthma_date >= CURRENT_DATE - INTERVAL '5 years'
        THEN TRUE
        ELSE FALSE
    END AS has_asthma_diagnosis_last_5y,
    
    -- Clinical severity indicators (based on diagnostic timing)
    CASE
        WHEN pa.earliest_asthma_date IS NOT NULL 
             AND pa.latest_asthma_date IS NOT NULL
             AND pa.latest_asthma_date = pa.earliest_asthma_date
        THEN TRUE
        ELSE FALSE
    END AS is_single_asthma_diagnosis,
    
    CASE
        WHEN pa.earliest_asthma_date IS NOT NULL 
             AND pa.latest_asthma_date IS NOT NULL
             AND pa.latest_asthma_date > pa.earliest_asthma_date
        THEN TRUE
        ELSE FALSE
    END AS has_multiple_asthma_diagnoses,
    
    -- Time since diagnosis calculation (for downstream use)
    CASE
        WHEN pa.earliest_asthma_date IS NOT NULL
        THEN DATEDIFF('day', pa.earliest_asthma_date, CURRENT_DATE)
        ELSE NULL
    END AS days_since_first_diagnosis,
    
    -- Traceability arrays
    pa.all_asthma_concept_codes,
    pa.all_asthma_concept_displays,
    pa.all_resolved_concept_codes,
    pa.all_resolved_concept_displays

FROM base_observations bo
LEFT JOIN person_aggregates pa 
    ON bo.person_id = pa.person_id

ORDER BY person_id, clinical_effective_date, observation_id 