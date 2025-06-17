{{
    config(
        materialized='table',
        cluster_by=['person_id', 'clinical_effective_date']
    )
}}

/*
All depression diagnosis observations from clinical records.
Uses QOF depression cluster IDs:
- DEPR_COD: Depression diagnoses
- DEPRES_COD: Depression resolved/remission codes
- DEPRVW_COD: Depression review codes
- DEPRINVITE_COD: Depression invite codes
- DEPRPCADEC_COD: Depression PCA decline codes
- DEPRPCAPU_COD: Depression PCA unsuitable codes

Clinical Purpose:
- QOF depression register data collection (aged 18+, unresolved episode since 1 April 2006)
- Depression care pathway monitoring
- Review and invitation tracking
- Resolution status monitoring

Key QOF Requirements:
- Register inclusion: Age ≥18, latest unresolved episode since 1 April 2006
- Recent episode tracking (12, 15, 24 months)
- Review timing validation (10-56 days post-diagnosis)

Includes ALL persons (active, inactive, deceased) following intermediate layer principles.
Use this model as input for fct_person_depression_register.sql which applies QOF business rules.
*/

WITH base_observations AS (
    
    SELECT
        obs.observation_id,
        obs.person_id,
        obs.clinical_effective_date,
        obs.mapped_concept_code AS concept_code,
        obs.mapped_concept_display AS concept_display,
        obs.cluster_id AS source_cluster_id,
        
        -- Flag different types of depression codes following QOF definitions
        CASE WHEN obs.cluster_id = 'DEPR_COD' THEN TRUE ELSE FALSE END AS is_depression_diagnosis_code,
        CASE WHEN obs.cluster_id = 'DEPRES_COD' THEN TRUE ELSE FALSE END AS is_depression_resolved_code,
        CASE WHEN obs.cluster_id = 'DEPRVW_COD' THEN TRUE ELSE FALSE END AS is_depression_review_code,
        CASE WHEN obs.cluster_id = 'DEPRINVITE_COD' THEN TRUE ELSE FALSE END AS is_depression_invite_code,
        CASE WHEN obs.cluster_id = 'DEPRPCADEC_COD' THEN TRUE ELSE FALSE END AS is_depression_pca_decline_code,
        CASE WHEN obs.cluster_id = 'DEPRPCAPU_COD' THEN TRUE ELSE FALSE END AS is_depression_pca_unsuitable_code
        
    FROM ({{ get_observations("'DEPR_COD', 'DEPRES_COD', 'DEPRVW_COD', 'DEPRINVITE_COD', 'DEPRPCADEC_COD', 'DEPRPCAPU_COD'") }}) obs
    WHERE obs.clinical_effective_date IS NOT NULL
),

person_aggregates AS (
    -- Calculate person-level depression date aggregates for context
    SELECT
        person_id,
        
        -- Depression diagnosis dates
        MIN(CASE WHEN is_depression_diagnosis_code THEN clinical_effective_date END) AS earliest_depression_date,
        MAX(CASE WHEN is_depression_diagnosis_code THEN clinical_effective_date END) AS latest_depression_date,
        
        -- Resolution dates
        MIN(CASE WHEN is_depression_resolved_code THEN clinical_effective_date END) AS earliest_resolved_date,
        MAX(CASE WHEN is_depression_resolved_code THEN clinical_effective_date END) AS latest_resolved_date,
        
        -- Review and care pathway dates
        MIN(CASE WHEN is_depression_review_code THEN clinical_effective_date END) AS earliest_review_date,
        MAX(CASE WHEN is_depression_review_code THEN clinical_effective_date END) AS latest_review_date,
        MIN(CASE WHEN is_depression_invite_code THEN clinical_effective_date END) AS earliest_invite_date,
        MAX(CASE WHEN is_depression_invite_code THEN clinical_effective_date END) AS latest_invite_date,
        MIN(CASE WHEN is_depression_pca_decline_code THEN clinical_effective_date END) AS earliest_pca_decline_date,
        MAX(CASE WHEN is_depression_pca_decline_code THEN clinical_effective_date END) AS latest_pca_decline_date,
        MIN(CASE WHEN is_depression_pca_unsuitable_code THEN clinical_effective_date END) AS earliest_pca_unsuitable_date,
        MAX(CASE WHEN is_depression_pca_unsuitable_code THEN clinical_effective_date END) AS latest_pca_unsuitable_date,
        
        -- Concept code arrays for traceability
        ARRAY_AGG(DISTINCT CASE WHEN is_depression_diagnosis_code THEN concept_code ELSE NULL END) AS all_depression_concept_codes,
        ARRAY_AGG(DISTINCT CASE WHEN is_depression_diagnosis_code THEN concept_display ELSE NULL END) AS all_depression_concept_displays,
        ARRAY_AGG(DISTINCT CASE WHEN is_depression_resolved_code THEN concept_code ELSE NULL END) AS all_resolved_concept_codes,
        ARRAY_AGG(DISTINCT CASE WHEN is_depression_review_code THEN concept_code ELSE NULL END) AS all_review_concept_codes,
        ARRAY_AGG(DISTINCT CASE WHEN is_depression_invite_code THEN concept_code ELSE NULL END) AS all_invite_concept_codes,
        ARRAY_AGG(DISTINCT CASE WHEN is_depression_pca_decline_code THEN concept_code ELSE NULL END) AS all_pca_decline_concept_codes,
        ARRAY_AGG(DISTINCT CASE WHEN is_depression_pca_unsuitable_code THEN concept_code ELSE NULL END) AS all_pca_unsuitable_concept_codes
            
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
    
    -- Depression type flags
    bo.is_depression_diagnosis_code,
    bo.is_depression_resolved_code,
    bo.is_depression_review_code,
    bo.is_depression_invite_code,
    bo.is_depression_pca_decline_code,
    bo.is_depression_pca_unsuitable_code,
    
    -- Person-level aggregate context (for downstream QOF logic)
    pa.earliest_depression_date,
    pa.latest_depression_date,
    pa.earliest_resolved_date,
    pa.latest_resolved_date,
    pa.earliest_review_date,
    pa.latest_review_date,
    pa.earliest_invite_date,
    pa.latest_invite_date,
    pa.earliest_pca_decline_date,
    pa.latest_pca_decline_date,
    pa.earliest_pca_unsuitable_date,
    pa.latest_pca_unsuitable_date,
    
    -- QOF-specific derived fields
    CASE 
        WHEN pa.latest_resolved_date IS NULL THEN FALSE
        WHEN pa.latest_depression_date > pa.latest_resolved_date THEN FALSE
        ELSE TRUE
    END AS is_depression_currently_resolved,
    
    -- Depression observation type determination
    CASE
        WHEN bo.is_depression_diagnosis_code THEN 'Depression Diagnosis'
        WHEN bo.is_depression_resolved_code THEN 'Depression Resolved'
        WHEN bo.is_depression_review_code THEN 'Depression Review'
        WHEN bo.is_depression_invite_code THEN 'Depression Invite'
        WHEN bo.is_depression_pca_decline_code THEN 'Depression PCA Decline'
        WHEN bo.is_depression_pca_unsuitable_code THEN 'Depression PCA Unsuitable'
        ELSE 'Unknown'
    END AS depression_observation_type,
    
    -- QOF register eligibility context (basic - needs age and date filters in fact layer)
    CASE
        WHEN pa.latest_depression_date IS NOT NULL 
             AND pa.latest_depression_date >= '2006-04-01'
             AND (pa.latest_resolved_date IS NULL OR pa.latest_depression_date > pa.latest_resolved_date)
        THEN TRUE
        ELSE FALSE
    END AS has_potential_qof_depression,
    
    -- QOF temporal flags for recent episodes
    CASE WHEN pa.latest_depression_date >= CURRENT_DATE - INTERVAL '12 months' THEN TRUE ELSE FALSE END AS has_episode_last_12m,
    CASE WHEN pa.latest_depression_date >= CURRENT_DATE - INTERVAL '15 months' THEN TRUE ELSE FALSE END AS has_episode_last_15m,
    CASE WHEN pa.latest_depression_date >= CURRENT_DATE - INTERVAL '24 months' THEN TRUE ELSE FALSE END AS has_episode_last_24m,
    
    -- Review timing validation (QOF requires reviews 10-56 days post-diagnosis)
    CASE 
        WHEN bo.is_depression_review_code 
             AND pa.latest_depression_date IS NOT NULL
             AND bo.clinical_effective_date BETWEEN 
                 pa.latest_depression_date + INTERVAL '10 days' AND 
                 pa.latest_depression_date + INTERVAL '56 days'
        THEN TRUE 
        ELSE FALSE 
    END AS is_valid_qof_review_timing,
    
    -- Traceability arrays
    pa.all_depression_concept_codes,
    pa.all_depression_concept_displays,
    pa.all_resolved_concept_codes,
    pa.all_review_concept_codes,
    pa.all_invite_concept_codes,
    pa.all_pca_decline_concept_codes,
    pa.all_pca_unsuitable_concept_codes

FROM base_observations bo
LEFT JOIN person_aggregates pa 
    ON bo.person_id = pa.person_id

ORDER BY person_id, clinical_effective_date, observation_id 