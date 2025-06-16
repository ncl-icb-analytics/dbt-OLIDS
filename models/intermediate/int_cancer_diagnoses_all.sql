{{
    config(
        materialized='table',
        cluster_by=['person_id', 'clinical_effective_date']
    )
}}

/*
All cancer diagnosis and care pathway observations from clinical records.
Uses QOF cancer cluster IDs:
- CAN_COD: Cancer diagnoses (excluding non-melanotic skin cancers)
- MDRV_COD: Cancer care reviews  
- CANINVITE_COD: Cancer care review invitations
- CANPCADEC_COD: Cancer patient care adjustment declined
- CANPCAPU_COD: Cancer patient care adjustment unsuitable
- CANPCSUPP_COD: Cancer patient care support information provided

Clinical Purpose:
- QOF cancer register data collection (diagnosis on/after 1 April 2003)
- Cancer care pathway monitoring
- Patient care adjustment tracking
- Support service provision tracking

Key QOF Requirements:
- Register inclusion: Cancer diagnosis (CAN_COD) on/after 1 April 2003
- Excludes non-melanotic skin cancers
- Care pathway review requirements
- Patient care adjustment considerations

Complex cancer care pathway with multiple observation types for comprehensive management.

Includes ALL persons (active, inactive, deceased) following intermediate layer principles.
Use this model as input for fct_person_cancer_register.sql which applies QOF business rules.
*/

WITH base_observations AS (
    
    SELECT
        obs.observation_id,
        obs.person_id,
        obs.clinical_effective_date,
        obs.concept_code,
        obs.concept_display,
        obs.source_cluster_id,
        
        -- Flag different types of cancer codes following QOF definitions
        CASE WHEN obs.source_cluster_id = 'CAN_COD' THEN TRUE ELSE FALSE END AS is_cancer_diagnosis_code,
        CASE WHEN obs.source_cluster_id = 'MDRV_COD' THEN TRUE ELSE FALSE END AS is_cancer_review_code,
        CASE WHEN obs.source_cluster_id = 'CANINVITE_COD' THEN TRUE ELSE FALSE END AS is_cancer_invite_code,
        CASE WHEN obs.source_cluster_id = 'CANPCADEC_COD' THEN TRUE ELSE FALSE END AS is_cancer_pca_decline_code,
        CASE WHEN obs.source_cluster_id = 'CANPCAPU_COD' THEN TRUE ELSE FALSE END AS is_cancer_pca_unsuitable_code,
        CASE WHEN obs.source_cluster_id = 'CANPCSUPP_COD' THEN TRUE ELSE FALSE END AS is_cancer_pca_support_code
        
    FROM {{ get_observations("'CAN_COD', 'MDRV_COD', 'CANINVITE_COD', 'CANPCADEC_COD', 'CANPCAPU_COD', 'CANPCSUPP_COD'") }} obs
    WHERE obs.clinical_effective_date IS NOT NULL
),

person_aggregates AS (
    
    SELECT
        person_id,
        
        -- Cancer diagnosis dates
        MIN(CASE WHEN is_cancer_diagnosis_code THEN clinical_effective_date END) AS earliest_cancer_diagnosis_date,
        MAX(CASE WHEN is_cancer_diagnosis_code THEN clinical_effective_date END) AS latest_cancer_diagnosis_date,
        
        -- Cancer review dates
        MIN(CASE WHEN is_cancer_review_code THEN clinical_effective_date END) AS earliest_cancer_review_date,
        MAX(CASE WHEN is_cancer_review_code THEN clinical_effective_date END) AS latest_cancer_review_date,
        
        -- Cancer invite dates
        MIN(CASE WHEN is_cancer_invite_code THEN clinical_effective_date END) AS earliest_cancer_invite_date,
        MAX(CASE WHEN is_cancer_invite_code THEN clinical_effective_date END) AS latest_cancer_invite_date,
        
        -- Patient care adjustment dates
        MAX(CASE WHEN is_cancer_pca_decline_code THEN clinical_effective_date END) AS latest_pca_decline_date,
        MAX(CASE WHEN is_cancer_pca_unsuitable_code THEN clinical_effective_date END) AS latest_pca_unsuitable_date,
        MAX(CASE WHEN is_cancer_pca_support_code THEN clinical_effective_date END) AS latest_pca_support_date,
        
        -- QOF register eligibility flag (simplified for intermediate layer)
        CASE 
            WHEN MAX(CASE WHEN is_cancer_diagnosis_code THEN clinical_effective_date END) >= DATE '2003-04-01'
            THEN TRUE 
            ELSE FALSE 
        END AS is_eligible_for_cancer_register,
        
        -- Arrays for traceability
        ARRAY_AGG(DISTINCT CASE WHEN is_cancer_diagnosis_code THEN concept_code END) AS all_cancer_diagnosis_codes,
        ARRAY_AGG(DISTINCT CASE WHEN is_cancer_diagnosis_code THEN concept_display END) AS all_cancer_diagnosis_displays,
        ARRAY_AGG(DISTINCT CASE WHEN is_cancer_review_code THEN concept_code END) AS all_cancer_review_codes,
        ARRAY_AGG(DISTINCT CASE WHEN is_cancer_review_code THEN concept_display END) AS all_cancer_review_displays,
        ARRAY_AGG(DISTINCT CASE WHEN is_cancer_invite_code THEN concept_code END) AS all_cancer_invite_codes,
        ARRAY_AGG(DISTINCT CASE WHEN is_cancer_invite_code THEN concept_display END) AS all_cancer_invite_displays
        
    FROM base_observations
    GROUP BY person_id
)

SELECT
    obs.person_id,
    obs.observation_id,
    obs.clinical_effective_date,
    obs.concept_code,
    obs.concept_display,
    obs.source_cluster_id,
    obs.is_cancer_diagnosis_code,
    obs.is_cancer_review_code,
    obs.is_cancer_invite_code,
    obs.is_cancer_pca_decline_code,
    obs.is_cancer_pca_unsuitable_code,
    obs.is_cancer_pca_support_code,
    
    -- Add person-level aggregates
    agg.earliest_cancer_diagnosis_date,
    agg.latest_cancer_diagnosis_date,
    agg.earliest_cancer_review_date,
    agg.latest_cancer_review_date,
    agg.earliest_cancer_invite_date,
    agg.latest_cancer_invite_date,
    agg.latest_pca_decline_date,
    agg.latest_pca_unsuitable_date,
    agg.latest_pca_support_date,
    agg.is_eligible_for_cancer_register,
    agg.all_cancer_diagnosis_codes,
    agg.all_cancer_diagnosis_displays,
    agg.all_cancer_review_codes,
    agg.all_cancer_review_displays,
    agg.all_cancer_invite_codes,
    agg.all_cancer_invite_displays

FROM base_observations obs
JOIN person_aggregates agg ON obs.person_id = agg.person_id

-- Sort for consistent output
ORDER BY obs.person_id, obs.clinical_effective_date DESC 