{{
    config(
        materialized='table',
        cluster_by=['person_id', 'clinical_effective_date']
    )
}}

/*
All spirometry test results for COPD diagnosis (FEV1/FVC ratios).
Includes both raw FEV1/FVC values and pre-coded "less than 0.7" observations.
Includes ALL persons (active, inactive, deceased) following intermediate layer principles.
*/

WITH base_observations AS (
    
    SELECT
        obs.observation_id,
        obs.person_id,
        obs.clinical_effective_date,
        CAST(obs.result_value AS NUMBER(10,3)) AS fev1_fvc_ratio,
        obs.concept_code,
        obs.concept_display,
        obs.source_cluster_id,
        obs.result_value AS original_result_value
        
    FROM {{ get_observations("'FEV1FVC_COD', 'FEV1FVCL70_COD'") }} obs
    WHERE obs.clinical_effective_date IS NOT NULL
)

SELECT
    person_id,
    observation_id,
    clinical_effective_date,
    fev1_fvc_ratio,
    original_result_value,
    concept_code,
    concept_display,
    source_cluster_id,
    
    -- Determine if ratio indicates COPD (below 0.7)
    CASE
        WHEN source_cluster_id = 'FEV1FVCL70_COD' THEN TRUE -- Pre-coded as less than 0.7
        WHEN source_cluster_id = 'FEV1FVC_COD' AND fev1_fvc_ratio < 0.7 THEN TRUE -- Raw value less than 0.7
        ELSE FALSE
    END AS is_below_0_7,
    
    -- Validate spirometry reading
    CASE 
        WHEN source_cluster_id = 'FEV1FVCL70_COD' THEN TRUE -- Pre-coded values are valid
        WHEN source_cluster_id = 'FEV1FVC_COD' AND fev1_fvc_ratio BETWEEN 0.1 AND 2.0 THEN TRUE -- Valid ratio range
        ELSE FALSE
    END AS is_valid_spirometry,
    
    -- Clinical interpretation
    CASE
        WHEN source_cluster_id = 'FEV1FVCL70_COD' THEN 'COPD Indicated (Coded <0.7)'
        WHEN source_cluster_id = 'FEV1FVC_COD' AND fev1_fvc_ratio < 0.7 THEN 'COPD Indicated (Measured <0.7)'
        WHEN source_cluster_id = 'FEV1FVC_COD' AND fev1_fvc_ratio >= 0.7 THEN 'Normal (≥0.7)'
        ELSE 'Invalid'
    END AS spirometry_interpretation

FROM base_observations
ORDER BY person_id, clinical_effective_date DESC 