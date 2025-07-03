{{
    config(
        materialized='table',
        cluster_by=['person_id', 'clinical_effective_date'],
        post_hook=[
            "COMMENT ON TABLE {{ this }} IS 'Intermediate: Spirometry Observations - All spirometry test results for COPD diagnosis and monitoring.

Clinical Purpose:
• Tracks spirometry results (FEV1/FVC ratios) for COPD diagnosis and management
• Supports COPD register eligibility and respiratory disease monitoring
• Enables airway obstruction assessment and clinical decision support

Data Granularity:
• One row per spirometry observation
• Includes both raw FEV1/FVC values and pre-coded ‘<0.7’ observations
• Uses FEV1FVC_COD and FEV1FVCL70_COD clusters

Key Features:
• COPD diagnostic threshold assessment (FEV1/FVC <0.7)
• Spirometry validation and clinical interpretation
• COPD severity staging and airway obstruction indicators
• QOF-specific spirometry confirmation flags for register eligibility'"
        ]
    )
}}

/*
All spirometry test results for COPD diagnosis (FEV1/FVC ratios).
Includes both raw FEV1/FVC values and pre-coded "less than 0.7" observations.
Includes ALL persons (active, inactive, deceased) following intermediate layer principles.
Enhanced with analytics-ready fields and legacy structure alignment.
*/

WITH base_observations AS (

    SELECT
        obs.observation_id,
        obs.person_id,
        obs.clinical_effective_date,
        CAST(obs.result_value AS NUMBER(10,3)) AS fev1_fvc_ratio,
        obs.result_unit_display,
        obs.mapped_concept_code AS concept_code,
        obs.mapped_concept_display AS code_description,
        obs.cluster_id AS source_cluster_id,
        obs.result_value AS original_result_value

    FROM ({{ get_observations("'FEV1FVC_COD', 'FEV1FVCL70_COD'") }}) obs
    WHERE obs.clinical_effective_date IS NOT NULL
)

SELECT
    person_id,
    observation_id,
    clinical_effective_date,
    fev1_fvc_ratio,
    result_unit_display,
    original_result_value,
    concept_code,
    code_description,
    source_cluster_id,

    -- Core legacy boolean flag (matching legacy exactly)
    CASE
        WHEN source_cluster_id = 'FEV1FVCL70_COD' THEN TRUE -- Pre-coded as less than 0.7
        WHEN source_cluster_id = 'FEV1FVC_COD' AND fev1_fvc_ratio < 0.7 THEN TRUE -- Raw value less than 0.7
        ELSE FALSE
    END AS is_below_0_7,

    -- Enhanced analytics flags
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
    END AS spirometry_interpretation,

    -- COPD severity staging based on FEV1/FVC ratio
    CASE
        WHEN source_cluster_id = 'FEV1FVCL70_COD' OR fev1_fvc_ratio < 0.7 THEN 'Airway Obstruction'
        WHEN fev1_fvc_ratio >= 0.7 THEN 'Normal'
        ELSE 'Unknown'
    END AS copd_staging,

    -- Analytics-ready COPD risk indicators
    CASE
        WHEN source_cluster_id = 'FEV1FVCL70_COD' OR fev1_fvc_ratio < 0.7 THEN TRUE
        ELSE FALSE
    END AS indicates_copd,

    -- QOF-specific spirometry confirmation flag
    CASE
        WHEN (source_cluster_id = 'FEV1FVCL70_COD' OR fev1_fvc_ratio < 0.7) AND is_valid_spirometry THEN TRUE
        ELSE FALSE
    END AS confirms_copd_spirometry

FROM base_observations
ORDER BY person_id, clinical_effective_date DESC
