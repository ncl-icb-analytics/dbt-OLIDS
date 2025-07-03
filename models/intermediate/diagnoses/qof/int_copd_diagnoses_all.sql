{{
    config(
        materialized='table',
        cluster_by=['person_id', 'clinical_effective_date'],
        post_hook=[
            "COMMENT ON TABLE {{ this }} IS 'Intermediate: COPD Diagnoses All - Complete history of all COPD diagnosis and resolution observations for QOF register management.

Clinical Purpose:
• QOF COPD register data collection and monitoring
• Respiratory function assessment and spirometry confirmation requirements
• COPD management pathway coordination and clinical decision support
• Resolution status tracking for improved respiratory outcomes

Data Granularity:
• One row per COPD diagnosis or resolution observation
• Uses COPD_COD (diagnosis) and COPDRES_COD (resolved) QOF clusters
• Includes all patients regardless of status for comprehensive QOF reporting

Key Features:
• Post-April 2023 spirometry confirmation requirements (FEV1/FVC <0.7)
• Integration with unable-to-have-spirometry status tracking
• Critical for respiratory care pathway optimisation
• Essential input for COPD quality improvement initiatives'"
        ]
    )
}}

/*
All COPD diagnosis observations from clinical records.
Uses QOF COPD cluster IDs:
- COPD_COD: COPD diagnoses
- COPDRES_COD: COPD resolved/remission codes

Clinical Purpose:
- QOF COPD register data collection
- COPD spirometry confirmation requirements (post-April 2023)
- Respiratory management monitoring
- Resolution status tracking

Key QOF Requirements:
- Pre-April 2023: Diagnosis alone sufficient for register
- Post-April 2023: Requires spirometry confirmation (FEV1/FVC <0.7) OR unable-to-have-spirometry status

Includes ALL persons (active, inactive, deceased) following intermediate layer principles.
This is OBSERVATION-LEVEL data - one row per COPD observation.
Use this model as input for fct_person_copd_register.sql which applies QOF business rules and spirometry requirements.
*/

SELECT
    obs.observation_id,
    obs.person_id,
    obs.clinical_effective_date,
    obs.mapped_concept_code AS concept_code,
    obs.mapped_concept_display AS concept_display,
    obs.cluster_id AS source_cluster_id,

    -- COPD-specific flags (observation-level only)
    CASE WHEN obs.cluster_id = 'COPD_COD' THEN TRUE ELSE FALSE END AS is_copd_diagnosis_code,
    CASE WHEN obs.cluster_id = 'COPDRES_COD' THEN TRUE ELSE FALSE END AS is_copd_resolved_code,

    -- COPD observation type determination
    CASE
        WHEN obs.cluster_id = 'COPD_COD' THEN 'COPD Diagnosis'
        WHEN obs.cluster_id = 'COPDRES_COD' THEN 'COPD Resolved'
        ELSE 'Unknown'
    END AS copd_observation_type

FROM ({{ get_observations("'COPD_COD', 'COPDRES_COD'") }}) obs
WHERE obs.clinical_effective_date IS NOT NULL

ORDER BY person_id, clinical_effective_date, observation_id
