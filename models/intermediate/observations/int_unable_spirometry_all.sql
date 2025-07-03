{{
    config(
        materialized='table',
        cluster_by=['person_id', 'clinical_effective_date'],
        post_hook=[
            "COMMENT ON TABLE {{ this }} IS 'Intermediate: Unable Spirometry Observations - All records where spirometry testing was unsuitable or contraindicated.

Clinical Purpose:
• Tracks spirometry contraindications for COPD register confirmation requirements
• Supports alternative pathway for COPD register inclusion when spirometry cannot be performed
• Enables documentation of clinical assessment limitations and patient factors

Data Granularity:
• One row per unable spirometry observation
• Includes all patients regardless of status (active/inactive/deceased)
• Uses SPIRPU_COD cluster for spirometry unsuitability identification

Key Features:
• Comprehensive documentation of spirometry contraindications
• Alternative evidence pathway for COPD register eligibility
• Clinical assessment limitation tracking for quality assurance
• Essential for COPD register spirometry validation processes'"
        ]
    )
}}

/*
All unable-to-have-spirometry observations from clinical records.
Uses QOF cluster ID SPIRPU_COD for patients where spirometry is unsuitable.

Clinical Purpose:
- COPD register spirometry confirmation requirements (post-April 2023)
- Alternative pathway for COPD register inclusion when spirometry cannot be performed
- Documentation of contraindications or patient inability

Includes ALL persons (active, inactive, deceased) following intermediate layer principles.
One row per unable spirometry observation.
Use this model as input for COPD register spirometry validation.
*/

SELECT
    obs.observation_id,
    obs.person_id,
    obs.clinical_effective_date,
    obs.mapped_concept_code AS concept_code,
    obs.mapped_concept_display AS concept_display,
    obs.cluster_id AS source_cluster_id,

    -- Unable spirometry-specific flags (observation-level only)
    TRUE AS is_unable_spirometry_record,

    -- Classification of this specific observation
    'Unable to Perform Spirometry' AS spirometry_observation_type

FROM ({{ get_observations("'SPIRPU_COD'") }}) obs
WHERE obs.clinical_effective_date IS NOT NULL

ORDER BY person_id, clinical_effective_date DESC
