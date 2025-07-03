{{
    config(
        materialized='table',
        cluster_by=['person_id', 'clinical_effective_date'],
        post_hook=[
            "COMMENT ON TABLE {{ this }} IS 'Intermediate: Asthma Diagnoses - All recorded asthma diagnosis and resolution codes for QOF register management.

Clinical Purpose:
• Tracks asthma diagnoses for QOF asthma register inclusion and respiratory care pathways
• Supports asthma management programmes and clinical monitoring
• Enables childhood and adult asthma care coordination and quality improvement

Data Granularity:
• One row per asthma diagnosis or resolution code
• Uses AST_COD (diagnosis) and ASTRES_COD (resolved) QOF clusters
• Includes all patients regardless of status for comprehensive QOF reporting

Key Features:
• Diagnosis and resolution code classification for register eligibility
• QOF-compliant asthma register logic with resolution tracking
• Age and clinical context considerations for downstream processing
• Essential input for QOF asthma register and respiratory care analytics'"
        ]
    )
}}

/*
All asthma diagnoses from clinical records.
Uses QOF cluster IDs AST_COD (diagnosis) and ASTRES_COD (resolved).

Clinical Purpose:
- Asthma register inclusion for QOF respiratory disease management
- Respiratory care pathway identification and monitoring
- Childhood and adult asthma management tracking

QOF Context:
Asthma register includes persons with asthma diagnosis codes who have not
been resolved. Resolution logic applied in downstream fact models.
Age restrictions (typically 6+ years) applied in fact layer.

Includes ALL persons (active, inactive, deceased) following intermediate layer principles.
Use this model as input for asthma register and respiratory care models.
*/

SELECT
    obs.observation_id,
    obs.person_id,
    obs.clinical_effective_date,
    obs.mapped_concept_code AS concept_code,
    obs.mapped_concept_display AS concept_display,
    obs.cluster_id AS source_cluster_id,

    -- Asthma-specific flags (observation-level only)
    CASE WHEN obs.cluster_id = 'AST_COD' THEN TRUE ELSE FALSE END AS is_asthma_diagnosis_code,
    CASE WHEN obs.cluster_id = 'ASTRES_COD' THEN TRUE ELSE FALSE END AS is_asthma_resolved_code

FROM ({{ get_observations("'AST_COD', 'ASTRES_COD'") }}) obs
WHERE obs.clinical_effective_date IS NOT NULL

ORDER BY person_id, clinical_effective_date, observation_id
