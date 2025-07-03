{{
    config(
        materialized='table',
        cluster_by=['person_id', 'clinical_effective_date'],
        post_hook=[
            "COMMENT ON TABLE {{ this }} IS 'Intermediate: Gestational Diabetes Diagnoses All - Complete history of all gestational diabetes diagnosis observations from clinical records.

Clinical Purpose:
• QOF gestational diabetes register data collection
• Pregnancy-related diabetes monitoring and risk assessment
• Postpartum diabetes screening pathway identification
• Future diabetes prevention planning and early intervention

Data Granularity:
• One row per gestational diabetes diagnosis observation
• Includes all diagnosis dates across pregnancy episodes
• Uses GESTDIAB_COD cluster for QOF-compliant diagnosis identification

Key Features:
• Pregnancy-specific diabetes condition tracking
• No resolution codes (condition-specific to pregnancy episodes)
• Applies primarily to women of childbearing age
• Critical for postpartum diabetes risk stratification'"
        ]
    )
}}

/*
All gestational diabetes diagnosis observations from clinical records.
Uses QOF gestational diabetes cluster ID:
- GESTDIAB_COD: Gestational diabetes diagnoses

Clinical Purpose:
- QOF gestational diabetes register data collection
- Pregnancy-related diabetes monitoring
- Postpartum diabetes risk assessment
- Future diabetes prevention planning

Key QOF Requirements:
- Register inclusion: Gestational diabetes diagnosis (GESTDIAB_COD)
- No resolution codes - gestational diabetes is condition-specific to pregnancy
- Usually applies to women of childbearing age
- Important for postpartum diabetes screening

Note: Gestational diabetes does not have resolved codes as it is specific to pregnancy episodes.
The register tracks diagnosis history which informs future diabetes risk.

Includes ALL persons (active, inactive, deceased) following intermediate layer principles.
This is OBSERVATION-LEVEL data - one row per gestational diabetes observation.
*/

SELECT
    obs.observation_id,
    obs.person_id,
    obs.clinical_effective_date,
    obs.mapped_concept_code AS concept_code,
    obs.mapped_concept_display AS concept_display,
    obs.cluster_id AS source_cluster_id,

    -- Flag gestational diabetes diagnosis codes following QOF definitions
    CASE WHEN obs.cluster_id = 'GESTDIAB_COD' THEN TRUE ELSE FALSE END AS is_gestational_diabetes_diagnosis_code

FROM ({{ get_observations("'GESTDIAB_COD'") }}) obs
WHERE obs.clinical_effective_date IS NOT NULL

ORDER BY person_id, clinical_effective_date, observation_id
