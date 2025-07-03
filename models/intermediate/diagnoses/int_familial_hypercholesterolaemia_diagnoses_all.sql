{{
    config(
        materialized='table',
        cluster_by=['person_id', 'clinical_effective_date'],
        post_hook=[
            "COMMENT ON TABLE {{ this }} IS 'Intermediate: Familial Hypercholesterolaemia Diagnoses All - Complete history of all FH diagnosis observations from clinical records.

Clinical Purpose:
• QOF FH register data collection and monitoring
• Genetic cardiovascular risk assessment for familial hypercholesterolaemia
• Family screening pathway identification and cascade testing
• Lipid management and statin therapy decision support

Data Granularity:
• One row per FH diagnosis observation
• Includes all diagnosis dates for complete clinical history
• Uses FHYP_COD cluster for QOF-compliant diagnosis identification

Key Features:
• Genetic condition tracking with no resolution codes
• Age restrictions (typically ≥20 years for QOF)
• Essential for family screening programmes
• Supports intensive cholesterol management protocols'"
        ]
    )
}}

/*
All familial hypercholesterolaemia (FH) diagnosis observations from clinical records.
Uses QOF familial hypercholesterolaemia cluster ID:
- FHYP_COD: Familial hypercholesterolaemia diagnoses

Clinical Purpose:
- QOF FH register data collection
- Familial hypercholesterolaemia monitoring
- Genetic cardiovascular risk assessment
- Family screening pathway identification

Key QOF Requirements:
- Register inclusion: FH diagnosis (FHYP_COD)
- No resolution codes - FH is a genetic condition
- Age restrictions apply (usually age ≥20 years for QOF)
- Important for statin therapy and family screening

Note: FH does not have resolved codes as it is a genetic cardiovascular condition.
The register tracks diagnosis for family screening and intensive cholesterol management.

Includes ALL persons (active, inactive, deceased) following intermediate layer principles.
This is OBSERVATION-LEVEL data - one row per FH observation.
*/

SELECT
    obs.observation_id,
    obs.person_id,
    obs.clinical_effective_date,
    obs.mapped_concept_code AS concept_code,
    obs.mapped_concept_display AS concept_display,
    obs.cluster_id AS source_cluster_id,

    -- Flag FH diagnosis codes following QOF definitions
    CASE WHEN obs.cluster_id = 'FHYP_COD' THEN TRUE ELSE FALSE END AS is_fh_diagnosis_code

FROM ({{ get_observations("'FHYP_COD'") }}) obs
WHERE obs.clinical_effective_date IS NOT NULL

ORDER BY person_id, clinical_effective_date, observation_id
