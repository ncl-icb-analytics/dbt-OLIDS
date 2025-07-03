{{
    config(
        materialized='table',
        cluster_by=['person_id', 'clinical_effective_date'],
        post_hook=[
            "COMMENT ON TABLE {{ this }} IS 'Intermediate: Osteoporosis Diagnoses All - Complete history of all osteoporosis diagnosis observations for QOF register management.

Clinical Purpose:
• QOF osteoporosis register data collection and monitoring
• Bone health assessment and fracture risk stratification
• Osteoporosis care pathway coordination and treatment monitoring
• Integration with DXA scan results for comprehensive bone health evaluation

Data Granularity:
• One row per osteoporosis diagnosis observation
• Uses OSTEO_COD cluster for QOF-compliant osteoporosis identification
• Includes all patients regardless of status for comprehensive QOF reporting

Key Features:
• Clinical osteoporosis diagnosis tracking
• Integration with DXA scan confirmation logic (handled in fact layer)
• Critical for bone health preservation and fracture prevention
• Essential input for osteoporosis care quality improvement initiatives'"
        ]
    )
}}

/*
All osteoporosis diagnosis observations from clinical records.
Uses QOF osteoporosis cluster ID:
- OSTEO_COD: Osteoporosis diagnoses

Clinical Purpose:
- QOF osteoporosis register data collection
- Bone health assessment
- Osteoporosis diagnosis tracking

Key QOF Requirements:
- Register inclusion: Osteoporosis diagnosis (OSTEO_COD) OR DXA confirmation
- DXA confirmation handled via separate int_dxa_scans_all model
- Combined logic applied in fact layer

Note: DXA scans and T-scores are handled in separate int_dxa_scans_all model.
The register logic combines clinical diagnosis with DXA confirmation in the fact layer.

Includes ALL persons (active, inactive, deceased) following intermediate layer principles.
This is OBSERVATION-LEVEL data - one row per osteoporosis observation.
Use this model as input for fct_person_osteoporosis_register.sql which applies QOF business rules.
*/

SELECT
    obs.observation_id,
    obs.person_id,
    obs.clinical_effective_date,
    obs.mapped_concept_code AS concept_code,
    obs.mapped_concept_display AS concept_display,
    obs.cluster_id AS source_cluster_id,

    -- Osteoporosis-specific flags (observation-level only)
    CASE WHEN obs.cluster_id = 'OSTEO_COD' THEN TRUE ELSE FALSE END AS is_osteoporosis_diagnosis_code,

    -- Observation type determination
    CASE
        WHEN obs.cluster_id = 'OSTEO_COD' THEN 'Osteoporosis Diagnosis'
        ELSE 'Unknown'
    END AS osteoporosis_observation_type

FROM ({{ get_observations("'OSTEO_COD'") }}) obs
WHERE obs.clinical_effective_date IS NOT NULL

ORDER BY person_id, clinical_effective_date, observation_id
