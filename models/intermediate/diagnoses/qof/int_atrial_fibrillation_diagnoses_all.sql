{{
    config(
        materialized='table',
        cluster_by=['person_id', 'clinical_effective_date'],
        post_hook=[
            "COMMENT ON TABLE {{ this }} IS 'Intermediate: Atrial Fibrillation Diagnoses All - Complete history of all atrial fibrillation diagnosis and resolution observations for QOF register management.

Clinical Purpose:
• QOF atrial fibrillation register data collection and monitoring
• Stroke risk assessment and anticoagulation therapy management
• Cardiovascular rhythm disorder tracking and care coordination
• Resolution status monitoring for clinical decision support

Data Granularity:
• One row per atrial fibrillation diagnosis or resolution observation
• Uses AFIB_COD (diagnosis) and AFIBRES_COD (resolved) QOF clusters
• Includes all patients regardless of status for comprehensive QOF reporting

Key Features:
• Diagnosis and resolution code classification for register eligibility
• Critical for stroke prevention and anticoagulation protocols
• Supports complex QOF business rules and age restrictions
• Essential input for cardiovascular risk stratification'"
        ]
    )
}}

/*
All atrial fibrillation diagnosis observations from clinical records.
Uses QOF atrial fibrillation cluster IDs:
- AFIB_COD: Atrial fibrillation diagnoses
- AFIBRES_COD: Atrial fibrillation resolved/remission codes

Clinical Purpose:
- QOF atrial fibrillation register data collection
- Stroke risk assessment and anticoagulation monitoring
- Cardiovascular rhythm disorder tracking
- Resolution status monitoring

QOF Context:
AF register includes persons with atrial fibrillation diagnosis codes who have not
been resolved. Complex business rules (age restrictions, resolution logic) applied
in downstream fact models for register inclusion.

Includes ALL persons (active, inactive, deceased) following intermediate layer principles.
This is OBSERVATION-LEVEL data - one row per AF observation.
Use this model as input for fct_person_atrial_fibrillation_register.sql which applies QOF business rules.
*/

SELECT
    obs.observation_id,
    obs.person_id,
    obs.clinical_effective_date,
    obs.mapped_concept_code AS concept_code,
    obs.mapped_concept_display AS concept_display,
    obs.cluster_id AS source_cluster_id,

    -- AF-specific flags (observation-level only)
    CASE WHEN obs.cluster_id = 'AFIB_COD' THEN TRUE ELSE FALSE END AS is_af_diagnosis_code,
    CASE WHEN obs.cluster_id = 'AFIBRES_COD' THEN TRUE ELSE FALSE END AS is_af_resolved_code,

    -- AF observation type determination
    CASE
        WHEN obs.cluster_id = 'AFIB_COD' THEN 'AF Diagnosis'
        WHEN obs.cluster_id = 'AFIBRES_COD' THEN 'AF Resolved'
        ELSE 'Unknown'
    END AS af_observation_type

FROM ({{ get_observations("'AFIB_COD', 'AFIBRES_COD'") }}) obs
WHERE obs.clinical_effective_date IS NOT NULL

ORDER BY person_id, clinical_effective_date, observation_id
