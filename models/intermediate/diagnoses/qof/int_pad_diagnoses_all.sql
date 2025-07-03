{{
    config(
        materialized='table',
        cluster_by=['person_id', 'clinical_effective_date'],
        post_hook=[
            "COMMENT ON TABLE {{ this }} IS 'Intermediate: PAD Diagnoses All - Complete history of all peripheral arterial disease diagnosis observations for QOF register management.

Clinical Purpose:
• QOF peripheral arterial disease register data collection and monitoring
• Cardiovascular risk stratification and secondary prevention pathway coordination
• PAD care pathway management and treatment optimisation
• Long-term cardiovascular outcome tracking and intervention support

Data Granularity:
• One row per peripheral arterial disease diagnosis observation
• Uses PAD_COD cluster for QOF-compliant PAD diagnosis identification
• Includes all patients regardless of status for comprehensive QOF reporting

Key Features:
• Lifelong condition register for cardiovascular secondary prevention
• Simple diagnosis-only pattern with no resolution codes
• Critical for cardiovascular risk management and prevention protocols
• Essential input for PAD care quality improvement initiatives'"
        ]
    )
}}

/*
All peripheral arterial disease (PAD) diagnoses from clinical records.
Uses QOF cluster ID PAD_COD for all forms of PAD diagnosis.

Clinical Purpose:
- PAD register inclusion for QOF cardiovascular disease management
- Cardiovascular risk stratification and monitoring
- Secondary prevention pathway identification

QOF Context:
PAD register follows simple diagnosis-only pattern - any PAD diagnosis code
qualifies for register inclusion. No resolution codes or complex criteria.
This is a lifelong condition register for cardiovascular secondary prevention.

Includes ALL persons (active, inactive, deceased) following intermediate layer principles.
This is OBSERVATION-LEVEL data - one row per PAD observation.
Use this model as input for fct_person_pad_register.sql which applies QOF business rules.
*/

SELECT
    obs.observation_id,
    obs.person_id,
    obs.clinical_effective_date,
    obs.mapped_concept_code AS concept_code,
    obs.mapped_concept_display AS concept_display,
    obs.cluster_id AS source_cluster_id,

    -- PAD-specific flags (observation-level only)
    CASE WHEN obs.cluster_id = 'PAD_COD' THEN TRUE ELSE FALSE END AS is_pad_diagnosis_code,

    -- Observation type determination
    CASE
        WHEN obs.cluster_id = 'PAD_COD' THEN 'PAD Diagnosis'
        ELSE 'Unknown'
    END AS pad_observation_type

FROM ({{ get_observations("'PAD_COD'") }}) obs
WHERE obs.clinical_effective_date IS NOT NULL

ORDER BY person_id, clinical_effective_date, observation_id
