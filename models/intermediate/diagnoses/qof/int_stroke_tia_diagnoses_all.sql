{{
    config(
        materialized='table',
        cluster_by=['person_id', 'clinical_effective_date'],
        post_hook=[
            "COMMENT ON TABLE {{ this }} IS 'Intermediate: Stroke TIA Diagnoses All - Complete history of all stroke and TIA diagnosis observations for QOF register management.

Clinical Purpose:
• QOF stroke register data collection and monitoring
• Cerebrovascular event tracking and secondary prevention pathway coordination
• Stroke care pathway management and rehabilitation support
• Long-term cardiovascular outcome tracking and intervention assessment

Data Granularity:
• One row per stroke or TIA diagnosis observation
• Uses STRK_COD (stroke) and TIA_COD (TIA) QOF clusters
• Includes all patients regardless of status for comprehensive QOF reporting

Key Features:
• Permanent cardiovascular event tracking with no resolution codes
• No specific age restrictions for stroke register inclusion
• Critical for cerebrovascular secondary prevention protocols
• Essential input for stroke care quality improvement initiatives'"
        ]
    )
}}

/*
All stroke and TIA diagnosis observations from clinical records.
Uses QOF stroke cluster IDs:
- STRK_COD: Stroke diagnoses
- TIA_COD: TIA diagnoses

Clinical Purpose:
- QOF stroke register data collection
- Stroke care pathway monitoring
- Cardiovascular event tracking
- Resolution status tracking

QOF Context:
Stroke register includes persons with stroke or TIA diagnosis codes.
Strokes and TIAs are considered permanent events with no resolution codes.
No specific age restrictions for stroke register.

Includes ALL persons (active, inactive, deceased) following intermediate layer principles.
This is OBSERVATION-LEVEL data - one row per stroke/TIA observation.
Use this model as input for fct_person_stroke_tia_register.sql which applies QOF business rules.
*/

SELECT
    obs.observation_id,
    obs.person_id,
    obs.clinical_effective_date,
    obs.mapped_concept_code AS concept_code,
    obs.mapped_concept_display AS concept_display,
    obs.cluster_id AS source_cluster_id,

    -- Stroke/TIA-specific flags (observation-level only)
    CASE WHEN obs.cluster_id = 'STRK_COD' THEN TRUE ELSE FALSE END AS is_stroke_diagnosis_code,
    CASE WHEN obs.cluster_id = 'TIA_COD' THEN TRUE ELSE FALSE END AS is_tia_diagnosis_code,

    -- Stroke/TIA observation type determination
    CASE
        WHEN obs.cluster_id = 'STRK_COD' THEN 'Stroke Diagnosis'
        WHEN obs.cluster_id = 'TIA_COD' THEN 'TIA Diagnosis'
        ELSE 'Unknown'
    END AS stroke_tia_observation_type

FROM ({{ get_observations("'STRK_COD', 'TIA_COD'") }}) obs
WHERE obs.clinical_effective_date IS NOT NULL

ORDER BY person_id, clinical_effective_date, observation_id
