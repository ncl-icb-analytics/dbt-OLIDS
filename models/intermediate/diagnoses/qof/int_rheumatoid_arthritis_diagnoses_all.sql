{{
    config(
        materialized='table',
        cluster_by=['person_id', 'clinical_effective_date'],
        post_hook=[
            "COMMENT ON TABLE {{ this }} IS 'Intermediate: Rheumatoid Arthritis Diagnoses All - Complete history of all rheumatoid arthritis diagnosis observations for QOF register management.

Clinical Purpose:
• QOF rheumatoid arthritis register data collection and monitoring
• Musculoskeletal disease management and inflammatory arthritis care pathway
• Disease activity monitoring and treatment response assessment
• Long-term joint health and mobility outcome tracking

Data Granularity:
• One row per rheumatoid arthritis diagnosis observation
• Uses RA_COD cluster for QOF-compliant RA diagnosis identification
• Includes all patients regardless of status for comprehensive QOF reporting

Key Features:
• Lifelong condition register for ongoing disease management
• Simple diagnosis-only pattern with no resolution codes
• Critical for inflammatory arthritis treatment optimisation
• Essential input for rheumatoid arthritis care quality improvement initiatives'"
        ]
    )
}}

/*
All rheumatoid arthritis diagnoses from clinical records.
Uses QOF cluster ID RA_COD for all forms of rheumatoid arthritis diagnosis.

Clinical Purpose:
- RA register inclusion for QOF musculoskeletal disease management
- Disease activity monitoring and treatment pathway identification
- Inflammatory arthritis care pathway

QOF Context:
RA register follows simple diagnosis-only pattern - any RA diagnosis code
qualifies for register inclusion. No resolution codes or complex criteria.
This is a lifelong condition register for ongoing disease management.

Includes ALL persons (active, inactive, deceased) following intermediate layer principles.
This is OBSERVATION-LEVEL data - one row per RA observation.
Use this model as input for fct_person_rheumatoid_arthritis_register.sql which applies QOF business rules.
*/

SELECT
    obs.observation_id,
    obs.person_id,
    obs.clinical_effective_date,
    obs.mapped_concept_code AS concept_code,
    obs.mapped_concept_display AS concept_display,
    obs.cluster_id AS source_cluster_id,

    -- RA-specific flags (observation-level only)
    CASE WHEN obs.cluster_id = 'RA_COD' THEN TRUE ELSE FALSE END AS is_ra_diagnosis_code,

    -- Observation type determination
    CASE
        WHEN obs.cluster_id = 'RA_COD' THEN 'Rheumatoid Arthritis Diagnosis'
        ELSE 'Unknown'
    END AS ra_observation_type

FROM ({{ get_observations("'RARTH_COD'") }}) obs
WHERE obs.clinical_effective_date IS NOT NULL

ORDER BY person_id, clinical_effective_date, observation_id
