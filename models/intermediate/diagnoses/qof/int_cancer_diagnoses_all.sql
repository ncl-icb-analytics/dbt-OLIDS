{{
    config(
        materialized='table',
        cluster_by=['person_id', 'clinical_effective_date'],
        post_hook=[
            "COMMENT ON TABLE {{ this }} IS 'Intermediate: Cancer Diagnoses All - Complete history of all cancer diagnosis observations for QOF register management.

Clinical Purpose:
• QOF cancer register data collection and monitoring
• Cancer care pathway coordination and treatment tracking
• Oncology service utilisation and quality improvement
• Resolution and remission status tracking

Data Granularity:
• One row per cancer diagnosis observation
• Uses CAN_COD cluster for QOF-compliant cancer diagnosis identification
• Includes all patients regardless of status for comprehensive QOF reporting

Key Features:
• Comprehensive cancer diagnosis tracking across all age groups
• No specific age restrictions for cancer register inclusion
• Resolution and remission monitoring capability
• Critical for cancer care quality indicators and pathway management'"
        ]
    )
}}

/*
All cancer diagnosis observations from clinical records.
Uses QOF cancer cluster IDs:
- CAN_COD: Cancer diagnoses

Clinical Purpose:
- QOF cancer register data collection
- Cancer care pathway monitoring
- Oncology treatment tracking
- Resolution/remission status tracking

QOF Context:
Cancer register includes persons with cancer diagnosis codes who have not
been resolved/in remission. Resolution logic applied in downstream fact models.
No specific age restrictions for cancer register.

Includes ALL persons (active, inactive, deceased) following intermediate layer principles.
This is OBSERVATION-LEVEL data - one row per cancer observation.
Use this model as input for fct_person_cancer_register.sql which applies QOF business rules.
*/

SELECT
    obs.observation_id,
    obs.person_id,
    obs.clinical_effective_date,
    obs.mapped_concept_code AS concept_code,
    obs.mapped_concept_display AS concept_display,
    obs.cluster_id AS source_cluster_id,

    -- Cancer-specific flags (observation-level only)
    CASE WHEN obs.cluster_id = 'CAN_COD' THEN TRUE ELSE FALSE END AS is_cancer_diagnosis_code,

    -- Cancer observation type determination
    CASE
        WHEN obs.cluster_id = 'CAN_COD' THEN 'Cancer Diagnosis'
        ELSE 'Unknown'
    END AS cancer_observation_type

FROM ({{ get_observations("'CAN_COD'") }}) obs
WHERE obs.clinical_effective_date IS NOT NULL

ORDER BY person_id, clinical_effective_date, observation_id
