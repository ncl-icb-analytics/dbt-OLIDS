{{
    config(
        materialized='table',
        cluster_by=['person_id', 'clinical_effective_date'],
        persist_docs={"relation": true})
}}

/*
All cervical screening programme observations from clinical records.
Uses QOF cervical screening cluster IDs:
- SMEAR_COD: Cervical screening completed codes
- CSPU_COD: Cervical screening unsuitable codes  
- CSDEC_COD: Cervical screening declined codes
- CSPCAINVITE_COD: Not responded to three invitations codes

Clinical Purpose:
- Cervical screening programme data collection
- Observation-level screening events tracking
- Foundation data for programme analysis

Key Business Rules:
- Women aged 25-49: invited every 3 years
- Women aged 50-64: invited every 5 years
- Declined/non-response status: valid for 12 months only
- Unsuitable status: permanent unless superseded by completed screening

Includes ALL persons (active, inactive, deceased) following intermediate layer principles.
This is OBSERVATION-LEVEL data - one row per cervical screening observation.
*/

SELECT
    obs.observation_id,
    obs.person_id,
    obs.clinical_effective_date,
    obs.mapped_concept_code AS concept_code,
    obs.mapped_concept_display AS concept_display,
    obs.cluster_id AS source_cluster_id,

    -- Screening type classification
    CASE
        WHEN obs.cluster_id = 'SMEAR_COD' THEN 'Cervical Screening Completed'
        WHEN obs.cluster_id = 'CSPU_COD' THEN 'Screening Unsuitable'
        WHEN obs.cluster_id = 'CSDEC_COD' THEN 'Screening Declined'
        WHEN obs.cluster_id = 'CSPCAINVITE_COD' THEN 'Non-response to Invitations'
        ELSE 'Unknown'
    END AS screening_observation_type,

    -- Simple screening type flags based on core cluster codes
    CASE WHEN obs.cluster_id = 'SMEAR_COD' THEN TRUE ELSE FALSE END AS is_completed_screening,
    CASE WHEN obs.cluster_id = 'CSPU_COD' THEN TRUE ELSE FALSE END AS is_unsuitable_screening,
    CASE WHEN obs.cluster_id = 'CSDEC_COD' THEN TRUE ELSE FALSE END AS is_declined_screening,
    CASE WHEN obs.cluster_id = 'CSPCAINVITE_COD' THEN TRUE ELSE FALSE END AS is_non_response_screening

FROM ({{ get_observations("'SMEAR_COD', 'CSPU_COD', 'CSDEC_COD', 'CSPCAINVITE_COD'") }}) obs
WHERE obs.clinical_effective_date IS NOT NULL

ORDER BY person_id, clinical_effective_date DESC, observation_id