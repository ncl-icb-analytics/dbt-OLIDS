{{
    config(
        materialized='table',
        cluster_by=['person_id', 'clinical_effective_date'])
}}

/*
All palliative care pathway observations from clinical records.
Uses QOF palliative care cluster IDs:
- PALCARE_COD: Palliative care codes
- PALCARENI_COD: Palliative care no longer indicated codes

Clinical Purpose:
- QOF palliative care register data collection (on/after 1 April 2008)
- End-of-life care pathway monitoring
- Palliative care status tracking
- Care appropriateness assessment

Key QOF Requirements:
- Register inclusion: Palliative care code (PALCARE_COD) on/after 1 April 2008
- Exclusion logic: 'No longer indicated' codes (PALCARENI_COD)
- Current status determination based on latest codes
- Comprehensive end-of-life care monitoring

Important pathway with inclusion/exclusion logic for appropriate care targeting.

Includes ALL persons (active, inactive, deceased) following intermediate layer principles.
Use this model as input for fct_person_palliative_care_register.sql which applies QOF business rules.
*/

WITH base_observations AS (

    SELECT
        obs.observation_id,
        obs.person_id,
        obs.clinical_effective_date,
        obs.mapped_concept_code AS concept_code,
        obs.mapped_concept_display AS concept_display,
        obs.cluster_id AS source_cluster_id,

        -- Flag different types of palliative care codes following QOF definitions
        CASE WHEN obs.cluster_id = 'PALCARE_COD' THEN TRUE ELSE FALSE END AS is_palliative_care_code,
        CASE WHEN obs.cluster_id = 'PALCARENI_COD' THEN TRUE ELSE FALSE END AS is_palliative_care_not_indicated_code,

        -- Composite flag for unified clinical tracking
        CASE WHEN obs.cluster_id = 'PALCARE_COD' THEN TRUE ELSE FALSE END AS is_diagnosis_code,
        CASE WHEN obs.cluster_id = 'PALCARENI_COD' THEN TRUE ELSE FALSE END AS is_resolved_code

    FROM ({{ get_observations("'PALCARE_COD', 'PALCARENI_COD'", source='PCD') }}) obs
    WHERE obs.clinical_effective_date IS NOT NULL
)

SELECT
    person_id,
    observation_id,
    clinical_effective_date,
    concept_code,
    concept_display,
    source_cluster_id,
    is_palliative_care_code,
    is_palliative_care_not_indicated_code,
    is_diagnosis_code,
    is_resolved_code

FROM base_observations

-- Sort for consistent output
ORDER BY person_id, clinical_effective_date DESC
