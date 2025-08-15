{{
    config(
        materialized='table',
        tags=['intermediate', 'observations', 'fractures'],
        cluster_by=['person_id', 'clinical_effective_date'])
}}

/*
All fragility fracture observations from clinical records.
Uses QOF cluster ID FF_COD for fractures after April 2012 as per QOF guidelines.

Clinical Purpose:
- QOF osteoporosis register data collection
- Fracture site and pattern tracking
- Bone health monitoring support

One row per fracture observation.
Use this model as input for osteoporosis register and bone health models.
*/

SELECT
    obs.observation_id,
    obs.person_id,
    obs.patient_id,
    obs.clinical_effective_date,
    obs.cluster_id AS source_cluster_id,
    obs.mapped_concept_code AS concept_code,
    obs.mapped_concept_display AS code_description,
    CAST(obs.result_value AS NUMBER(10,2)) AS numeric_value,

    -- Extract fracture site from code description
    CASE
        WHEN LOWER(obs.mapped_concept_display) LIKE '%hip%' THEN 'Hip'
        WHEN LOWER(obs.mapped_concept_display) LIKE '%wrist%' OR LOWER(obs.mapped_concept_display) LIKE '%radius%' THEN 'Wrist'
        WHEN LOWER(obs.mapped_concept_display) LIKE '%spine%' OR LOWER(obs.mapped_concept_display) LIKE '%vertebra%' THEN 'Spine'
        WHEN LOWER(obs.mapped_concept_display) LIKE '%humerus%' OR LOWER(obs.mapped_concept_display) LIKE '%shoulder%' THEN 'Humerus'
        WHEN LOWER(obs.mapped_concept_display) LIKE '%pelvis%' THEN 'Pelvis'
        WHEN LOWER(obs.mapped_concept_display) LIKE '%femur%' THEN 'Femur'
        ELSE 'Other'
    END AS fracture_site,

    -- Clinical flags (observation-level only)
    obs.cluster_id = 'FF_COD' AS is_fragility_fracture_code

FROM ({{ get_observations("'FF_COD'") }}) obs
-- Only include fractures after April 2012 as per QOF requirements
WHERE obs.clinical_effective_date >= '2012-04-01'
  AND obs.clinical_effective_date IS NOT NULL

ORDER BY person_id, clinical_effective_date DESC
