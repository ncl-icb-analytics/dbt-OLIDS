{{
    config(
        materialized='table',
        cluster_by=['person_id', 'clinical_effective_date']
    )
}}

/*
All familial hypercholesterolaemia (FH) diagnoses from clinical records.
Uses QOF cluster ID FHYP_COD for familial hypercholesterolaemia diagnosis codes.

Clinical Purpose:
- FH register inclusion for QOF quality measures
- Familial hypercholesterolaemia cascade screening
- High-intensity statin therapy monitoring

QOF Context:
FH register follows simple diagnosis-only pattern - any FH diagnosis qualifies for 
register inclusion. No resolution codes or complex criteria.
This supports FH quality measures and cascade family screening programmes.

Includes ALL persons (active, inactive, deceased) following intermediate layer principles.
Use this model as input for FH register.
*/

WITH base_observations AS (
    SELECT
        person_id,
        clinical_effective_date,
        source_cluster_id,
        concept_code,
        concept_description,
        observation_value_text,
        observation_value_numeric,
        observation_units,
        date_recorded
    FROM {{ get_observations("'FHYP_COD'") }}
),

-- Add person demographics for context
observations_with_person AS (
    SELECT
        obs.*,
        p.age_years,
        p.gender,
        p.is_active
    FROM base_observations obs
    LEFT JOIN {{ ref('dim_person') }} p
        ON obs.person_id = p.person_id
),

-- Person-level aggregation for efficient downstream use
person_level_aggregates AS (
    SELECT
        person_id,
        
        -- Diagnosis flags
        TRUE AS has_fh_diagnosis,
        
        -- Date aggregates
        MIN(clinical_effective_date) AS earliest_fh_date,
        MAX(clinical_effective_date) AS latest_fh_date,
        COUNT(DISTINCT clinical_effective_date) AS total_fh_episodes,
        
        -- Code arrays for detailed analysis
        ARRAY_AGG(DISTINCT concept_code) AS all_fh_concept_codes,
        ARRAY_AGG(DISTINCT concept_description) AS all_fh_concept_displays,
        
        -- Latest values for reference
        FIRST_VALUE(concept_code) OVER (
            PARTITION BY person_id 
            ORDER BY clinical_effective_date DESC, date_recorded DESC
        ) AS latest_fh_concept_code,
        
        FIRST_VALUE(concept_description) OVER (
            PARTITION BY person_id 
            ORDER BY clinical_effective_date DESC, date_recorded DESC
        ) AS latest_fh_concept_description

    FROM observations_with_person
    GROUP BY person_id
)

SELECT
    person_id,
    has_fh_diagnosis,
    earliest_fh_date,
    latest_fh_date,
    total_fh_episodes,
    all_fh_concept_codes,
    all_fh_concept_displays,
    latest_fh_concept_code,
    latest_fh_concept_description
FROM person_level_aggregates 