{{
    config(
        materialized='table',
        cluster_by=['person_id', 'clinical_effective_date']
    )
}}

/*
All rheumatoid arthritis (RA) diagnoses from clinical records.
Uses QOF cluster ID RARTH_COD for rheumatoid arthritis diagnosis codes.

Clinical Purpose:
- RA register inclusion for QOF quality measures
- Rheumatoid arthritis disease management monitoring
- DMARDs (Disease-Modifying Anti-Rheumatic Drugs) prescribing support

QOF Context:
RA register follows simple diagnosis-only pattern with age restriction - any RA diagnosis 
for patients aged 16+ qualifies for register inclusion. No resolution codes.
This supports RA quality measures and specialist care monitoring.

Note: QOF RA register requires age ≥16 years at diagnosis.

Includes ALL persons (active, inactive, deceased) following intermediate layer principles.
Use this model as input for RA register (with age filtering applied in fact layer).
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
    FROM {{ get_observations("'RARTH_COD'") }}
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
        TRUE AS has_ra_diagnosis,
        
        -- Date aggregates
        MIN(clinical_effective_date) AS earliest_ra_date,
        MAX(clinical_effective_date) AS latest_ra_date,
        COUNT(DISTINCT clinical_effective_date) AS total_ra_episodes,
        
        -- Code arrays for detailed analysis
        ARRAY_AGG(DISTINCT concept_code) AS all_ra_concept_codes,
        ARRAY_AGG(DISTINCT concept_description) AS all_ra_concept_displays,
        
        -- Latest values for reference
        FIRST_VALUE(concept_code) OVER (
            PARTITION BY person_id 
            ORDER BY clinical_effective_date DESC, date_recorded DESC
        ) AS latest_ra_concept_code,
        
        FIRST_VALUE(concept_description) OVER (
            PARTITION BY person_id 
            ORDER BY clinical_effective_date DESC, date_recorded DESC
        ) AS latest_ra_concept_description

    FROM observations_with_person
    GROUP BY person_id
)

SELECT
    person_id,
    has_ra_diagnosis,
    earliest_ra_date,
    latest_ra_date,
    total_ra_episodes,
    all_ra_concept_codes,
    all_ra_concept_displays,
    latest_ra_concept_code,
    latest_ra_concept_description
FROM person_level_aggregates 