{{
    config(
        materialized='table',
        cluster_by=['person_id', 'clinical_effective_date']
    )
}}

/*
All cancer diagnoses from clinical records.
Uses QOF cluster ID CAN_COD for cancer diagnosis codes (excluding non-melanotic skin cancers).

Clinical Purpose:
- Cancer register inclusion for QOF quality measures
- Cancer care pathway tracking
- Survivorship care planning

QOF Context:
Cancer register follows simple diagnosis-only pattern with date restriction - any cancer 
diagnosis on/after April 1, 2003 qualifies for register inclusion. No resolution codes.
This supports cancer care quality measures and survivorship monitoring.

Note: Excludes non-melanotic skin cancers as per QOF guidelines.

Includes ALL persons (active, inactive, deceased) following intermediate layer principles.
Use this model as input for cancer register.
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
    FROM {{ get_observations("'CAN_COD'") }}
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
        TRUE AS has_cancer_diagnosis,
        
        -- Date aggregates
        MIN(clinical_effective_date) AS earliest_cancer_date,
        MAX(clinical_effective_date) AS latest_cancer_date,
        COUNT(DISTINCT clinical_effective_date) AS total_cancer_episodes,
        
        -- Code arrays for detailed analysis
        ARRAY_AGG(DISTINCT concept_code) AS all_cancer_concept_codes,
        ARRAY_AGG(DISTINCT concept_description) AS all_cancer_concept_displays,
        
        -- Latest values for reference
        FIRST_VALUE(concept_code) OVER (
            PARTITION BY person_id 
            ORDER BY clinical_effective_date DESC, date_recorded DESC
        ) AS latest_cancer_concept_code,
        
        FIRST_VALUE(concept_description) OVER (
            PARTITION BY person_id 
            ORDER BY clinical_effective_date DESC, date_recorded DESC
        ) AS latest_cancer_concept_description

    FROM observations_with_person
    GROUP BY person_id
)

SELECT
    person_id,
    has_cancer_diagnosis,
    earliest_cancer_date,
    latest_cancer_date,
    total_cancer_episodes,
    all_cancer_concept_codes,
    all_cancer_concept_displays,
    latest_cancer_concept_code,
    latest_cancer_concept_description
FROM person_level_aggregates 