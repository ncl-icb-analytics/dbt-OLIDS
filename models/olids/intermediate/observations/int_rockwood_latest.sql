{{
    config(
        materialized='table',
        cluster_by=['person_id']
    )
}}

/*
Latest Rockwood Clinical Frailty Scale score per person.
Uses most recent assessment date, with observation_id as tiebreaker.
*/

SELECT
    person_id,
    observation_id,
    clinical_effective_date,
    concept_code,
    concept_display,
    source_cluster_id,
    rockwood_score,
    rockwood_description,
    frailty_level,
    frailty_category,
    is_valid_rockwood_code,
    is_frail,
    is_severely_frail
FROM {{ ref('int_rockwood_all') }}
QUALIFY ROW_NUMBER() OVER (
    PARTITION BY person_id
    ORDER BY clinical_effective_date DESC, observation_id DESC
) = 1