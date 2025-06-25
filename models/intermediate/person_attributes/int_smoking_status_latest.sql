{{
    config(
        materialized='table',
        cluster_by=['person_id']
    )
}}

/*
Latest smoking status per person based on most recent smoking-related observation.
Uses QOF definitions and prioritises specific status codes over general smoking codes.
*/

SELECT
    person_id,
    observation_id,
    clinical_effective_date,
    concept_code,
    code_description,
    source_cluster_id,
    is_smoker_code,
    is_ex_smoker_code,
    is_never_smoked_code,
    smoking_status,
    is_current_smoker,
    is_ex_smoker

FROM (
    {{ get_latest_events(
        ref('int_smoking_status_all'),
        partition_by=['person_id'],
        order_by='clinical_effective_date'
    ) }}
) latest_smoking_status
