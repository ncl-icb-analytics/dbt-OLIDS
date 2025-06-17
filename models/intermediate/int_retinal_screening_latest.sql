{{
    config(
        materialized='table',
        cluster_by=['person_id']
    )
}}

/*
Latest diabetes retinal screening completion per person.
Used for diabetes care processes and screening programme analysis.
*/

SELECT
    person_id,
    observation_id,
    clinical_effective_date,
    concept_code,
    concept_display,
    source_cluster_id,
    is_completed_screening,
    days_since_screening,
    screening_current_12m,
    screening_current_24m

FROM (
    {{ get_latest_events(
        ref('int_retinal_screening_all'), 
        partition_by=['person_id'],
        order_by='clinical_effective_date'
    ) }}
) latest_retinal_screening 