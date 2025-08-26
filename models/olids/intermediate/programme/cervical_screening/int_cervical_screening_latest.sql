{{
    config(
        materialized='table',
        cluster_by=['person_id'],
        persist_docs={"relation": true})
}}

/*
Latest cervical screening status per person.
Simple QUALIFY-based latest record selection from int_cervical_screening_all.

Business Rules:
- Returns the most recent screening observation per person
- No date-based business logic (kept in fact layer)
- Foundation for person-level screening status analysis

Used for cervical screening programme analysis and current status determination.
*/

SELECT
    observation_id,
    person_id,
    clinical_effective_date,
    concept_code,
    concept_display,
    source_cluster_id,
    screening_observation_type,
    is_completed_screening,
    is_unsuitable_screening,
    is_declined_screening,
    is_non_response_screening

FROM {{ ref('int_cervical_screening_all') }}

QUALIFY ROW_NUMBER() OVER (
    PARTITION BY person_id 
    ORDER BY clinical_effective_date DESC, observation_id DESC
) = 1

ORDER BY person_id