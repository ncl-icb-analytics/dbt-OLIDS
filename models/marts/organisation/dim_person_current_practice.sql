{{
    config(
        materialized='table',
        tags=['dimension', 'person', 'practice', 'current'],
        cluster_by=['person_id'])
}}

-- Person Current Practice Dimension Table
-- Simple view of current practice registrations from historical practice table

SELECT
    person_id,
    sk_patient_id,
    practice_id,
    practice_code,
    practice_name,
    practice_type_code,
    practice_type_desc,
    practice_postcode,
    practice_parent_org_id,
    practice_open_date,
    practice_close_date,
    practice_is_obsolete,
    registration_start_date,
    registration_end_date
FROM {{ ref('dim_person_historical_practice') }}
WHERE is_current_registration = TRUE
