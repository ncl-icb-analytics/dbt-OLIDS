{{
    config(
        materialized='table',
        tags=['dimension', 'person', 'practice', 'current'],
        cluster_by=['person_id'],
        post_hook=[
            "COMMENT ON TABLE {{ this }} IS 'Mart: Person Current Practice Dimension - Current practice registration details for each person.

Population Scope:
• All persons with current practice registrations
• One row per person with active practice registration status
• Includes practice organisational details and registration timeline

Key Features:
• Links patients to their current registered practice with organisational context
• Includes practice operational status and hierarchy information
• Supports practice-level reporting and population health management'"
        ]
    )
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
