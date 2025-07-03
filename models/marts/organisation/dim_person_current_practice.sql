{{
    config(
        materialized='table',
        tags=['dimension', 'person', 'practice', 'current'],
        cluster_by=['person_id'],
        post_hook=[
            "COMMENT ON TABLE {{ this }} IS 'Mart: Person Current Practice Dimension - Current practice registration details for each person.

Business Purpose:
• Support operational analytics for practice list management and patient attribution
• Enable business intelligence reporting on practice populations and patient distribution
• Provide foundation for QOF reporting and practice performance analysis
• Support population health analytics and practice-level resource allocation

Data Granularity:
• One row per person with current practice registration
• Includes practice organisational details and registration timeline
• Current snapshot of practice registration status and relationships

Key Features:
• Links patients to their current registered practice with full organisational context
• Includes practice operational status and hierarchy information
• Supports practice-level reporting and population health management
• Enables business intelligence for practice operations and patient care coordination'"
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
