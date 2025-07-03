{{
    config(
        materialized='table',
        cluster_by=['organisation_id'],
        post_hook=[
            "COMMENT ON TABLE {{ this }} IS 'Mart: Organisation Active Patients - Current active patient list size per organisation for capacity planning and performance monitoring.

Business Purpose:
• Support operational analytics for practice list management and capacity planning
• Enable business intelligence reporting on practice list sizes and patient distribution
• Provide foundation for commissioning and resource allocation based on patient volumes
• Support population health analytics and practice performance monitoring

Data Granularity:
• One row per organisation with active patient count
• Includes current active patient list size for operational planning
• Current snapshot of organisation patient volumes and list management

Key Features:
• Tracks active patient count per organisation for capacity planning
• Supports practice-level reporting and operational management
• Enables business intelligence for resource allocation and service planning
• Provides foundation for performance monitoring and commissioning analytics'"
        ]
    )
}}

-- Organisation Active Patients Fact Table
-- Service usage metric: Current active patient list size per organisation

SELECT
    org.id AS organisation_id,
    org.organisation_code AS ods_code,
    'ORGANISATION_ACTIVE_LIST_SIZE' AS measure_id,
    COUNT(ap.person_id) AS active_patient_count

FROM {{ ref('stg_olids_organisation') }} AS org
INNER JOIN {{ ref('dim_person_active_patients') }} AS ap
    ON org.organisation_code = ap.record_owner_org_code -- Use active patients dimension directly
WHERE org.is_obsolete = FALSE
GROUP BY
    org.id,
    org.organisation_code
