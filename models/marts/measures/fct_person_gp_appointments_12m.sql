{{
    config(
        materialized='table',
        cluster_by=['person_id', 'organisation_id'],
        post_hook=[
            "COMMENT ON TABLE {{ this }} IS 'Mart: GP Appointments 12-Month Summary - Service usage analytics for primary care appointment activity.

Business Purpose:
• Support operational analytics for GP practice capacity planning and resource allocation
• Enable business intelligence reporting on patient service utilisation patterns
• Provide population health analytics for primary care access and demand management
• Support commissioning and performance monitoring of GP services

Data Granularity:
• One row per person per organisation with appointments in last 12 months
• Includes appointment count and temporal distribution over 12-month period
• Current analysis of GP service utilisation and patient engagement

Key Features:
• Tracks total appointment count per person per practice over 12 months
• Includes earliest and latest appointment dates for utilisation patterns
• Supports operational decision-making for practice capacity management
• Enables population health management and service planning analytics'"
        ]
    )
}}

-- GP Appointments 12-Month Fact Table
-- Service usage metric: Number of GP appointments per person per organisation in last 12 months

SELECT
    a.person_id,
    a.organisation_id,
    'GP_APPOINTMENTS_12M' AS measure_id,
    age.age,
    COUNT(a.id) AS appointment_count,
    MIN(a.start_date) AS earliest_appointment_date,

    -- Age for context
    MAX(a.start_date) AS latest_appointment_date

FROM {{ ref('stg_olids_appointment') }} AS a
INNER JOIN {{ ref('dim_person_age') }} AS age ON a.person_id = age.person_id
WHERE
    a.start_date >= CURRENT_DATE() - INTERVAL '12 months'
    AND a.start_date <= CURRENT_DATE()
GROUP BY
    a.person_id,
    a.organisation_id,
    age.age
