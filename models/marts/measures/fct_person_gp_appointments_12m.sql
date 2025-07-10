{{
    config(
        materialized='table',
        cluster_by=['person_id', 'organisation_id'])
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
