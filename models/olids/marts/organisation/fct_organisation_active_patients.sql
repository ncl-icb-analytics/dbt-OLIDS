{{
    config(
        materialized='table',
        cluster_by=['organisation_id'])
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
