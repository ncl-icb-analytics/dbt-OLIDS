{{
    config(
        materialized='table',
        cluster_by=['organisation_id']
    )
}}

-- Organisation Active Patients Fact Table
-- Service usage metric: Current active patient list size per organisation

SELECT 
    org.organisation_id,
    org.organisation_code AS ods_code,
    'ORGANISATION_ACTIVE_LIST_SIZE' AS measure_id,
    COUNT(p.person_id) AS active_patient_count
    
FROM {{ ref('stg_olids_organisation') }} org
INNER JOIN {{ ref('stg_olids_patient') }} pat 
    ON org.organisation_code = pat.record_owner_organisation_code
INNER JOIN {{ ref('dim_person') }} p 
    ON pat.person_id = p.person_id
WHERE org.is_obsolete = FALSE
    AND pat.lds_end_date_time < pat.lds_start_date_time -- Legacy business rule
    AND pat.death_year IS NULL
    AND pat.death_month IS NULL
GROUP BY 
    org.organisation_id,
    org.organisation_code 