{{
    config(
        materialized='table',
        tags=['dimension', 'person', 'inactive'],
        cluster_by=['person_id'],
        post_hook=[
            "COMMENT ON TABLE {{ this }} IS 'Dimension table providing inactive patient status at person level. Includes only deceased patients and patients from closed/obsolete practices. Excludes dummy patients.'"
        ]
    )
}}

-- Person Inactive Patients Dimension Table
-- Includes only deceased patients and patients from closed/obsolete practices
-- Excludes dummy patients as they are not considered real inactive patients

WITH patient_ids_per_person AS (
    -- First collect all patient IDs for each person
    SELECT
        pp.person_id,
        ARRAY_AGG(DISTINCT pp.patient_id) WITHIN GROUP (
            ORDER BY pp.patient_id
        ) AS patient_ids
    FROM {{ ref('stg_olids_patient_person') }} AS pp
    GROUP BY pp.person_id
),

latest_patient_record_per_person AS (
    -- Get the latest patient record for each person
    SELECT
        pp.person_id,
        p.sk_patient_id,
        per.ldsbusinessid_primarypatient,
        pip.patient_ids,
        -- Determine if patient is active based on various criteria
        p.is_dummy_patient,
        p.is_confidential,
        p.is_spine_sensitive,
        p.birth_year,
        p.birth_month,
        p.death_year,
        p.death_month,
        php.practice_id AS registered_practice_id,
        php.practice_code,
        -- Practice details from DIM_PERSON_HISTORICAL_PRACTICE
        php.practice_name,
        php.practice_type_code,
        php.practice_type_desc,
        php.practice_postcode,
        php.practice_parent_org_id,
        php.practice_open_date,
        php.practice_close_date,
        php.practice_is_obsolete,
        p.record_owner_organisation_code AS record_owner_org_code,
        p.lds_datetime_data_acquired AS latest_record_date,
        CASE
            WHEN p.death_year IS NOT NULL THEN FALSE -- Deceased
            WHEN p.is_dummy_patient THEN FALSE -- Dummy patient
            WHEN php.practice_close_date IS NOT NULL THEN FALSE -- Practice closed
            WHEN php.practice_is_obsolete THEN FALSE -- Practice obsolete
            ELSE TRUE
        END AS is_active,
        p.death_year IS NOT NULL AS is_deceased,
        -- Determine reason for inactivity
        CASE
            WHEN p.death_year IS NOT NULL THEN 'Deceased'
            WHEN php.practice_close_date IS NOT NULL THEN 'Practice Closed'
            WHEN php.practice_is_obsolete THEN 'Practice Obsolete'
        END AS inactive_reason,
        -- Rank to get the latest record
        ROW_NUMBER() OVER (
            PARTITION BY pp.person_id
            ORDER BY
                p.lds_datetime_data_acquired DESC,
                p.id DESC
        ) AS record_rank
    FROM {{ ref('stg_olids_patient_person') }} AS pp
    INNER JOIN {{ ref('stg_olids_patient') }} AS p
        ON pp.patient_id = p.id
    INNER JOIN {{ ref('stg_olids_person') }} AS per
        ON pp.person_id = per.id
    INNER JOIN patient_ids_per_person AS pip
        ON pp.person_id = pip.person_id
    LEFT JOIN {{ ref('dim_person_historical_practice') }} AS php
        ON
            pp.person_id = php.person_id
            AND php.is_current_registration = TRUE
)

-- Select only the latest record per person and only inactive patients
SELECT
    person_id,
    sk_patient_id,
    ldsbusinessid_primarypatient,
    patient_ids,
    is_active,
    is_deceased,
    is_dummy_patient,
    is_confidential,
    is_spine_sensitive,
    birth_year,
    birth_month,
    death_year,
    death_month,
    registered_practice_id,
    practice_code,
    practice_name,
    practice_type_code,
    practice_type_desc,
    practice_postcode,
    practice_parent_org_id,
    practice_open_date,
    practice_close_date,
    practice_is_obsolete,
    record_owner_org_code,
    latest_record_date,
    inactive_reason
FROM latest_patient_record_per_person
WHERE
    record_rank = 1
    AND is_active = FALSE
    AND is_dummy_patient = FALSE -- Exclude dummy patients
    AND inactive_reason IS NOT NULL -- Only include patients with a valid inactive reason
