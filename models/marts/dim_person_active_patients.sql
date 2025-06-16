{{
    config(
        materialized='table',
        tags=['dimension', 'person', 'active'],
        cluster_by=['person_id'],
        post_hook=[
            "COMMENT ON TABLE {{ this }} IS 'Dimension table providing active patient status at person level. Excludes deceased patients and dummy patients. Includes practice registration details.'"
        ]
    )
}}

-- Person Active Patients Dimension Table
-- Filters out deceased patients and dummy patients
-- Links PATIENT_PERSON, PATIENT, PERSON, and ORGANISATION tables

WITH patient_ids_per_person AS (
    -- First collect all patient IDs for each person
    SELECT
        pp.person_id,
        ARRAY_AGG(DISTINCT pp.patient_id) WITHIN GROUP (ORDER BY pp.patient_id) AS patient_ids
    FROM {{ ref('stg_olids_patient_person') }} pp
    GROUP BY pp.person_id
),

latest_patient_record_per_person AS (
    -- Get the latest patient record for each person
    SELECT
        pp.person_id,
        p.sk_patient_id,
        per.primary_patient_id,
        pip.patient_ids,
        -- Determine if patient is active based on various criteria
        CASE
            WHEN p.death_year IS NOT NULL THEN FALSE -- Deceased
            WHEN p.is_dummy_patient THEN FALSE -- Dummy patient
            WHEN php.practice_close_date IS NOT NULL THEN FALSE -- Practice closed
            WHEN php.practice_is_obsolete THEN FALSE -- Practice obsolete
            ELSE TRUE
        END AS is_active,
        p.death_year IS NOT NULL AS is_deceased,
        p.is_dummy_patient,
        p.is_confidential,
        p.is_spine_sensitive,
        p.birth_year,
        p.birth_month,
        p.death_year,
        p.death_month,
        -- Practice details from DIM_PERSON_HISTORICAL_PRACTICE
        php.practice_id AS registered_practice_id,
        php.practice_code,
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
        -- Rank to get the latest record
        ROW_NUMBER() OVER (
            PARTITION BY pp.person_id
            ORDER BY 
                p.lds_datetime_data_acquired DESC,
                p.id DESC
        ) AS record_rank
    FROM {{ ref('stg_olids_patient_person') }} pp
    JOIN {{ ref('stg_olids_patient') }} p
        ON pp.patient_id = p.id
    JOIN {{ ref('stg_olids_person') }} per
        ON pp.person_id = per.id
    JOIN patient_ids_per_person pip
        ON pp.person_id = pip.person_id
    LEFT JOIN {{ ref('dim_person_historical_practice') }} php
        ON pp.person_id = php.person_id
        AND php.is_current_practice = TRUE
)

-- Select only the latest record per person and only active patients
SELECT
    person_id,
    sk_patient_id,
    primary_patient_id,
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
    latest_record_date
FROM latest_patient_record_per_person
WHERE record_rank = 1
    AND is_active = TRUE 