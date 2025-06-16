{{
    config(
        materialized='table',
        tags=['dimension', 'person', 'practice', 'historical'],
        cluster_by=['person_id'],
        post_hook=[
            "COMMENT ON TABLE {{ this }} IS 'Dimension table tracking all practice registrations (current and historical) for each person. Each row represents a unique practice registration period, determined by aggregating GP relationships - using the earliest GP assignment as the registration start date and the latest GP assignment end date as the registration end date for each practice.'"
        ]
    )
}}

-- Person Historical Practice Dimension Table
-- Tracks all practice registrations (current and historical) for each person

WITH practice_registration_periods AS (
    -- Aggregate practice registrations by person and practice, finding earliest start and latest end dates
    SELECT 
        prp.person_id,
        prp.organisation_id,
        MIN(prp.start_date) AS registration_start_date,
        MAX(prp.end_date) AS registration_end_date
    FROM {{ ref('stg_olids_patient_registered_practitioner_in_role') }} prp
    GROUP BY 
        prp.person_id,
        prp.organisation_id
),

all_registrations AS (
    -- Gets all practice registrations for each person with sequencing
    SELECT 
        pp.person_id,
        p.sk_patient_id,
        prp.organisation_id AS practice_id,
        prp.registration_start_date,
        prp.registration_end_date,
        o.organisation_code AS practice_code,
        o.name AS practice_name,
        o.type_code AS practice_type_code,
        o.type_desc AS practice_type_desc,
        o.postcode AS practice_postcode,
        o.parent_organisation_id AS practice_parent_org_id,
        o.open_date AS practice_open_date,
        o.close_date AS practice_close_date,
        o.is_obsolete AS practice_is_obsolete,
        -- Sequence number (1 is oldest registration)
        ROW_NUMBER() OVER (
            PARTITION BY pp.person_id 
            ORDER BY 
                prp.registration_start_date ASC,
                prp.registration_end_date ASC NULLS LAST
        ) AS registration_sequence,
        -- Reverse sequence to identify current registration (1 is newest)
        ROW_NUMBER() OVER (
            PARTITION BY pp.person_id 
            ORDER BY 
                prp.registration_start_date DESC,
                prp.registration_end_date DESC NULLS FIRST
        ) AS reverse_sequence,
        -- Count total registrations per person
        COUNT(*) OVER (PARTITION BY pp.person_id) AS total_registrations
    FROM {{ ref('stg_olids_patient_person') }} pp
    JOIN {{ ref('stg_olids_patient') }} p 
        ON pp.patient_id = p.id
    JOIN practice_registration_periods prp 
        ON pp.person_id = prp.person_id
    JOIN {{ ref('stg_olids_organisation') }} o 
        ON prp.organisation_id = o.id
)

-- Select all registrations with additional flags
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
    registration_end_date,
    registration_sequence,
    total_registrations,
    -- Flag for current practice (reverse_sequence = 1)
    reverse_sequence = 1 AS is_current_practice
FROM all_registrations
ORDER BY person_id, registration_sequence 