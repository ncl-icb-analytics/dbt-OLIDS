{{
    config(
        materialized='table',
        cluster_by=['person_id'])
}}

/*
Person Postcode Hash
Gets the current postcode_hash for each person via patient address.
*/

WITH current_addresses AS (
    -- Get the latest address for each person using SCD2 logic
    SELECT
        pp.person_id,
        pa.postcode_hash,
        pa.start_date,
        pa.end_date
    FROM {{ ref('int_patient_person_unique') }} pp
    INNER JOIN {{ ref('stg_olids_patient_address') }} pa
        ON pp.patient_id = pa.patient_id
    WHERE pa.start_date IS NOT NULL
    QUALIFY ROW_NUMBER() OVER (
        PARTITION BY pp.person_id
        ORDER BY
            CASE WHEN pa.end_date IS NULL THEN 0 ELSE 1 END,  -- Active addresses first
            pa.start_date DESC,
            pa.lds_datetime_data_acquired DESC
    ) = 1
)

SELECT
    person_id,
    postcode_hash,
    start_date as address_start_date,
    end_date as address_end_date,
    CASE WHEN end_date IS NULL THEN TRUE ELSE FALSE END as is_current_address
FROM current_addresses