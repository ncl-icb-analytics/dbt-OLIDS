{{
    config(
        materialized='table',
        tags=['intermediate', 'organisation', 'ncl', 'practices'],
        cluster_by=['practice_code'])
}}

/*
NCL Practices Lookup
Identifies all GP practices belonging to North Central London ICB (QMJ).
Used for filtering OLIDS data to NCL practices only.
*/

SELECT DISTINCT
    practicecode AS practice_code,
    practicename AS practice_name,
    stpcode AS stp_code,
    stpname AS stp_name
FROM {{ ref('stg_dictionary_organisationmatrixpracticeview') }}
WHERE stpcode = 'QMJ'  -- NHS NORTH CENTRAL LONDON INTEGRATED CARE BOARD
    AND practicecode IS NOT NULL