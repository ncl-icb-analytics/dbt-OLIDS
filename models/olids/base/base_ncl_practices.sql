{{
    config(
        secure=true,
        alias='ncl_practices')
}}

/*
Base NCL Practices Reference
Identifies all GP practices belonging to North Central London ICB (QMJ).
Foundation filtering for all other base models that need NCL practice restriction.
Pattern: Reference data with STP filtering
*/

SELECT DISTINCT
    practicecode AS practice_code,
    practicename AS practice_name,
    stpcode AS stp_code,
    stpname AS stp_name
FROM {{ ref('stg_dictionary_organisationmatrixpracticeview') }}
WHERE stpcode = 'QMJ'  -- NHS NORTH CENTRAL LONDON INTEGRATED CARE BOARD
    AND practicecode IS NOT NULL