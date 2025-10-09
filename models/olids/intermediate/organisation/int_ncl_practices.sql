{{
    config(
        materialized='table',
        tags=['intermediate', 'organisation', 'ncl', 'practices'],
        cluster_by=['practice_code'])
}}

/*
NCL Practices Lookup
Identifies all GP practices belonging to North Central London ICB (QMJ).
Foundation filtering for all base models requiring NCL practice restriction.
*/

SELECT DISTINCT
    "PracticeCode" AS practice_code,
    "PracticeName" AS practice_name,
    "STPCode" AS stp_code,
    "STPName" AS stp_name
FROM {{ source('dictionary', 'OrganisationMatrixPracticeView') }}
WHERE "STPCode" = 'QMJ'  -- NHS NORTH CENTRAL LONDON INTEGRATED CARE BOARD
    AND "PracticeCode" IS NOT NULL