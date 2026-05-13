{{
    config(
        materialized='table',
        tags=['intermediate', 'organisation', 'wnl', 'practices'],
        cluster_by=['practice_code'])
}}

/*
WNL Practices Lookup
GP practices under the West and North London ICB (Z9B2Z, post-April 2026 merger).
Retains legacy NCL (QMJ) and NWL (QRV) STP codes as a safety net for ODS records
not yet repointed to Z9B2Z. Foundation filter for all OLIDS base models.
*/

SELECT DISTINCT
    "PracticeCode" AS practice_code,
    "PracticeName" AS practice_name,
    "STPCode" AS stp_code,
    "STPName" AS stp_name
FROM {{ source('dictionary', 'OrganisationMatrixPracticeView') }}
WHERE "STPCode" IN ('Z9B2Z', 'QMJ', 'QRV')
    AND "PracticeCode" IS NOT NULL
