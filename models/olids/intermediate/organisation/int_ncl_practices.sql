{{
    config(
        materialized='table',
        tags=['intermediate', 'organisation', 'ncl', 'practices'],
        cluster_by=['practice_code'])
}}

/*
NCL Practices Lookup
Wrapper around base_ncl_practices for backward compatibility.
The actual filtering logic is now in the base layer.
*/

SELECT
    practice_code,
    practice_name,
    stp_code,
    stp_name
FROM {{ ref('base_olids_ncl_practices') }}