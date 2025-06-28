{{
    config(
        materialized='view',
        tags=['staging', 'practice', 'geography']
    )
}}

/*
Staging model for practice neighbourhood lookup data.
Provides practice geographic information including local authority and neighbourhood.
Note: This is placeholder data for dummy environment - geography will be limited.
*/

SELECT
    "PRACTICECODE" AS practice_code,
    "PRACTICENAME" AS practice_name,
    "LOCALAUTHORITY" AS local_authority,
    "PRACTICENEIGHBOURHOOD" AS practice_neighbourhood,
    "PCNCODE" AS pcn_code
FROM {{ source('POPULATION_HEALTH', 'PRACTICE_NEIGHBOURHOOD_LOOKUP') }}
