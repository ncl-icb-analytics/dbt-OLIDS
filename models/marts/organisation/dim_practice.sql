{{
    config(
        materialized='table',
        tags=['dimension', 'practice', 'organisation'],
        cluster_by=['practice_code'])
}}

/*
Practice Dimension
Contains comprehensive practice details including organisational hierarchy.
Sources from Dictionary.dbo.OrganisationMatrixPracticeView and OLIDS organisation data.
*/

SELECT
    -- OLIDS identifiers
    org.id AS organisation_id,
    
    -- Practice details
    dict.practice_code,
    dict.practice_name,
    
    -- PCN details
    dict.network_code AS pcn_code,
    dict.network_name AS pcn_name,
    -- PCN name with borough prefix
    CASE 
        WHEN borough_map.pcn_borough IS NOT NULL 
        THEN borough_map.pcn_borough || ': ' || dict.network_name
        ELSE dict.network_name
    END AS pcn_name_with_borough,
    
    -- Borough information
    borough_map.practice_borough,
    borough_map.pcn_borough,
    borough_map.practice_historic_ccg,
    
    -- Practice organisational details from OLIDS
    org.type_code AS practice_type_code,
    org.type_desc AS practice_type_desc,
    org.postcode AS practice_postcode,
    org.open_date AS practice_open_date,
    org.close_date AS practice_close_date,
    org.is_obsolete AS practice_is_obsolete,
    org.parent_organisation_id AS practice_parent_organisation_id,
    
    -- Enhanced practice details from Dictionary Organisation
    dict_org.start_date AS practice_start_date,
    dict_org.end_date AS practice_end_date,
    dict_org.address_line_1 AS practice_address_line_1,
    dict_org.address_line_2 AS practice_address_line_2,
    dict_org.address_line_3 AS practice_address_line_3,
    dict_org.address_line_4 AS practice_address_line_4,
    dict_org.address_line_5 AS practice_address_line_5,
    dict_org.first_created AS practice_first_created,
    dict_org.last_updated AS practice_last_updated,
    
    -- Geographic details from Dictionary Postcode
    dict_pc.postcode AS practice_postcode_dict,
    dict_pc.lsoa AS practice_lsoa,
    dict_pc.msoa AS practice_msoa,
    dict_pc.latitude AS practice_latitude,
    dict_pc.longitude AS practice_longitude,
    
    -- Commissioner relationship
    dict.commissioner_code,
    dict.commissioner_name,
    dict.sk_organisation_id_commissioner AS sk_commissioner_id,
    
    -- STP relationship
    dict.stp_code,
    dict.stp_name,
    dict.sk_organisation_id_stp AS sk_stp_id,
    
    -- Dictionary surrogate keys
    dict.sk_organisation_id_practice AS sk_practice_id,
    dict_org.sk_organisation_id AS sk_practice_dict_id

FROM {{ ref('stg_dictionary_organisation_matrix_practice_view') }} AS dict
INNER JOIN {{ ref('stg_olids_organisation') }} AS org
    ON dict.practice_code = org.organisation_code
LEFT JOIN {{ ref('stg_dictionary_organisation') }} AS dict_org
    ON dict.practice_code = dict_org.organisation_code
LEFT JOIN {{ ref('stg_dictionary_postcode') }} AS dict_pc
    ON dict_org.sk_postcode_id = dict_pc.sk_postcode_id
LEFT JOIN {{ ref('int_organisation_borough_mapping') }} AS borough_map
    ON dict.practice_code = borough_map.practice_code
WHERE dict.practice_code IS NOT NULL
    AND dict.stp_code = 'QMJ'