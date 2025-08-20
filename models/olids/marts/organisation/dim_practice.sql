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
    dict.practicecode AS practice_code,
    UPPER(dict.practicename) AS practice_name,
    
    -- PCN details
    dict.networkcode AS pcn_code,
    UPPER(dict.networkname) AS pcn_name,
    -- PCN name with borough prefix
    CASE 
        WHEN borough_map.pcn_borough IS NOT NULL 
        THEN borough_map.pcn_borough || ': ' || UPPER(dict.networkname)
        ELSE UPPER(dict.networkname)
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
    dict_org.startdate AS practice_start_date,
    dict_org.enddate AS practice_end_date,
    dict_org.address_line_1 AS practice_address_line_1,
    dict_org.address_line_2 AS practice_address_line_2,
    dict_org.address_line_3 AS practice_address_line_3,
    dict_org.address_line_4 AS practice_address_line_4,
    dict_org.address_line_5 AS practice_address_line_5,
    dict_org.firstcreated AS practice_first_created,
    dict_org.lastupdated AS practice_last_updated,
    
    -- Geographic details from Dictionary Postcode
    dict_pc.postcode AS practice_postcode_dict,
    dict_pc.lsoa AS practice_lsoa,
    dict_pc.msoa AS practice_msoa,
    dict_pc.latitude AS practice_latitude,
    dict_pc.longitude AS practice_longitude,
    
    -- Commissioner relationship
    dict.commissionercode AS commissioner_code,
    dict.commissionername AS commissioner_name,
    dict.sk_organisationid_commissioner AS sk_commissioner_id,
    
    -- STP relationship
    dict.stpcode AS stp_code,
    dict.stpname AS stp_name,
    dict.sk_organisationid_stp AS sk_stp_id,
    
    -- Dictionary surrogate keys
    dict.sk_organisationid_practice AS sk_practice_id,
    dict_org.sk_organisationid AS sk_practice_dict_id

FROM {{ ref('stg_dictionary_organisationmatrixpracticeview') }} AS dict
INNER JOIN {{ ref('stg_olids_organisation') }} AS org
    ON dict.practicecode = org.organisation_code
LEFT JOIN {{ ref('stg_dictionary_organisation') }} AS dict_org
    ON dict.practicecode = dict_org.organisation_code
LEFT JOIN {{ ref('stg_dictionary_postcode') }} AS dict_pc
    ON dict_org.sk_postcodeid = dict_pc.sk_postcodeid
LEFT JOIN {{ ref('int_organisation_borough_mapping') }} AS borough_map
    ON dict.practicecode = borough_map.practice_code
WHERE dict.practicecode IS NOT NULL
    AND dict.stpcode IN (
        'QMJ',  -- NHS NORTH CENTRAL LONDON INTEGRATED CARE BOARD
        'QMF',  -- NHS NORTH EAST LONDON INTEGRATED CARE BOARD
        'QRV',  -- NHS NORTH WEST LONDON INTEGRATED CARE BOARD
        'QWE',  -- NHS SOUTH WEST LONDON INTEGRATED CARE BOARD
        'QKK'   -- NHS SOUTH EAST LONDON INTEGRATED CARE BOARD
    )