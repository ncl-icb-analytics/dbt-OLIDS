{{
    config(
        materialized='table',
        tags=['dimension', 'pcn', 'organisation'],
        cluster_by=['pcn_code'])
}}

/*
PCN (Primary Care Network) Dimension
Contains PCN details and member practice information.
Sources from Dictionary.dbo.OrganisationMatrixPracticeView.
Includes borough context for PCN naming.
*/

WITH pcn_practices AS (
    SELECT
        networkcode AS pcn_code,
        ARRAY_AGG(practicecode) AS member_practice_codes,
        COUNT(DISTINCT practicecode) AS member_practice_count
    FROM {{ ref('stg_dictionary_organisationmatrixpracticeview') }}
    WHERE networkcode IS NOT NULL
        AND practicecode IS NOT NULL
        AND stpcode = 'QMJ'
    GROUP BY networkcode
)

SELECT DISTINCT
    -- PCN identifiers
    dict.networkcode AS pcn_code,
    dict.networkname AS pcn_name,
    -- PCN name with borough prefix
    CASE 
        WHEN borough_map.pcn_borough IS NOT NULL 
        THEN borough_map.pcn_borough || ': ' || dict.networkname
        ELSE dict.networkname
    END AS pcn_name_with_borough,
    dict.sk_organisationid_network AS sk_pcn_id,
    
    -- Borough information
    borough_map.pcn_borough,
    
    -- PCN membership details
    pp.member_practice_count,
    pp.member_practice_codes,
    
    -- Enhanced PCN details from Dictionary Organisation
    dict_org.startdate AS pcn_start_date,
    dict_org.enddate AS pcn_end_date,
    dict_org.address_line_1 AS pcn_address_line_1,
    dict_org.address_line_2 AS pcn_address_line_2,
    dict_org.address_line_3 AS pcn_address_line_3,
    dict_org.address_line_4 AS pcn_address_line_4,
    dict_org.address_line_5 AS pcn_address_line_5,
    dict_org.firstcreated AS pcn_first_created,
    dict_org.lastupdated AS pcn_last_updated,
    
    -- PCN organisational hierarchy
    dict.commissionercode,
    dict.commissionername,
    dict.sk_organisationid_commissioner AS sk_commissioner_id,
    
    -- STP relationship
    dict.stpcode,
    dict.stpname,
    dict.sk_organisationid_stp AS sk_stp_id,
    
    -- Dictionary surrogate keys
    dict_org.sk_organisationid AS sk_pcn_dict_id
    
FROM {{ ref('stg_dictionary_organisationmatrixpracticeview') }} AS dict
INNER JOIN pcn_practices AS pp
    ON dict.networkcode = pp.pcn_code
LEFT JOIN {{ ref('stg_dictionary_organisation') }} AS dict_org
    ON dict.networkcode = dict_org.organisation_code
LEFT JOIN {{ ref('int_organisation_borough_mapping') }} AS borough_map
    ON dict.networkcode = borough_map.network_code
WHERE dict.networkcode IS NOT NULL
    AND dict.stpcode = 'QMJ'