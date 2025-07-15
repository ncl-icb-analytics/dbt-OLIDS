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
        network_code AS pcn_code,
        ARRAY_AGG(practice_code) AS member_practice_codes,
        COUNT(DISTINCT practice_code) AS member_practice_count
    FROM {{ ref('stg_dictionary_organisation_matrix_practice_view') }}
    WHERE network_code IS NOT NULL
        AND practice_code IS NOT NULL
        AND stp_code = 'QMJ'
    GROUP BY network_code
)

SELECT DISTINCT
    -- PCN identifiers
    dict.network_code AS pcn_code,
    dict.network_name AS pcn_name,
    -- PCN name with borough prefix
    CASE 
        WHEN borough_map.pcn_borough IS NOT NULL 
        THEN borough_map.pcn_borough || ': ' || dict.network_name
        ELSE dict.network_name
    END AS pcn_name_with_borough,
    dict.sk_organisation_id_network AS sk_pcn_id,
    
    -- Borough information
    borough_map.pcn_borough,
    
    -- PCN membership details
    pp.member_practice_count,
    pp.member_practice_codes,
    
    -- Enhanced PCN details from Dictionary Organisation
    dict_org.start_date AS pcn_start_date,
    dict_org.end_date AS pcn_end_date,
    dict_org.address_line_1 AS pcn_address_line_1,
    dict_org.address_line_2 AS pcn_address_line_2,
    dict_org.address_line_3 AS pcn_address_line_3,
    dict_org.address_line_4 AS pcn_address_line_4,
    dict_org.address_line_5 AS pcn_address_line_5,
    dict_org.first_created AS pcn_first_created,
    dict_org.last_updated AS pcn_last_updated,
    
    -- PCN organisational hierarchy
    dict.commissioner_code,
    dict.commissioner_name,
    dict.sk_organisation_id_commissioner AS sk_commissioner_id,
    
    -- STP relationship
    dict.stp_code,
    dict.stp_name,
    dict.sk_organisation_id_stp AS sk_stp_id,
    
    -- Dictionary surrogate keys
    dict_org.sk_organisation_id AS sk_pcn_dict_id
    
FROM {{ ref('stg_dictionary_organisation_matrix_practice_view') }} AS dict
INNER JOIN pcn_practices AS pp
    ON dict.network_code = pp.pcn_code
LEFT JOIN {{ ref('stg_dictionary_organisation') }} AS dict_org
    ON dict.network_code = dict_org.organisation_code
LEFT JOIN {{ ref('int_organisation_borough_mapping') }} AS borough_map
    ON dict.network_code = borough_map.pcn_code
WHERE dict.network_code IS NOT NULL
    AND dict.stp_code = 'QMJ'