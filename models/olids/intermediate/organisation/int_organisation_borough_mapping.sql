{{
    config(
        materialized='table',
        tags=['intermediate', 'organisation', 'borough', 'mapping'],
        cluster_by=['practice_code'])
}}

/*
Organisation Borough Mapping
Maps practices and PCNs to North Central London boroughs based on historic CCG relationships.
Uses Dictionary.dbo.OrganisationDescendent to trace organisational hierarchy paths.

Special handling:
- Medicus Select Care (Y03103) manually assigned to Enfield borough regardless of CCG history,
  as they provide cross-borough services but parent organisation is Enfield-based.
*/

WITH borough_ccgs AS (
    -- Correct CCG codes for North Central London boroughs
    SELECT '07R' AS ccg_code, 'Camden' AS borough UNION ALL
    SELECT '08H' AS ccg_code, 'Islington' AS borough UNION ALL
    SELECT '07M' AS ccg_code, 'Barnet' AS borough UNION ALL
    SELECT '07X' AS ccg_code, 'Enfield' AS borough UNION ALL
    SELECT '08D' AS ccg_code, 'Haringey' AS borough
),

practice_borough_mapping AS (
    SELECT DISTINCT
        od.organisationcode_child AS practice_code,
        bc.ccg_code AS historic_ccg,
        bc.borough,
        od.path,
        -- Get the most recent relationship for each practice-borough combination
        ROW_NUMBER() OVER (
            PARTITION BY od.organisationcode_child, bc.borough 
            ORDER BY od.relationshipstartdate DESC
        ) AS rn
    FROM {{ ref('stg_dictionary_organisationdescendent') }} AS od
    INNER JOIN borough_ccgs AS bc 
        ON od.path LIKE '%[' || bc.ccg_code || ']%'
    WHERE od.organisationprimaryrole_child = 'RO177' -- GP Practice
),

practice_borough_final AS (
    -- Get final practice-to-borough mapping (one borough per practice)
    SELECT 
        practice_code,
        CASE 
            -- Special exception for Medicus Select Care - manually set to Enfield
            WHEN practice_code = 'Y03103' THEN 'Enfield'
            ELSE borough
        END AS borough,
        historic_ccg
    FROM practice_borough_mapping
    WHERE rn = 1
        -- For Medicus Select Care, only take the Enfield mapping to avoid duplicates
        AND (practice_code != 'Y03103' OR borough = 'Enfield')
),

pcn_borough_mapping AS (
    -- Map PCNs to boroughs based on their member practices
    SELECT 
        dict.networkcode AS network_code,
        pbf.borough,
        COUNT(DISTINCT pbf.practice_code) AS borough_practice_count,
        -- Get the borough with the most practices for this PCN
        ROW_NUMBER() OVER (
            PARTITION BY dict.networkcode 
            ORDER BY COUNT(DISTINCT pbf.practice_code) DESC
        ) AS borough_rank
    FROM {{ ref('stg_dictionary_organisationmatrixpracticeview') }} AS dict
    INNER JOIN practice_borough_final AS pbf
        ON dict.practicecode = pbf.practice_code
    WHERE dict.stpcode = 'QMJ'
    GROUP BY dict.networkcode, pbf.borough
),

pcn_borough_final AS (
    -- Get final PCN-to-borough mapping (one borough per PCN)
    SELECT 
        network_code,
        borough,
        borough_practice_count
    FROM pcn_borough_mapping
    WHERE borough_rank = 1
)

-- Final output with both practice and PCN mappings
SELECT
    -- Practice mapping
    pbf.practice_code AS practice_code,
    pbf.borough AS practice_borough,
    pbf.historic_ccg AS practice_historic_ccg,
    
    -- PCN mapping (join via practice code to get PCN)
    dict.networkcode AS network_code,
    pcnbf.borough AS pcn_borough,
    pcnbf.borough_practice_count AS pcn_borough_practice_count

FROM practice_borough_final AS pbf
LEFT JOIN {{ ref('stg_dictionary_organisationmatrixpracticeview') }} AS dict
    ON pbf.practice_code = dict.practicecode
    AND dict.stpcode = 'QMJ'
LEFT JOIN pcn_borough_final AS pcnbf
    ON dict.networkcode = pcnbf.network_code