{{
    config(
        materialized='table',
        tags=['intermediate', 'organisation', 'borough', 'mapping'],
        cluster_by=['practice_code'])
}}

/*
Organisation Borough Mapping
Maps practices and PCNs to North Central London boroughs using current organisational hierarchy.
Uses Dictionary.dbo.OrganisationDescendent for GP Practice -> PCN -> CCG -> ICB hierarchy.

Special handling:
- Medicus Select Care (Y03103) manually assigned to Enfield borough regardless of CCG history,
  as they provide cross-borough services but parent organisation is Enfield-based.
- Filters to London practices only (maintains existing behaviour)
*/

WITH borough_ccgs AS (
    -- Historic CCG codes mapped to London boroughs 
    -- North Central London
    SELECT '07R' AS ccg_code, 'Camden' AS borough UNION ALL
    SELECT '08H' AS ccg_code, 'Islington' AS borough UNION ALL
    SELECT '07M' AS ccg_code, 'Barnet' AS borough UNION ALL
    SELECT '07X' AS ccg_code, 'Enfield' AS borough UNION ALL
    SELECT '08D' AS ccg_code, 'Haringey' AS borough UNION ALL
    -- North East London  
    SELECT '07T' AS ccg_code, 'Waltham Forest' AS borough UNION ALL
    SELECT '08F' AS ccg_code, 'Hackney' AS borough UNION ALL
    SELECT '07W' AS ccg_code, 'Tower Hamlets' AS borough UNION ALL
    SELECT '08E' AS ccg_code, 'Newham' AS borough UNION ALL
    SELECT '08G' AS ccg_code, 'Redbridge' AS borough UNION ALL
    SELECT '08C' AS ccg_code, 'Havering' AS borough UNION ALL
    SELECT '07L' AS ccg_code, 'Barking and Dagenham' AS borough UNION ALL
    -- South East London historic CCGs
    SELECT '07Q' AS ccg_code, 'Bromley' AS borough UNION ALL
    SELECT '07N' AS ccg_code, 'Bexley' AS borough UNION ALL
    -- North West London
    SELECT '08J' AS ccg_code, 'Hillingdon' AS borough UNION ALL
    SELECT '08K' AS ccg_code, 'Harrow' AS borough UNION ALL
    SELECT '08L' AS ccg_code, 'Brent' AS borough UNION ALL
    SELECT '07P' AS ccg_code, 'Brent' AS borough UNION ALL
    SELECT '08M' AS ccg_code, 'Ealing' AS borough UNION ALL
    SELECT '08N' AS ccg_code, 'Hounslow' AS borough UNION ALL
    SELECT '08A' AS ccg_code, 'Hammersmith and Fulham' AS borough UNION ALL
    SELECT '08P' AS ccg_code, 'Kensington and Chelsea' AS borough UNION ALL
    SELECT '08Q' AS ccg_code, 'Westminster' AS borough UNION ALL
    -- South East London
    SELECT '08R' AS ccg_code, 'Greenwich' AS borough UNION ALL
    SELECT '08S' AS ccg_code, 'Bexley' AS borough UNION ALL
    SELECT '08T' AS ccg_code, 'Bromley' AS borough UNION ALL
    SELECT '08U' AS ccg_code, 'Lewisham' AS borough UNION ALL
    SELECT '08V' AS ccg_code, 'Southwark' AS borough UNION ALL
    SELECT '08W' AS ccg_code, 'Lambeth' AS borough UNION ALL
    -- South West London
    SELECT '08X' AS ccg_code, 'Wandsworth' AS borough UNION ALL
    SELECT '08Y' AS ccg_code, 'Merton' AS borough UNION ALL
    SELECT '08Z' AS ccg_code, 'Sutton' AS borough UNION ALL
    SELECT '09A' AS ccg_code, 'Croydon' AS borough UNION ALL
    SELECT '09C' AS ccg_code, 'Kingston' AS borough UNION ALL
    SELECT '09D' AS ccg_code, 'Richmond' AS borough
),

-- Get Practice to PCN relationships from OrganisationMatrixPracticeView (for current hierarchy)
practice_pcn AS (
    SELECT DISTINCT
        "PracticeCode" AS practice_code,
        "NetworkCode" AS pcn_code,
        "NetworkName" AS pcn_name,
        "CommissionerCode" AS ccg_code_from_matrix,
        "CommissionerName" AS ccg_name_from_matrix
    FROM {{ source('dictionary', 'OrganisationMatrixPracticeView') }}
    WHERE "PracticeCode" IS NOT NULL
        AND "NetworkCode" IS NOT NULL
        AND "STPCode" IN ('QMJ', 'QMF', 'QRV', 'QWE', 'QKK')  -- All London ICBs
),

-- Get historic CCG relationships from OrganisationDescendent paths (for borough mapping)
practice_borough_mapping AS (
    SELECT DISTINCT
        od."OrganisationCode_Child" AS practice_code,
        bc.ccg_code AS historic_ccg,
        bc.borough,
        od."Path",
        -- Get the most recent relationship for each practice-borough combination
        ROW_NUMBER() OVER (
            PARTITION BY od."OrganisationCode_Child", bc.borough 
            ORDER BY od."RelationshipStartDate" DESC
        ) AS rn
    FROM {{ source('dictionary', 'OrganisationDescendent') }} od
    INNER JOIN borough_ccgs bc 
        ON od."Path" LIKE '%[' || bc.ccg_code || ']%'
    WHERE od."OrganisationPrimaryRole_Child" = 'RO177' -- GP Practice
),

practice_borough_final AS (
    -- Get final practice-to-borough mapping (one borough per practice) using historic CCG paths
    SELECT 
        pbm.practice_code,
        CASE 
            -- Special exception for Medicus Select Care - manually set to Enfield
            WHEN pbm.practice_code = 'Y03103' THEN 'Enfield'
            ELSE pbm.borough
        END AS borough,
        pbm.historic_ccg
    FROM practice_borough_mapping pbm
    WHERE pbm.rn = 1
        -- For Medicus Select Care, only take the Enfield mapping to avoid duplicates
        AND (pbm.practice_code != 'Y03103' OR pbm.borough = 'Enfield')
),

pcn_borough_mapping AS (
    -- Map PCNs to boroughs based on their member practices
    SELECT 
        pp.pcn_code AS network_code,
        pbf.borough,
        COUNT(DISTINCT pbf.practice_code) AS borough_practice_count,
        -- Get the borough with the most practices for this PCN
        ROW_NUMBER() OVER (
            PARTITION BY pp.pcn_code 
            ORDER BY COUNT(DISTINCT pbf.practice_code) DESC
        ) AS borough_rank
    FROM practice_pcn pp
    INNER JOIN practice_borough_final pbf
        ON pp.practice_code = pbf.practice_code
    WHERE pp.pcn_code IS NOT NULL
    GROUP BY pp.pcn_code, pbf.borough
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

-- Final output with both practice and PCN mappings (maintain original interface)
SELECT
    -- Practice mapping
    pbf.practice_code,
    pbf.borough AS practice_borough,
    pbf.historic_ccg AS practice_historic_ccg,
    
    -- PCN mapping
    pp.pcn_code AS network_code,
    pcnbf.borough AS pcn_borough,
    pcnbf.borough_practice_count AS pcn_borough_practice_count

FROM practice_borough_final pbf
LEFT JOIN practice_pcn pp
    ON pbf.practice_code = pp.practice_code
LEFT JOIN pcn_borough_final pcnbf
    ON pp.pcn_code = pcnbf.network_code