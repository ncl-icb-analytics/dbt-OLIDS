-- Investigation: Duplicate organisation_codes in Alpha database
-- Purpose: Identify why dim_practice has duplicate organisation_ids

-- Check for duplicate organisation_codes in stg_olids_organisation
WITH duplicate_org_codes AS (
    SELECT 
        organisation_code,
        COUNT(*) as record_count,
        COUNT(DISTINCT id) as unique_ids,
        LISTAGG(DISTINCT id, ', ') WITHIN GROUP (ORDER BY id) as organisation_ids
    FROM {{ ref('stg_olids_organisation') }}
    WHERE organisation_code IS NOT NULL
    GROUP BY organisation_code
    HAVING COUNT(*) > 1
)

SELECT 
    organisation_code,
    record_count,
    unique_ids,
    organisation_ids
FROM duplicate_org_codes
ORDER BY record_count DESC, organisation_code
LIMIT 20;

-- Check which practice codes are affected
WITH duplicate_orgs AS (
    SELECT organisation_code
    FROM {{ ref('stg_olids_organisation') }}
    WHERE organisation_code IS NOT NULL
    GROUP BY organisation_code
    HAVING COUNT(*) > 1
),
affected_practices AS (
    SELECT 
        dict.practicecode,
        dict.practicename,
        COUNT(DISTINCT org.id) as org_id_count,
        LISTAGG(DISTINCT org.id, ', ') WITHIN GROUP (ORDER BY org.id) as organisation_ids
    FROM {{ ref('stg_dictionary_organisationmatrixpracticeview') }} AS dict
    INNER JOIN {{ ref('stg_olids_organisation') }} AS org
        ON dict.practicecode = org.organisation_code
    WHERE dict.practicecode IN (SELECT organisation_code FROM duplicate_orgs)
        AND dict.stpcode IN ('QMJ', 'QMF', 'QRV', 'QWE', 'QKK')
    GROUP BY dict.practicecode, dict.practicename
)

SELECT 
    practicecode,
    practicename,
    org_id_count,
    organisation_ids
FROM affected_practices
ORDER BY org_id_count DESC, practicecode
LIMIT 20;