-- Check if multiple practice codes map to the same organisation_id
WITH org_practice_mapping AS (
    SELECT DISTINCT
        org.id as organisation_id,
        org.organisation_code,
        dict.practicecode,
        dict.practicename
    FROM {{ ref('stg_dictionary_organisationmatrixpracticeview') }} AS dict
    INNER JOIN {{ ref('stg_olids_organisation') }} AS org
        ON dict.practicecode = org.organisation_code
    WHERE dict.stpcode IN ('QMJ', 'QMF', 'QRV', 'QWE', 'QKK')
),
duplicates AS (
    SELECT 
        organisation_id,
        COUNT(DISTINCT practicecode) as practice_count,
        LISTAGG(DISTINCT practicecode, ', ') WITHIN GROUP (ORDER BY practicecode) as practice_codes
    FROM org_practice_mapping
    GROUP BY organisation_id
    HAVING COUNT(DISTINCT practicecode) > 1
)
SELECT * FROM duplicates
ORDER BY practice_count DESC, organisation_id;