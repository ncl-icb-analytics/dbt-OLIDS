-- Check which organisation_ids are duplicated in dim_practice
WITH duplicate_check AS (
    SELECT 
        organisation_id,
        COUNT(*) as count_records,
        LISTAGG(practice_code, ', ') WITHIN GROUP (ORDER BY practice_code) as practice_codes,
        LISTAGG(practice_name, ' | ') WITHIN GROUP (ORDER BY practice_code) as practice_names
    FROM {{ ref('dim_practice') }}
    GROUP BY organisation_id
    HAVING COUNT(*) > 1
)
SELECT * FROM duplicate_check
ORDER BY count_records DESC, organisation_id;