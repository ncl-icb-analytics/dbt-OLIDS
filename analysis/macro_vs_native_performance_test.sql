/*
Analysis: Performance comparison between get_observations macro vs native SQL
Purpose: Compare execution time and explain plans for different approaches
*/

-- Test 1: Simple count comparison for multiple clusters
WITH performance_test AS (
    -- Macro approach (multiple clusters)
    SELECT 
        'get_observations_macro' AS method,
        cluster_id,
        COUNT(DISTINCT person_id) AS distinct_persons,
        COUNT(*) AS total_observations,
        MIN(clinical_effective_date) AS earliest_date,
        MAX(clinical_effective_date) AS latest_date
    FROM (
        SELECT *, 'AST_COD' AS cluster_id FROM ({{ get_observations("'AST_COD'", 'UKHSA_FLU') }})
        UNION ALL
        SELECT *, 'DIAB_COD' AS cluster_id FROM ({{ get_observations("'DIAB_COD'", 'UKHSA_FLU') }})
        UNION ALL
        SELECT *, 'RESP_COD' AS cluster_id FROM ({{ get_observations("'RESP_COD'", 'UKHSA_FLU') }})
    )
    GROUP BY cluster_id
    
    UNION ALL
    
    -- Native approach (all clusters in one query)
    SELECT 
        'native_sql_join' AS method,
        cc.cluster_id,
        COUNT(DISTINCT pp."person_id") AS distinct_persons,
        COUNT(*) AS total_observations,
        MIN(o."clinical_effective_date") AS earliest_date,
        MAX(o."clinical_effective_date") AS latest_date
    FROM "Data_Store_OLIDS_UAT".olids_masked.observation o
    JOIN "Data_Store_OLIDS_UAT".olids_masked.patient_person pp ON o."patient_id" = pp."patient_id"
    JOIN "Data_Store_OLIDS_UAT".olids_terminology.concept_map mc ON o."observation_source_concept_id" = mc."source_code_id"
    JOIN "Data_Store_OLIDS_UAT".olids_terminology.concept c ON mc."target_code_id" = c."id"
    JOIN data_lab_olids_uat.reference.combined_codesets cc ON c."code" = cc.code
    WHERE cc.cluster_id IN ('AST_COD', 'DIAB_COD', 'RESP_COD')
        AND cc.source = 'UKHSA_FLU'
        AND o."clinical_effective_date" IS NOT NULL
    GROUP BY cc.cluster_id
)

SELECT 
    method,
    cluster_id,
    distinct_persons,
    total_observations,
    earliest_date,
    latest_date,
    ROUND(total_observations::FLOAT / distinct_persons, 2) AS avg_obs_per_person
FROM performance_test
ORDER BY cluster_id, method;

-- Test 2: Practice-level aggregation performance
-- WITH practice_comparison AS (
--     SELECT 
--         'macro_practice_agg' AS method,
--         COUNT(DISTINCT practice_code) AS practices_with_data,
--         SUM(person_count) AS total_persons,
--         AVG(person_count) AS avg_persons_per_practice,
--         MAX(person_count) AS max_persons_per_practice
--     FROM (
--         SELECT 
--             d.practice_code,
--             COUNT(DISTINCT d.person_id) AS person_count
--         FROM ({{ get_observations("'AST_COD'", 'UKHSA_FLU') }}) obs
--         JOIN {{ ref('dim_person_demographics') }} d ON obs.person_id = d.person_id
--         WHERE d.is_active = TRUE
--         GROUP BY d.practice_code
--     )
--     
--     UNION ALL
--     
--     SELECT 
--         'native_practice_agg' AS method,
--         COUNT(DISTINCT practice_code) AS practices_with_data,
--         SUM(person_count) AS total_persons,
--         AVG(person_count) AS avg_persons_per_practice,
--         MAX(person_count) AS max_persons_per_practice
--     FROM (
--         SELECT 
--             d.practice_code,
--             COUNT(DISTINCT d.person_id) AS person_count
--         FROM "Data_Store_OLIDS_UAT".olids_masked.observation o
--         JOIN "Data_Store_OLIDS_UAT".olids_masked.patient_person pp ON o."patient_id" = pp."patient_id"
--         JOIN "Data_Store_OLIDS_UAT".olids_terminology.concept_map mc ON o."observation_source_concept_id" = mc."source_code_id"
--         JOIN "Data_Store_OLIDS_UAT".olids_terminology.concept c ON mc."target_code_id" = c."id"
--         JOIN data_lab_olids_uat.reference.combined_codesets cc ON c."code" = cc.code
--         JOIN {{ ref('dim_person_demographics') }} d ON pp."person_id" = d.person_id
--         WHERE cc.cluster_id = 'AST_COD'
--             AND cc.source = 'UKHSA_FLU'
--             AND d.is_active = TRUE
--         GROUP BY d.practice_code
--     )
-- )
-- SELECT * FROM practice_comparison;