/*
Analysis: Understand deduplication behavior in get_observations macro
Purpose: Examine how the macro handles duplicate observations vs native SQL
*/

-- Analyze deduplication patterns
WITH macro_with_duplicates AS (
    -- Modified macro query to see duplicates before QUALIFY
    SELECT
        observation_id,
        patient_id,
        person_id,
        clinical_effective_date,
        mapped_concept_code,
        mapped_concept_display,
        cluster_id,
        ROW_NUMBER() OVER (
            PARTITION BY observation_id, cluster_id 
            ORDER BY mapped_concept_code
        ) AS rn_macro
    FROM (
        SELECT
            o.id AS observation_id,
            o.patient_id,
            pp.person_id,
            o.clinical_effective_date,
            sc.mapped_concept_code,
            sc.mapped_concept_display,
            sc.cluster_id
        FROM {{ ref('stg_olids_observation') }} o
        JOIN (
            SELECT *
            FROM {{ ref('int_mapped_concepts') }}
            WHERE UPPER(cluster_id) IN ('AST_COD')
            AND source = 'UKHSA_FLU'
        ) sc ON o.observation_source_concept_id = sc.source_code_id
        JOIN {{ ref('int_patient_person_unique') }} pp ON o.patient_id = pp.patient_id
        WHERE o.clinical_effective_date IS NOT NULL
    )
),

native_with_duplicates AS (
    SELECT
        o."id" AS observation_id,
        o."patient_id",
        pp."person_id",
        o."clinical_effective_date",
        c."code" AS mapped_concept_code,
        c."display" AS mapped_concept_display,
        cc.cluster_id,
        ROW_NUMBER() OVER (
            PARTITION BY o."id", cc.cluster_id 
            ORDER BY c."code"
        ) AS rn_native
    FROM "Data_Store_OLIDS_UAT".olids_masked.observation o
    JOIN "Data_Store_OLIDS_UAT".olids_masked.patient_person pp ON o."patient_id" = pp."patient_id"
    JOIN "Data_Store_OLIDS_UAT".olids_terminology.concept_map mc ON o."observation_source_concept_id" = mc."source_code_id"
    JOIN "Data_Store_OLIDS_UAT".olids_terminology.concept c ON mc."target_code_id" = c."id"
    JOIN data_lab_olids_uat.reference.combined_codesets cc ON c."code" = cc.code
    WHERE cc.cluster_id = 'AST_COD'
        AND cc.source = 'UKHSA_FLU'
        AND o."clinical_effective_date" IS NOT NULL
),

duplication_analysis AS (
    SELECT 
        'Before deduplication' AS stage,
        'macro' AS method,
        COUNT(*) AS total_rows,
        COUNT(DISTINCT observation_id) AS unique_observations,
        COUNT(*) - COUNT(DISTINCT observation_id) AS duplicate_rows,
        COUNT(DISTINCT person_id) AS unique_persons
    FROM macro_with_duplicates
    
    UNION ALL
    
    SELECT 
        'After deduplication' AS stage,
        'macro' AS method,
        COUNT(*) AS total_rows,
        COUNT(DISTINCT observation_id) AS unique_observations,
        COUNT(*) - COUNT(DISTINCT observation_id) AS duplicate_rows,
        COUNT(DISTINCT person_id) AS unique_persons
    FROM macro_with_duplicates
    WHERE rn_macro = 1
    
    UNION ALL
    
    SELECT 
        'Before deduplication' AS stage,
        'native' AS method,
        COUNT(*) AS total_rows,
        COUNT(DISTINCT observation_id) AS unique_observations,
        COUNT(*) - COUNT(DISTINCT observation_id) AS duplicate_rows,
        COUNT(DISTINCT person_id) AS unique_persons
    FROM native_with_duplicates
    
    UNION ALL
    
    SELECT 
        'After deduplication' AS stage,
        'native' AS method,
        COUNT(*) AS total_rows,
        COUNT(DISTINCT observation_id) AS unique_observations,
        COUNT(*) - COUNT(DISTINCT observation_id) AS duplicate_rows,
        COUNT(DISTINCT person_id) AS unique_persons
    FROM native_with_duplicates
    WHERE rn_native = 1
)

SELECT 
    stage,
    method,
    total_rows,
    unique_observations,
    duplicate_rows,
    unique_persons,
    ROUND(duplicate_rows * 100.0 / total_rows, 2) AS duplicate_percentage
FROM duplication_analysis
ORDER BY method, stage;

-- Show examples of observations that have multiple code mappings
-- WITH multi_code_observations AS (
--     SELECT 
--         observation_id,
--         person_id,
--         clinical_effective_date,
--         COUNT(*) AS code_count,
--         STRING_AGG(mapped_concept_code, ', ') AS all_codes,
--         STRING_AGG(mapped_concept_display, ' | ') AS all_displays
--     FROM macro_with_duplicates
--     GROUP BY observation_id, person_id, clinical_effective_date
--     HAVING COUNT(*) > 1
--     ORDER BY code_count DESC
--     LIMIT 10
-- )
-- SELECT 
--     'Examples of observations with multiple code mappings:' AS analysis_type,
--     observation_id,
--     clinical_effective_date,
--     code_count,
--     all_codes,
--     all_displays
-- FROM multi_code_observations;