/*
Analysis: Compare get_observations macro vs native SQL joins for AST_COD
Purpose: Validate that the macro produces equivalent results to direct table joins
*/

-- Method 1: Using get_observations macro
WITH macro_results AS (
    SELECT 
        d.practice_code,
        d.practice_name,
        COUNT(DISTINCT d.person_id) AS person_count_macro
    FROM ({{ get_observations("'AST_COD'", 'UKHSA_FLU') }}) obs
    JOIN {{ ref('dim_person_demographics') }} d ON obs.person_id = d.person_id
    WHERE d.is_active = TRUE
        AND obs.clinical_effective_date IS NOT NULL
    GROUP BY d.practice_code, d.practice_name
),

-- Method 2: Native SQL joins using combined_codesets
native_results AS (
    SELECT 
        d.practice_code,
        d.practice_name,
        COUNT(DISTINCT d.person_id) AS person_count_native
    FROM "Data_Store_OLIDS_UAT".olids_masked.observation o
    JOIN "Data_Store_OLIDS_UAT".olids_masked.patient_person pp ON o."patient_id" = pp."patient_id"
    JOIN "Data_Store_OLIDS_UAT".olids_terminology.concept_map mc ON o."observation_source_concept_id" = mc."source_code_id"
    JOIN "Data_Store_OLIDS_UAT".olids_terminology.concept c ON mc."target_code_id" = c."id"
    JOIN data_lab_olids_uat.reference.combined_codesets cc ON c."code" = cc.code
    JOIN {{ ref('dim_person_demographics') }} d ON pp."person_id" = d.person_id
    WHERE cc.cluster_id = 'AST_COD'
        AND cc.source = 'UKHSA_FLU'
        AND d.is_active = TRUE
        AND o."clinical_effective_date" IS NOT NULL
    GROUP BY d.practice_code, d.practice_name
),

-- Comparison analysis
comparison AS (
    SELECT 
        COALESCE(m.practice_code, n.practice_code) AS practice_code,
        COALESCE(m.practice_name, n.practice_name) AS practice_name,
        COALESCE(m.person_count_macro, 0) AS macro_count,
        COALESCE(n.person_count_native, 0) AS native_count,
        COALESCE(m.person_count_macro, 0) - COALESCE(n.person_count_native, 0) AS difference,
        CASE 
            WHEN COALESCE(n.person_count_native, 0) = 0 THEN NULL
            ELSE ROUND((COALESCE(m.person_count_macro, 0) - COALESCE(n.person_count_native, 0)) * 100.0 / n.person_count_native, 2)
        END AS percent_difference
    FROM macro_results m
    FULL OUTER JOIN native_results n
        ON m.practice_code = n.practice_code
)

SELECT 
    practice_code,
    practice_name,
    macro_count,
    native_count,
    difference,
    percent_difference,
    CASE 
        WHEN difference = 0 THEN '✓ Match'
        WHEN ABS(difference) <= 5 THEN '⚠ Minor difference'
        ELSE '❌ Significant difference'
    END AS status
FROM comparison
ORDER BY ABS(difference) DESC, practice_code;

-- Summary statistics
-- SELECT 
--     COUNT(*) AS total_practices,
--     SUM(CASE WHEN difference = 0 THEN 1 ELSE 0 END) AS exact_matches,
--     SUM(CASE WHEN ABS(difference) <= 5 THEN 1 ELSE 0 END) AS close_matches,
--     AVG(ABS(difference)) AS avg_absolute_difference,
--     MAX(ABS(difference)) AS max_absolute_difference
-- FROM comparison;