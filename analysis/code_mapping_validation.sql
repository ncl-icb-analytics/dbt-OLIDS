/*
Analysis: Validate code mapping consistency between macro and native approaches
Purpose: Ensure the get_observations macro uses the same code mappings as direct queries
*/

-- Compare the actual codes being used in both approaches
WITH macro_codes AS (
    SELECT DISTINCT
        cluster_id,
        mapped_concept_code,
        mapped_concept_display,
        code_description,
        COUNT(DISTINCT person_id) AS persons_with_code_macro
    FROM ({{ get_observations("'AST_COD', 'DIAB_COD', 'RESP_COD'", 'UKHSA_FLU') }})
    GROUP BY cluster_id, mapped_concept_code, mapped_concept_display, code_description
),

native_codes AS (
    SELECT DISTINCT
        cc.cluster_id,
        c."code" AS mapped_concept_code,
        c."display" AS mapped_concept_display,
        cc.code_description,
        COUNT(DISTINCT pp."person_id") AS persons_with_code_native
    FROM "Data_Store_OLIDS_UAT".olids_masked.observation o
    JOIN "Data_Store_OLIDS_UAT".olids_masked.patient_person pp ON o."patient_id" = pp."patient_id"
    JOIN "Data_Store_OLIDS_UAT".olids_terminology.concept_map mc ON o."observation_source_concept_id" = mc."source_code_id"
    JOIN "Data_Store_OLIDS_UAT".olids_terminology.concept c ON mc."target_code_id" = c."id"
    JOIN data_lab_olids_uat.reference.combined_codesets cc ON c."code" = cc.code
    WHERE cc.cluster_id IN ('AST_COD', 'DIAB_COD', 'RESP_COD')
        AND cc.source = 'UKHSA_FLU'
        AND o."clinical_effective_date" IS NOT NULL
    GROUP BY cc.cluster_id, c."code", c."display", cc.code_description
),

code_comparison AS (
    SELECT 
        COALESCE(m.cluster_id, n.cluster_id) AS cluster_id,
        COALESCE(m.mapped_concept_code, n.mapped_concept_code) AS concept_code,
        COALESCE(m.mapped_concept_display, n.mapped_concept_display) AS concept_display,
        COALESCE(m.code_description, n.code_description) AS code_description,
        COALESCE(m.persons_with_code_macro, 0) AS macro_persons,
        COALESCE(n.persons_with_code_native, 0) AS native_persons,
        CASE 
            WHEN m.mapped_concept_code IS NULL THEN 'Missing from macro'
            WHEN n.mapped_concept_code IS NULL THEN 'Missing from native'
            WHEN m.persons_with_code_macro = n.persons_with_code_native THEN 'Perfect match'
            ELSE 'Count difference'
        END AS status
    FROM macro_codes m
    FULL OUTER JOIN native_codes n
        ON m.cluster_id = n.cluster_id
        AND m.mapped_concept_code = n.mapped_concept_code
)

SELECT 
    cluster_id,
    concept_code,
    concept_display,
    code_description,
    macro_persons,
    native_persons,
    macro_persons - native_persons AS difference,
    status
FROM code_comparison
WHERE status != 'Perfect match'  -- Show only discrepancies
ORDER BY cluster_id, ABS(macro_persons - native_persons) DESC;

-- Summary by cluster
-- WITH cluster_summary AS (
--     SELECT 
--         cluster_id,
--         COUNT(*) AS total_codes,
--         SUM(CASE WHEN status = 'Perfect match' THEN 1 ELSE 0 END) AS matching_codes,
--         SUM(CASE WHEN status = 'Missing from macro' THEN 1 ELSE 0 END) AS missing_from_macro,
--         SUM(CASE WHEN status = 'Missing from native' THEN 1 ELSE 0 END) AS missing_from_native,
--         SUM(CASE WHEN status = 'Count difference' THEN 1 ELSE 0 END) AS count_differences
--     FROM code_comparison
--     GROUP BY cluster_id
-- )
-- SELECT 
--     cluster_id,
--     total_codes,
--     matching_codes,
--     ROUND(matching_codes * 100.0 / total_codes, 1) AS match_percentage,
--     missing_from_macro,
--     missing_from_native,
--     count_differences
-- FROM cluster_summary
-- ORDER BY cluster_id;