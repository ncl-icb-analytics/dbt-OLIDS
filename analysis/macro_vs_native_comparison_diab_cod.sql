/*
Analysis: Compare get_observations macro vs native SQL joins for DIAB_COD
Purpose: Validate macro results for diabetes codes across practices
*/

-- Method 1: Using get_observations macro
WITH macro_results AS (
    SELECT 
        d.practice_code,
        d.practice_name,
        COUNT(DISTINCT d.person_id) AS person_count_macro,
        MIN(obs.clinical_effective_date) AS earliest_date_macro,
        MAX(obs.clinical_effective_date) AS latest_date_macro
    FROM ({{ get_observations("'DIAB_COD'", 'UKHSA_FLU') }}) obs
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
        COUNT(DISTINCT d.person_id) AS person_count_native,
        MIN(o."clinical_effective_date") AS earliest_date_native,
        MAX(o."clinical_effective_date") AS latest_date_native
    FROM "Data_Store_OLIDS_UAT".olids_masked.observation o
    JOIN "Data_Store_OLIDS_UAT".olids_masked.patient_person pp ON o."patient_id" = pp."patient_id"
    JOIN "Data_Store_OLIDS_UAT".olids_terminology.concept_map mc ON o."observation_source_concept_id" = mc."source_code_id"
    JOIN "Data_Store_OLIDS_UAT".olids_terminology.concept c ON mc."target_code_id" = c."id"
    JOIN data_lab_olids_uat.reference.combined_codesets cc ON c."code" = cc.code
    JOIN {{ ref('dim_person_demographics') }} d ON pp."person_id" = d.person_id
    WHERE cc.cluster_id = 'DIAB_COD'
        AND cc.source = 'UKHSA_FLU'
        AND d.is_active = TRUE
        AND o."clinical_effective_date" IS NOT NULL
    GROUP BY d.practice_code, d.practice_name
)

-- Comparison with date validation
SELECT 
    COALESCE(m.practice_code, n.practice_code) AS practice_code,
    COALESCE(m.practice_name, n.practice_name) AS practice_name,
    COALESCE(m.person_count_macro, 0) AS macro_count,
    COALESCE(n.person_count_native, 0) AS native_count,
    COALESCE(m.person_count_macro, 0) - COALESCE(n.person_count_native, 0) AS count_difference,
    m.earliest_date_macro,
    n.earliest_date_native,
    m.latest_date_macro,
    n.latest_date_native,
    CASE 
        WHEN COALESCE(m.person_count_macro, 0) = COALESCE(n.person_count_native, 0) 
             AND m.earliest_date_macro = n.earliest_date_native 
             AND m.latest_date_macro = n.latest_date_native 
        THEN '✓ Perfect match'
        WHEN ABS(COALESCE(m.person_count_macro, 0) - COALESCE(n.person_count_native, 0)) <= 2 
        THEN '⚠ Minor difference'
        ELSE '❌ Significant difference'
    END AS validation_status
FROM macro_results m
FULL OUTER JOIN native_results n
    ON m.practice_code = n.practice_code
ORDER BY ABS(COALESCE(m.person_count_macro, 0) - COALESCE(n.person_count_native, 0)) DESC, practice_code;