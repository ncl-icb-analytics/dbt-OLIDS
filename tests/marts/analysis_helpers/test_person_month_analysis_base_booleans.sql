-- Test that all boolean condition flags are properly set (not NULL)
-- This ensures COALESCE(flag, FALSE) is working correctly

WITH boolean_nulls AS (
    SELECT 
        person_id,
        analysis_month,
        -- Check if any boolean is NULL
        CASE WHEN has_dm IS NULL THEN 'has_dm' END as null_flag_1,
        CASE WHEN has_htn IS NULL THEN 'has_htn' END as null_flag_2,
        CASE WHEN has_ckd IS NULL THEN 'has_ckd' END as null_flag_3,
        CASE WHEN has_copd IS NULL THEN 'has_copd' END as null_flag_4,
        CASE WHEN has_ast IS NULL THEN 'has_ast' END as null_flag_5,
        CASE WHEN has_af IS NULL THEN 'has_af' END as null_flag_6,
        CASE WHEN has_chd IS NULL THEN 'has_chd' END as null_flag_7,
        CASE WHEN has_dep IS NULL THEN 'has_dep' END as null_flag_8,
        CASE WHEN has_smi IS NULL THEN 'has_smi' END as null_flag_9,
        CASE WHEN has_any_condition IS NULL THEN 'has_any_condition' END as null_flag_10,
        CASE WHEN has_any_new_episode IS NULL THEN 'has_any_new_episode' END as null_flag_11
    FROM {{ ref('person_month_analysis_base') }}
    LIMIT 1000  -- Sample for performance
),

issues AS (
    SELECT 
        person_id,
        analysis_month,
        COALESCE(
            null_flag_1, null_flag_2, null_flag_3, null_flag_4, null_flag_5,
            null_flag_6, null_flag_7, null_flag_8, null_flag_9, null_flag_10,
            null_flag_11
        ) as null_boolean_field
    FROM boolean_nulls
    WHERE COALESCE(
        null_flag_1, null_flag_2, null_flag_3, null_flag_4, null_flag_5,
        null_flag_6, null_flag_7, null_flag_8, null_flag_9, null_flag_10,
        null_flag_11
    ) IS NOT NULL
)

SELECT * FROM issues