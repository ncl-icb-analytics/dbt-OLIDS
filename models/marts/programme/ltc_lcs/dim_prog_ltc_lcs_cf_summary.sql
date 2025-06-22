{{ config(
    materialized='table',
    post_hook="ALTER TABLE {{ this }} SET COMMENT = 'LTC/LCS case finding summary: Aggregated view of all case finding indicators per person'"
) }}

-- LTC/LCS case finding summary dimension
-- Provides a unified view of all case finding indicators per person

SELECT
    base.person_id,
    base.age,
    base.practice_code,
    
    -- AF indicators
    CASE WHEN af_61.person_id IS NOT NULL THEN TRUE ELSE FALSE END AS in_af_61,
    CASE WHEN af_62.person_id IS NOT NULL THEN TRUE ELSE FALSE END AS in_af_62,
    
    -- CKD indicators  
    CASE WHEN ckd_61.person_id IS NOT NULL THEN TRUE ELSE FALSE END AS in_ckd_61,
    CASE WHEN ckd_62.person_id IS NOT NULL THEN TRUE ELSE FALSE END AS in_ckd_62,
    CASE WHEN ckd_63.person_id IS NOT NULL THEN TRUE ELSE FALSE END AS in_ckd_63,
    CASE WHEN ckd_64.person_id IS NOT NULL THEN TRUE ELSE FALSE END AS in_ckd_64,
    
    -- CVD indicators
    CASE WHEN cvd_61.person_id IS NOT NULL THEN TRUE ELSE FALSE END AS in_cvd_61,
    CASE WHEN cvd_62.person_id IS NOT NULL THEN TRUE ELSE FALSE END AS in_cvd_62,
    CASE WHEN cvd_63.person_id IS NOT NULL THEN TRUE ELSE FALSE END AS in_cvd_63,
    CASE WHEN cvd_64.person_id IS NOT NULL THEN TRUE ELSE FALSE END AS in_cvd_64,
    CASE WHEN cvd_65.person_id IS NOT NULL THEN TRUE ELSE FALSE END AS in_cvd_65,
    CASE WHEN cvd_66.person_id IS NOT NULL THEN TRUE ELSE FALSE END AS in_cvd_66,
    
    -- Diabetes indicators
    CASE WHEN dm_61.person_id IS NOT NULL THEN TRUE ELSE FALSE END AS in_dm_61,
    CASE WHEN dm_62.person_id IS NOT NULL THEN TRUE ELSE FALSE END AS in_dm_62,
    CASE WHEN dm_63.person_id IS NOT NULL THEN TRUE ELSE FALSE END AS in_dm_63,
    CASE WHEN dm_64.person_id IS NOT NULL THEN TRUE ELSE FALSE END AS in_dm_64,
    CASE WHEN dm_65.person_id IS NOT NULL THEN TRUE ELSE FALSE END AS in_dm_65,
    CASE WHEN dm_66.person_id IS NOT NULL THEN TRUE ELSE FALSE END AS in_dm_66,
    
    -- Hypertension indicators
    CASE WHEN htn_61.person_id IS NOT NULL THEN TRUE ELSE FALSE END AS in_htn_61,
    CASE WHEN htn_62.person_id IS NOT NULL THEN TRUE ELSE FALSE END AS in_htn_62,
    CASE WHEN htn_63.person_id IS NOT NULL THEN TRUE ELSE FALSE END AS in_htn_63,
    CASE WHEN htn_65.person_id IS NOT NULL THEN TRUE ELSE FALSE END AS in_htn_65,
    CASE WHEN htn_66.person_id IS NOT NULL THEN TRUE ELSE FALSE END AS in_htn_66,
    
    -- CYP Asthma indicator
    CASE WHEN cyp_ast_61.person_id IS NOT NULL THEN TRUE ELSE FALSE END AS in_cyp_ast_61,
    
    -- Summary flags
    CASE WHEN (
        af_61.person_id IS NOT NULL OR af_62.person_id IS NOT NULL OR
        ckd_61.person_id IS NOT NULL OR ckd_62.person_id IS NOT NULL OR ckd_63.person_id IS NOT NULL OR ckd_64.person_id IS NOT NULL OR
        cvd_61.person_id IS NOT NULL OR cvd_62.person_id IS NOT NULL OR cvd_63.person_id IS NOT NULL OR cvd_64.person_id IS NOT NULL OR cvd_65.person_id IS NOT NULL OR cvd_66.person_id IS NOT NULL OR
        dm_61.person_id IS NOT NULL OR dm_62.person_id IS NOT NULL OR dm_63.person_id IS NOT NULL OR dm_64.person_id IS NOT NULL OR dm_65.person_id IS NOT NULL OR dm_66.person_id IS NOT NULL OR
        htn_61.person_id IS NOT NULL OR htn_62.person_id IS NOT NULL OR htn_63.person_id IS NOT NULL OR htn_65.person_id IS NOT NULL OR htn_66.person_id IS NOT NULL OR
        cyp_ast_61.person_id IS NOT NULL
    ) THEN TRUE ELSE FALSE END AS in_any_case_finding,
    
    -- Count of case finding indicators
    (
        CASE WHEN af_61.person_id IS NOT NULL THEN 1 ELSE 0 END +
        CASE WHEN af_62.person_id IS NOT NULL THEN 1 ELSE 0 END +
        CASE WHEN ckd_61.person_id IS NOT NULL THEN 1 ELSE 0 END +
        CASE WHEN ckd_62.person_id IS NOT NULL THEN 1 ELSE 0 END +
        CASE WHEN ckd_63.person_id IS NOT NULL THEN 1 ELSE 0 END +
        CASE WHEN ckd_64.person_id IS NOT NULL THEN 1 ELSE 0 END +
        CASE WHEN cvd_61.person_id IS NOT NULL THEN 1 ELSE 0 END +
        CASE WHEN cvd_62.person_id IS NOT NULL THEN 1 ELSE 0 END +
        CASE WHEN cvd_63.person_id IS NOT NULL THEN 1 ELSE 0 END +
        CASE WHEN cvd_64.person_id IS NOT NULL THEN 1 ELSE 0 END +
        CASE WHEN cvd_65.person_id IS NOT NULL THEN 1 ELSE 0 END +
        CASE WHEN cvd_66.person_id IS NOT NULL THEN 1 ELSE 0 END +
        CASE WHEN dm_61.person_id IS NOT NULL THEN 1 ELSE 0 END +
        CASE WHEN dm_62.person_id IS NOT NULL THEN 1 ELSE 0 END +
        CASE WHEN dm_63.person_id IS NOT NULL THEN 1 ELSE 0 END +
        CASE WHEN dm_64.person_id IS NOT NULL THEN 1 ELSE 0 END +
        CASE WHEN dm_65.person_id IS NOT NULL THEN 1 ELSE 0 END +
        CASE WHEN dm_66.person_id IS NOT NULL THEN 1 ELSE 0 END +
        CASE WHEN htn_61.person_id IS NOT NULL THEN 1 ELSE 0 END +
        CASE WHEN htn_62.person_id IS NOT NULL THEN 1 ELSE 0 END +
        CASE WHEN htn_63.person_id IS NOT NULL THEN 1 ELSE 0 END +
        CASE WHEN htn_65.person_id IS NOT NULL THEN 1 ELSE 0 END +
        CASE WHEN htn_66.person_id IS NOT NULL THEN 1 ELSE 0 END +
        CASE WHEN cyp_ast_61.person_id IS NOT NULL THEN 1 ELSE 0 END
    ) AS case_finding_count

FROM {{ ref('int_ltc_lcs_cf_base_population') }} base

-- AF joins
LEFT JOIN {{ ref('dim_prog_ltc_lcs_cf_af_61') }} af_61 ON base.person_id = af_61.person_id
LEFT JOIN {{ ref('dim_prog_ltc_lcs_cf_af_62') }} af_62 ON base.person_id = af_62.person_id

-- CKD joins
LEFT JOIN {{ ref('dim_prog_ltc_lcs_cf_ckd_61') }} ckd_61 ON base.person_id = ckd_61.person_id
LEFT JOIN {{ ref('dim_prog_ltc_lcs_cf_ckd_62') }} ckd_62 ON base.person_id = ckd_62.person_id
LEFT JOIN {{ ref('dim_prog_ltc_lcs_cf_ckd_63') }} ckd_63 ON base.person_id = ckd_63.person_id
LEFT JOIN {{ ref('dim_prog_ltc_lcs_cf_ckd_64') }} ckd_64 ON base.person_id = ckd_64.person_id

-- CVD joins
LEFT JOIN {{ ref('dim_prog_ltc_lcs_cf_cvd_61') }} cvd_61 ON base.person_id = cvd_61.person_id
LEFT JOIN {{ ref('dim_prog_ltc_lcs_cf_cvd_62') }} cvd_62 ON base.person_id = cvd_62.person_id
LEFT JOIN {{ ref('dim_prog_ltc_lcs_cf_cvd_63') }} cvd_63 ON base.person_id = cvd_63.person_id
LEFT JOIN {{ ref('dim_prog_ltc_lcs_cf_cvd_64') }} cvd_64 ON base.person_id = cvd_64.person_id
LEFT JOIN {{ ref('dim_prog_ltc_lcs_cf_cvd_65') }} cvd_65 ON base.person_id = cvd_65.person_id
LEFT JOIN {{ ref('dim_prog_ltc_lcs_cf_cvd_66') }} cvd_66 ON base.person_id = cvd_66.person_id

-- Diabetes joins
LEFT JOIN {{ ref('dim_prog_ltc_lcs_cf_dm_61') }} dm_61 ON base.person_id = dm_61.person_id
LEFT JOIN {{ ref('dim_prog_ltc_lcs_cf_dm_62') }} dm_62 ON base.person_id = dm_62.person_id
LEFT JOIN {{ ref('dim_prog_ltc_lcs_cf_dm_63') }} dm_63 ON base.person_id = dm_63.person_id
LEFT JOIN {{ ref('dim_prog_ltc_lcs_cf_dm_64') }} dm_64 ON base.person_id = dm_64.person_id
LEFT JOIN {{ ref('dim_prog_ltc_lcs_cf_dm_65') }} dm_65 ON base.person_id = dm_65.person_id
LEFT JOIN {{ ref('dim_prog_ltc_lcs_cf_dm_66') }} dm_66 ON base.person_id = dm_66.person_id

-- Hypertension joins
LEFT JOIN {{ ref('dim_prog_ltc_lcs_cf_htn_61') }} htn_61 ON base.person_id = htn_61.person_id
LEFT JOIN {{ ref('dim_prog_ltc_lcs_cf_htn_62') }} htn_62 ON base.person_id = htn_62.person_id
LEFT JOIN {{ ref('dim_prog_ltc_lcs_cf_htn_63') }} htn_63 ON base.person_id = htn_63.person_id
LEFT JOIN {{ ref('dim_prog_ltc_lcs_cf_htn_65') }} htn_65 ON base.person_id = htn_65.person_id
LEFT JOIN {{ ref('dim_prog_ltc_lcs_cf_htn_66') }} htn_66 ON base.person_id = htn_66.person_id

-- CYP Asthma join
LEFT JOIN {{ ref('dim_prog_ltc_lcs_cf_cyp_ast_61') }} cyp_ast_61 ON base.person_id = cyp_ast_61.person_id 