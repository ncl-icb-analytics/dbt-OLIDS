{{ config(
    materialized='table') }}

-- LTC/LCS case finding summary dimension
-- Provides a unified view of all case finding indicators per person

SELECT
    base.person_id,
    base.age,

    -- AF indicators
    coalesce(af_61.person_id IS NOT NULL, FALSE) AS in_af_61,
    coalesce(af_62.person_id IS NOT NULL, FALSE) AS in_af_62,

    -- CKD indicators
    coalesce(ckd_61.person_id IS NOT NULL, FALSE)
        AS in_ckd_61,
    coalesce(ckd_62.person_id IS NOT NULL, FALSE)
        AS in_ckd_62,
    coalesce(ckd_63.person_id IS NOT NULL, FALSE)
        AS in_ckd_63,
    coalesce(ckd_64.person_id IS NOT NULL, FALSE)
        AS in_ckd_64,

    -- CVD indicators
    coalesce(cvd_61.person_id IS NOT NULL, FALSE)
        AS in_cvd_61,
    coalesce(cvd_62.person_id IS NOT NULL, FALSE)
        AS in_cvd_62,
    coalesce(cvd_63.person_id IS NOT NULL, FALSE)
        AS in_cvd_63,
    coalesce(cvd_64.person_id IS NOT NULL, FALSE)
        AS in_cvd_64,
    coalesce(cvd_65.person_id IS NOT NULL, FALSE)
        AS in_cvd_65,
    coalesce(cvd_66.person_id IS NOT NULL, FALSE)
        AS in_cvd_66,

    -- Diabetes indicators
    coalesce(dm_61.person_id IS NOT NULL, FALSE) AS in_dm_61,
    coalesce(dm_62.person_id IS NOT NULL, FALSE) AS in_dm_62,
    coalesce(dm_63.person_id IS NOT NULL, FALSE) AS in_dm_63,
    coalesce(dm_64.person_id IS NOT NULL, FALSE) AS in_dm_64,
    coalesce(dm_65.person_id IS NOT NULL, FALSE) AS in_dm_65,
    coalesce(dm_66.person_id IS NOT NULL, FALSE) AS in_dm_66,

    -- Hypertension indicators
    coalesce(htn_61.person_id IS NOT NULL, FALSE)
        AS in_htn_61,
    coalesce(htn_62.person_id IS NOT NULL, FALSE)
        AS in_htn_62,
    coalesce(htn_63.person_id IS NOT NULL, FALSE)
        AS in_htn_63,
    coalesce(htn_65.person_id IS NOT NULL, FALSE)
        AS in_htn_65,
    coalesce(htn_66.person_id IS NOT NULL, FALSE)
        AS in_htn_66,

    -- CYP Asthma indicator
    coalesce(cyp_ast_61.person_id IS NOT NULL, FALSE)
        AS in_cyp_ast_61,

    -- Summary flags
    coalesce((
        af_61.person_id IS NOT NULL OR af_62.person_id IS NOT NULL
        OR ckd_61.person_id IS NOT NULL
        OR ckd_62.person_id IS NOT NULL
        OR ckd_63.person_id IS NOT NULL
        OR ckd_64.person_id IS NOT NULL
        OR cvd_61.person_id IS NOT NULL
        OR cvd_62.person_id IS NOT NULL
        OR cvd_63.person_id IS NOT NULL
        OR cvd_64.person_id IS NOT NULL
        OR cvd_65.person_id IS NOT NULL
        OR cvd_66.person_id IS NOT NULL
        OR dm_61.person_id IS NOT NULL
        OR dm_62.person_id IS NOT NULL
        OR dm_63.person_id IS NOT NULL
        OR dm_64.person_id IS NOT NULL
        OR dm_65.person_id IS NOT NULL
        OR dm_66.person_id IS NOT NULL
        OR htn_61.person_id IS NOT NULL
        OR htn_62.person_id IS NOT NULL
        OR htn_63.person_id IS NOT NULL
        OR htn_65.person_id IS NOT NULL
        OR htn_66.person_id IS NOT NULL
        OR cyp_ast_61.person_id IS NOT NULL
    ), FALSE) AS in_any_case_finding,

    -- Count of case finding indicators
    (
        CASE WHEN af_61.person_id IS NOT NULL THEN 1 ELSE 0 END
        + CASE WHEN af_62.person_id IS NOT NULL THEN 1 ELSE 0 END
        + CASE WHEN ckd_61.person_id IS NOT NULL THEN 1 ELSE 0 END
        + CASE WHEN ckd_62.person_id IS NOT NULL THEN 1 ELSE 0 END
        + CASE WHEN ckd_63.person_id IS NOT NULL THEN 1 ELSE 0 END
        + CASE WHEN ckd_64.person_id IS NOT NULL THEN 1 ELSE 0 END
        + CASE WHEN cvd_61.person_id IS NOT NULL THEN 1 ELSE 0 END
        + CASE WHEN cvd_62.person_id IS NOT NULL THEN 1 ELSE 0 END
        + CASE WHEN cvd_63.person_id IS NOT NULL THEN 1 ELSE 0 END
        + CASE WHEN cvd_64.person_id IS NOT NULL THEN 1 ELSE 0 END
        + CASE WHEN cvd_65.person_id IS NOT NULL THEN 1 ELSE 0 END
        + CASE WHEN cvd_66.person_id IS NOT NULL THEN 1 ELSE 0 END
        + CASE WHEN dm_61.person_id IS NOT NULL THEN 1 ELSE 0 END
        + CASE WHEN dm_62.person_id IS NOT NULL THEN 1 ELSE 0 END
        + CASE WHEN dm_63.person_id IS NOT NULL THEN 1 ELSE 0 END
        + CASE WHEN dm_64.person_id IS NOT NULL THEN 1 ELSE 0 END
        + CASE WHEN dm_65.person_id IS NOT NULL THEN 1 ELSE 0 END
        + CASE WHEN dm_66.person_id IS NOT NULL THEN 1 ELSE 0 END
        + CASE WHEN htn_61.person_id IS NOT NULL THEN 1 ELSE 0 END
        + CASE WHEN htn_62.person_id IS NOT NULL THEN 1 ELSE 0 END
        + CASE WHEN htn_63.person_id IS NOT NULL THEN 1 ELSE 0 END
        + CASE WHEN htn_65.person_id IS NOT NULL THEN 1 ELSE 0 END
        + CASE WHEN htn_66.person_id IS NOT NULL THEN 1 ELSE 0 END
        + CASE WHEN cyp_ast_61.person_id IS NOT NULL THEN 1 ELSE 0 END
    ) AS case_finding_count

FROM {{ ref('int_ltc_lcs_cf_base_population') }} AS base

-- AF joins
LEFT JOIN
    {{ ref('dim_ltc_lcs_cf_af_61') }} AS af_61
    ON base.person_id = af_61.person_id
LEFT JOIN
    {{ ref('dim_ltc_lcs_cf_af_62') }} AS af_62
    ON base.person_id = af_62.person_id

-- CKD joins
LEFT JOIN
    {{ ref('dim_ltc_lcs_cf_ckd_61') }} AS ckd_61
    ON base.person_id = ckd_61.person_id
LEFT JOIN
    {{ ref('dim_ltc_lcs_cf_ckd_62') }} AS ckd_62
    ON base.person_id = ckd_62.person_id
LEFT JOIN
    {{ ref('dim_ltc_lcs_cf_ckd_63') }} AS ckd_63
    ON base.person_id = ckd_63.person_id
LEFT JOIN
    {{ ref('dim_ltc_lcs_cf_ckd_64') }} AS ckd_64
    ON base.person_id = ckd_64.person_id

-- CVD joins
LEFT JOIN
    {{ ref('dim_ltc_lcs_cf_cvd_61') }} AS cvd_61
    ON base.person_id = cvd_61.person_id
LEFT JOIN
    {{ ref('dim_ltc_lcs_cf_cvd_62') }} AS cvd_62
    ON base.person_id = cvd_62.person_id
LEFT JOIN
    {{ ref('dim_ltc_lcs_cf_cvd_63') }} AS cvd_63
    ON base.person_id = cvd_63.person_id
LEFT JOIN
    {{ ref('dim_ltc_lcs_cf_cvd_64') }} AS cvd_64
    ON base.person_id = cvd_64.person_id
LEFT JOIN
    {{ ref('dim_ltc_lcs_cf_cvd_65') }} AS cvd_65
    ON base.person_id = cvd_65.person_id
LEFT JOIN
    {{ ref('dim_ltc_lcs_cf_cvd_66') }} AS cvd_66
    ON base.person_id = cvd_66.person_id

-- Diabetes joins
LEFT JOIN
    {{ ref('dim_ltc_lcs_cf_dm_61') }} AS dm_61
    ON base.person_id = dm_61.person_id
LEFT JOIN
    {{ ref('dim_ltc_lcs_cf_dm_62') }} AS dm_62
    ON base.person_id = dm_62.person_id
LEFT JOIN
    {{ ref('dim_ltc_lcs_cf_dm_63') }} AS dm_63
    ON base.person_id = dm_63.person_id
LEFT JOIN
    {{ ref('dim_ltc_lcs_cf_dm_64') }} AS dm_64
    ON base.person_id = dm_64.person_id
LEFT JOIN
    {{ ref('dim_ltc_lcs_cf_dm_65') }} AS dm_65
    ON base.person_id = dm_65.person_id
LEFT JOIN
    {{ ref('dim_ltc_lcs_cf_dm_66') }} AS dm_66
    ON base.person_id = dm_66.person_id

-- Hypertension joins
LEFT JOIN
    {{ ref('dim_ltc_lcs_cf_htn_61') }} AS htn_61
    ON base.person_id = htn_61.person_id
LEFT JOIN
    {{ ref('dim_ltc_lcs_cf_htn_62') }} AS htn_62
    ON base.person_id = htn_62.person_id
LEFT JOIN
    {{ ref('dim_ltc_lcs_cf_htn_63') }} AS htn_63
    ON base.person_id = htn_63.person_id
LEFT JOIN
    {{ ref('dim_ltc_lcs_cf_htn_65') }} AS htn_65
    ON base.person_id = htn_65.person_id
LEFT JOIN
    {{ ref('dim_ltc_lcs_cf_htn_66') }} AS htn_66
    ON base.person_id = htn_66.person_id

-- CYP Asthma join
LEFT JOIN
    {{ ref('dim_ltc_lcs_cf_cyp_ast_61') }} AS cyp_ast_61
    ON base.person_id = cyp_ast_61.person_id
