/*
Investigation: High pregnancy counts - analyze the logic and data patterns
Purpose: Understand why pregnancy eligibility might be too high
*/

-- Analysis 1: Compare PREG_COD vs PREGDEL_COD concepts
WITH preg_codes AS (
    SELECT DISTINCT 
        'PREG_COD' AS code_set,
        mapped_concept_code,
        mapped_concept_display,
        code_description
    FROM ({{ get_observations("'PREG_COD'", 'UKHSA_FLU') }})
),

pregdel_codes AS (
    SELECT DISTINCT 
        'PREGDEL_COD' AS code_set,
        mapped_concept_code,
        mapped_concept_display,
        code_description
    FROM ({{ get_observations("'PREGDEL_COD'", 'UKHSA_FLU') }})
),

code_comparison AS (
    SELECT 
        COALESCE(p.mapped_concept_code, pd.mapped_concept_code) AS concept_code,
        COALESCE(p.mapped_concept_display, pd.mapped_concept_display) AS concept_display,
        CASE 
            WHEN p.mapped_concept_code IS NOT NULL AND pd.mapped_concept_code IS NOT NULL THEN 'Both PREG_COD and PREGDEL_COD'
            WHEN p.mapped_concept_code IS NOT NULL THEN 'Only PREG_COD'
            WHEN pd.mapped_concept_code IS NOT NULL THEN 'Only PREGDEL_COD (delivery/termination)'
        END AS code_category,
        p.code_description AS preg_description,
        pd.code_description AS pregdel_description
    FROM preg_codes p
    FULL OUTER JOIN pregdel_codes pd
        ON p.mapped_concept_code = pd.mapped_concept_code
)

SELECT 
    code_category,
    COUNT(*) AS code_count,
    STRING_AGG(concept_code, ', ') AS example_codes
FROM code_comparison
GROUP BY code_category
ORDER BY code_count DESC;

-- Analysis 2: Age distribution of pregnant women
-- WITH pregnancy_ages AS (
--     SELECT 
--         d.age,
--         COUNT(DISTINCT p.person_id) AS pregnant_count
--     FROM {{ ref('int_flu_pregnancy') }} p
--     JOIN {{ ref('dim_person_demographics') }} d ON p.person_id = d.person_id
--     WHERE p.campaign_id = 'flu_2024_25'
--         AND d.is_active = TRUE
--     GROUP BY d.age
-- )
-- SELECT 
--     age,
--     pregnant_count,
--     CASE 
--         WHEN age < 12 THEN '⚠️ Under minimum age'
--         WHEN age > 50 THEN '⚠️ Over typical childbearing age'
--         WHEN age BETWEEN 15 AND 45 THEN '✓ Typical childbearing age'
--         ELSE '⚠️ Edge case age'
--     END AS age_assessment
-- FROM pregnancy_ages
-- WHERE pregnant_count > 0
-- ORDER BY age;

-- Analysis 3: Date patterns in pregnancy eligibility
-- WITH pregnancy_dates AS (
--     SELECT 
--         p.person_id,
--         p.qualifying_event_date,
--         p.campaign_id,
--         cc.campaign_start_date,
--         DATEDIFF('month', p.qualifying_event_date, cc.campaign_reference_date) AS months_from_event,
--         CASE 
--             WHEN p.qualifying_event_date >= cc.campaign_start_date THEN 'Recent pregnancy (since campaign start)'
--             WHEN DATEDIFF('month', p.qualifying_event_date, cc.campaign_start_date) <= 12 THEN 'Historical pregnancy (within 12 months)'
--             ELSE 'Very old pregnancy (>12 months ago)'
--         END AS pregnancy_timing
--     FROM {{ ref('int_flu_pregnancy') }} p
--     JOIN (
--         SELECT * FROM ({{ flu_campaign_config('flu_2024_25') }})
--     ) cc ON p.campaign_id = cc.campaign_id
-- )
-- SELECT 
--     pregnancy_timing,
--     COUNT(DISTINCT person_id) AS person_count,
--     MIN(months_from_event) AS min_months_ago,
--     AVG(months_from_event) AS avg_months_ago,
--     MAX(months_from_event) AS max_months_ago
-- FROM pregnancy_dates
-- GROUP BY pregnancy_timing
-- ORDER BY person_count DESC;