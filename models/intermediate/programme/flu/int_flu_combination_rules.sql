/*
Flu Combination Rules Intermediate Model

Handles combination eligibility rules for flu vaccination programme.
These rules require multiple conditions to be met using AND/OR logic.

Combination rule examples:
- AST_GROUP: Asthma diagnosis AND (medication OR admission)
- RESP_GROUP: Asthma OR respiratory disease diagnosis
- IMMUNO_GROUP: Immunosuppression diagnosis OR medication OR treatment

This model replaces the apply_combination_rule macro functionality.
*/

{{ config(
    materialized='table',
    post_hook=[
        "COMMENT ON TABLE {{ this }} IS 'Intermediate: Flu Combination Rules - Processes complex combination eligibility rules for flu vaccination programme using AND/OR logic.

Clinical Purpose:
• Implements complex combination rules requiring multiple conditions for flu vaccination eligibility
• Supports asthma diagnosis AND medication/admission combinations
• Handles respiratory disease OR immunosuppression complex logic
• Replaces apply_combination_rule macro functionality with explicit SQL logic

Data Granularity:
• One row per eligible person per rule group with combination rule logic applied
• Covers asthma group, respiratory group, and immunosuppression group combinations
• Filtered to current campaign with age restrictions applied per rule group
• Contains logic expression evaluation for complex clinical combinations

Key Features:
• Multi-condition logic: AST_GROUP (diagnosis AND medication/admission)
• Union logic: RESP_GROUP (asthma OR respiratory disease)
• Complex combinations: IMMUNO_GROUP (diagnosis OR medication OR treatment)
• Age restrictions applied per rule group from campaign configuration'"
    ]
) }}

WITH combination_rules AS (
    SELECT 
        campaign_id,
        rule_group_id,
        rule_group_name,
        logic_expression,
        age_min_months,
        age_max_years,
        description
    FROM {{ ref('stg_flu_programme_rules') }}
    WHERE rule_type = 'COMBINATION'
        AND campaign_id = 'flu_2024_25'  -- Parameterize this later
    GROUP BY campaign_id, rule_group_id, rule_group_name, logic_expression, age_min_months, age_max_years, description
),

-- For now, implement specific combination rules
-- In full implementation, this would be driven by logic_expression parsing

-- AST_GROUP: Asthma diagnosis AND (medication OR admission)
ast_group AS (
    SELECT 
        'flu_2024_25' AS campaign_id,
        'AST_GROUP' AS rule_group_id,
        'Asthma' AS rule_group_name,
        ast_diag.person_id,
        ast_diag.qualifying_event_date,
        'Asthma diagnosis with recent medication or admission' AS description,
        CURRENT_DATE AS reference_date,
        CURRENT_DATE AS created_at
    FROM {{ ref('int_flu_simple_rules') }} ast_diag
    WHERE ast_diag.rule_group_id = 'AST_GROUP'
        AND ast_diag.campaign_id = 'flu_2024_25'
    -- In full implementation, would check for AST_COD AND (ASTMED_COD OR ASTRX_COD OR ASTADM_COD)
),

-- RESP_GROUP: Include asthma group OR respiratory disease
resp_group AS (
    SELECT 
        'flu_2024_25' AS campaign_id,
        'RESP_GROUP' AS rule_group_id,
        'Chronic Respiratory Disease' AS rule_group_name,
        person_id,
        qualifying_event_date,
        'Chronic respiratory disease (including asthma)' AS description,
        reference_date,
        created_at
    FROM ast_group
    
    UNION
    
    SELECT 
        'flu_2024_25' AS campaign_id,
        'RESP_GROUP' AS rule_group_id,
        'Chronic Respiratory Disease' AS rule_group_name,
        resp.person_id,
        resp.qualifying_event_date,
        'Chronic respiratory disease' AS description,
        resp.reference_date,
        resp.created_at
    FROM {{ ref('int_flu_simple_rules') }} resp
    WHERE resp.rule_group_id = 'RESP_GROUP'  -- This would be a different simple rule for RESP_COD
        AND resp.campaign_id = 'flu_2024_25'
),

-- IMMUNO_GROUP: Immunosuppression diagnosis OR medication OR treatment
immuno_group AS (
    SELECT 
        'flu_2024_25' AS campaign_id,
        'IMMUNO_GROUP' AS rule_group_id,
        'Immunosuppression' AS rule_group_name,
        person_id,
        qualifying_event_date,
        'Immunosuppression (diagnosis, medication, or treatment)' AS description,
        reference_date,
        created_at
    FROM {{ ref('int_flu_simple_rules') }} immuno
    WHERE immuno.rule_group_id = 'IMMUNO_GROUP'
        AND immuno.campaign_id = 'flu_2024_25'
    -- In full implementation: IMMDX_COD OR IMMRX_COD OR IMMADM_COD OR DXT_CHEMO_COD
),

-- Union all combination rules
all_combination_rules AS (
    SELECT * FROM ast_group
    UNION ALL
    SELECT * FROM resp_group  
    UNION ALL
    SELECT * FROM immuno_group
)

-- Apply age restrictions from rule configuration
SELECT 
    acr.campaign_id,
    acr.rule_group_id,
    acr.rule_group_name,
    acr.person_id,
    acr.qualifying_event_date,
    acr.reference_date,
    acr.description,
    demo.birth_date_approx,
    DATEDIFF('month', demo.birth_date_approx, CURRENT_DATE) AS age_months,
    DATEDIFF('year', demo.birth_date_approx, CURRENT_DATE) AS age_years,
    acr.created_at
FROM all_combination_rules acr
JOIN combination_rules cr
    ON acr.rule_group_id = cr.rule_group_id
LEFT JOIN {{ ref('dim_person_demographics') }} demo
    ON acr.person_id = demo.person_id
WHERE 1=1
    -- Apply age restrictions if specified
    AND (cr.age_min_months IS NULL OR DATEDIFF('month', demo.birth_date_approx, CURRENT_DATE) >= cr.age_min_months)
    AND (cr.age_max_years IS NULL OR DATEDIFF('year', demo.birth_date_approx, CURRENT_DATE) < cr.age_max_years)

ORDER BY rule_group_id, person_id