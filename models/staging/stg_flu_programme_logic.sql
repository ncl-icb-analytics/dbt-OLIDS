/*
Flu Programme Logic Staging
Source: seeds/flu_programme_logic.csv

Business logic and rule types for flu vaccination eligibility.
These rules define how different clinical conditions and risk factors
are evaluated to determine flu vaccination eligibility.

Rule Types:
- SIMPLE: Single cluster logic
- COMBINATION: Multiple clusters with AND/OR logic
- HIERARCHICAL: Complex multi-step logic (e.g., CKD staging, BMI)
- EXCLUSION: Latest code determines inclusion (e.g., diabetes, carer status)
- AGE_BASED: Age threshold rules
- AGE_BIRTH_RANGE: Birth date range rules
*/

SELECT 
    campaign_id,
    rule_group_id,
    rule_group_name,
    rule_type,
    logic_expression,
    exclusion_groups,
    age_min_months,
    age_max_years,
    business_description,
    technical_description
FROM {{ env_var('SNOWFLAKE_TARGET_DATABASE', 'DATA_LAB_OLIDS_UAT') }}.DBT_DEV_REFERENCE.FLU_PROGRAMME_LOGIC
ORDER BY campaign_id, rule_group_id