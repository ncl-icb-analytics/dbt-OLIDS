/*
Flu Programme Logic Staging
Generated from static configuration - matches flu_programme_logic.csv exactly

Campaign-specific business logic and descriptions for flu vaccination eligibility rules.
*/

{{ config(materialized='view') }}

WITH logic_data AS (
    -- Row 2: AST_GROUP
    SELECT 'flu_2024_25' AS campaign_id, 'AST_GROUP' AS rule_group_id, 'Asthma' AS rule_group_name, 'COMBINATION' AS rule_type, 'AST_COD AND (ASTMED_COD OR ASTRX_COD)' AS logic_expression, '' AS exclusion_groups, 6 AS age_min_months, 65 AS age_max_years, 'People with asthma who have used inhalers since Sept 2023 or been hospitalised' AS business_description, 'Earliest asthma diagnosis + (latest medication since lookback date OR latest admission ever)' AS technical_description
    UNION ALL
    -- Row 3: AST_ADM_GROUP
    SELECT 'flu_2024_25', 'AST_ADM_GROUP', 'Asthma Admission', 'SIMPLE', 'ASTADM_COD', '', 6, 65, 'People with asthma who have been hospitalised for their condition', 'Latest asthma admission before audit end date'
    UNION ALL
    -- Row 4: RESP_GROUP
    SELECT 'flu_2024_25', 'RESP_GROUP', 'Chronic Respiratory Disease', 'COMBINATION', 'AST_GROUP OR AST_ADM_GROUP OR RESP_COD', '', 6, 65, 'People with chronic lung conditions (asthma, COPD, cystic fibrosis etc.)', 'Include if in asthma groups OR have chronic respiratory diagnosis'
    UNION ALL
    -- Row 5: CHD_GROUP
    SELECT 'flu_2024_25', 'CHD_GROUP', 'Chronic Heart Disease', 'SIMPLE', 'CHD_COD', '', 6, 65, 'People with coronary heart disease, heart failure or stroke', 'Earliest chronic heart disease diagnosis'
    UNION ALL
    -- Row 6: CKD_GROUP
    SELECT 'flu_2024_25', 'CKD_GROUP', 'Chronic Kidney Disease', 'HIERARCHICAL', 'CKD_COD OR (CKD35_COD >= CKD15_COD)', '', 6, 65, 'People with chronic kidney disease stage 3-5', 'CKD diagnosis OR (latest stage 3-5 >= latest any stage)'
    UNION ALL
    -- Row 7: CLD_GROUP
    SELECT 'flu_2024_25', 'CLD_GROUP', 'Chronic Liver Disease', 'SIMPLE', 'CLD_COD', '', 6, 65, 'People with chronic liver disease (cirrhosis, hepatitis etc.)', 'Earliest chronic liver disease diagnosis'
    UNION ALL
    -- Row 8: DIAB_GROUP
    SELECT 'flu_2024_25', 'DIAB_GROUP', 'Diabetes', 'EXCLUSION', 'ADDIS_COD OR (DIAB_COD AND (DMRES_COD IS NULL OR DIAB_COD > DMRES_COD))', '', 6, 65, 'People with diabetes (type 1, type 2) or Addison''s disease', 'Addisons disease OR (diabetes AND (no resolved code OR diabetes > resolved))'
    UNION ALL
    -- Row 9: IMMUNO_GROUP
    SELECT 'flu_2024_25', 'IMMUNO_GROUP', 'Immunosuppression', 'COMBINATION', 'IMMDX_COD OR IMMRX_COD OR IMMADM_COD OR DXT_CHEMO_COD', '', 6, 65, 'People with weakened immune systems or receiving immunosuppressive treatment', 'Immunosuppression diagnosis OR recent medication OR recent admin code OR recent chemotherapy'
    UNION ALL
    -- Row 10: CNS_GROUP
    SELECT 'flu_2024_25', 'CNS_GROUP', 'Chronic Neurological Disease', 'SIMPLE', 'CNSGROUP_COD', '', 6, 65, 'People with chronic neurological conditions (MS, motor neurone disease etc.)', 'Earliest chronic neurological disease diagnosis'
    UNION ALL
    -- Row 11: ASPLENIA_GROUP
    SELECT 'flu_2024_25', 'ASPLENIA_GROUP', 'Asplenia', 'SIMPLE', 'PNSPLEEN_COD', '', 6, 65, 'People with asplenia or splenic dysfunction', 'Earliest asplenia/spleen dysfunction diagnosis'
    UNION ALL
    -- Row 12: OVER65_GROUP
    SELECT 'flu_2024_25', 'OVER65_GROUP', 'Over 65', 'AGE_BASED', '', '', 780, NULL, 'Everyone aged 65 and over at end of March 2025', 'Age >= 65 years at ref_dat'
    UNION ALL
    -- Row 13: CHILD_2_3
    SELECT 'flu_2024_25', 'CHILD_2_3', 'Children 2-3', 'AGE_BIRTH_RANGE', '', '', NULL, NULL, 'Children aged 2-3 years (born Sept 2020 - Aug 2022)', 'Birth date between specified range'
    UNION ALL
    -- Row 14: CHILD_4_16
    SELECT 'flu_2024_25', 'CHILD_4_16', 'Children 4-16', 'AGE_BIRTH_RANGE', '', '', NULL, NULL, 'School children aged 4-16 years (Reception to Year 11)', 'Birth date between specified range'
    UNION ALL
    -- Row 15: BMI_GROUP
    SELECT 'flu_2024_25', 'BMI_GROUP', 'Morbid Obesity', 'HIERARCHICAL', 'BMI_VAL >= 40 OR SEV_OBESITY_COD', '', 216, 65, 'Adults aged 18-64 with severe obesity (BMI 40+)', '(BMI >= 40 AND BMI >= stage) OR (no stage AND BMI >= 40) OR (severe obesity > BMI OR no BMI)'
    UNION ALL
    -- Row 16: PREG_GROUP
    SELECT 'flu_2024_25', 'PREG_GROUP', 'Pregnant', 'HIERARCHICAL', 'PREG2_DAT OR (PREGDEL_DAT AND PREG_DAT >= PREGDEL_DAT)', '', 144, 65, 'Pregnant women aged 12-64', 'Group 1: pregnancy since start_dat OR Group 2: latest pregnancy/delivery before start_dat is pregnancy'
    UNION ALL
    -- Row 17: LEARNDIS_GROUP
    SELECT 'flu_2024_25', 'LEARNDIS_GROUP', 'Learning Disability', 'SIMPLE', 'LEARNDIS_COD', '', 6, 65, 'People with learning disabilities aged 6 months to 64 years', 'Latest learning disability diagnosis'
    UNION ALL
    -- Row 18: CARER_GROUP
    SELECT 'flu_2024_25', 'CARER_GROUP', 'Carer', 'EXCLUSION', 'CARER_COD AND (NOTCARER_COD IS NULL OR CARER_COD > NOTCARER_COD)', 'NOT IN clinical_risk_groups|BMI_GROUP|PREG_GROUP', 60, 65, 'Unpaid carers aged 5-64 (not already eligible for other reasons)', 'Latest carer code AND (no not-carer OR carer > not-carer) AND not in exclusion groups'
    UNION ALL
    -- Row 19: HOMELESS_GROUP
    SELECT 'flu_2024_25', 'HOMELESS_GROUP', 'Homeless', 'HIERARCHICAL', 'Latest RESIDE_COD is HOMELESS_COD', '', 192, 65, 'People who are homeless aged 16-64', 'Latest residential code is homeless'
    UNION ALL
    -- Row 20: LONGRES_GROUP
    SELECT 'flu_2024_25', 'LONGRES_GROUP', 'Long Term Residential Care', 'HIERARCHICAL', 'Latest RESIDE_COD is LONGRES_COD', '', 6, NULL, 'People living in care homes or long-term residential care', 'Latest residential code is long-term care'
    UNION ALL
    -- Row 21: HHLD_IMDEF_GROUP
    SELECT 'flu_2024_25', 'HHLD_IMDEF_GROUP', 'Household Contact Immunocompromised', 'SIMPLE', 'HHLD_IMDEF_COD', '', 6, 65, 'People who live with someone who has a weakened immune system', 'Latest household contact code'
    UNION ALL
    -- Row 22: HCWORKER_GROUP
    SELECT 'flu_2024_25', 'HCWORKER_GROUP', 'Health and Social Care Workers', 'COMBINATION', 'CAREHOME_COD OR NURSEHOME_COD OR DOMCARE_COD', '', 192, 65, 'Health and social care workers aged 16-64', 'Latest care worker code (care home/nursing home/domiciliary care)'
    UNION ALL
    -- Row 23: FLUVAX_GROUP
    SELECT 'flu_2024_25', 'FLUVAX_GROUP', 'Flu Vaccination Given', 'COMBINATION', 'FLUVAX_COD OR FLURX_COD', '', NULL, NULL, 'People who have already received flu vaccination this campaign', 'Flu vaccination code OR medication since latest_after_date'
    UNION ALL
    -- Row 24: FLUDECLINED_GROUP
    SELECT 'flu_2024_25', 'FLUDECLINED_GROUP', 'Flu Vaccination Declined', 'EXCLUSION', '(DECL_COD OR NOCONS_COD) AND NOT vaccinated', '', NULL, NULL, 'People who have declined flu vaccination this campaign', 'Declined OR no consent AND not vaccinated'
    UNION ALL
    -- Row 25: LAIV_GROUP
    SELECT 'flu_2024_25', 'LAIV_GROUP', 'LAIV Vaccination', 'COMBINATION', 'LAIV_COD OR LAIVRX_COD', '', NULL, NULL, 'People who have received live attenuated influenza vaccine (nasal spray)', 'LAIV code OR LAIV medication since latest_after_date'
)

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
FROM logic_data
ORDER BY campaign_id, rule_group_id