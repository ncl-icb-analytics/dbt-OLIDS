/*
Flu Code Clusters Staging
Generated from static configuration - matches flu_code_clusters.csv exactly

Clinical code cluster definitions for flu programme rules.
These clusters define which clinical codes belong to each rule group
and how they should be queried (date qualifiers).
*/

{{ config(materialized='view') }}

WITH cluster_data AS (
    -- Row 2: AST_GROUP
    SELECT 'AST_GROUP' AS rule_group_id, 'AST_COD' AS cluster_id, 'observation' AS data_source_type, 'EARLIEST' AS date_qualifier, 'Asthma diagnosis codes' AS cluster_description
    UNION ALL
    SELECT 'AST_GROUP', 'ASTMED_COD', 'observation', 'LATEST_SINCE', 'Asthma medication administration codes'
    UNION ALL
    SELECT 'AST_GROUP', 'ASTRX_COD', 'medication', 'LATEST_SINCE', 'Asthma medication prescription codes'
    UNION ALL
    -- Row 5: AST_ADM_GROUP
    SELECT 'AST_ADM_GROUP', 'ASTADM_COD', 'observation', 'LATEST', 'Asthma admission codes'
    UNION ALL
    -- Row 6: RESP_GROUP
    SELECT 'RESP_GROUP', 'RESP_COD', 'observation', 'EARLIEST', 'Chronic respiratory disease codes'
    UNION ALL
    -- Row 7: CHD_GROUP
    SELECT 'CHD_GROUP', 'CHD_COD', 'observation', 'EARLIEST', 'Chronic heart disease codes'
    UNION ALL
    -- Row 8-10: CKD_GROUP
    SELECT 'CKD_GROUP', 'CKD_COD', 'observation', 'EARLIEST', 'Chronic kidney disease diagnosis codes'
    UNION ALL
    SELECT 'CKD_GROUP', 'CKD15_COD', 'observation', 'LATEST', 'CKD stage 1-5 codes'
    UNION ALL
    SELECT 'CKD_GROUP', 'CKD35_COD', 'observation', 'LATEST', 'CKD stage 3-5 codes'
    UNION ALL
    -- Row 11: CLD_GROUP
    SELECT 'CLD_GROUP', 'CLD_COD', 'observation', 'EARLIEST', 'Chronic liver disease codes'
    UNION ALL
    -- Row 12-14: DIAB_GROUP
    SELECT 'DIAB_GROUP', 'DIAB_COD', 'observation', 'LATEST', 'Diabetes diagnosis codes'
    UNION ALL
    SELECT 'DIAB_GROUP', 'DMRES_COD', 'observation', 'LATEST', 'Diabetes resolved codes'
    UNION ALL
    SELECT 'DIAB_GROUP', 'ADDIS_COD', 'observation', 'EARLIEST', 'Addison''s disease codes'
    UNION ALL
    -- Row 15-18: IMMUNO_GROUP
    SELECT 'IMMUNO_GROUP', 'IMMDX_COD', 'observation', 'LATEST', 'Immunosuppression diagnosis codes'
    UNION ALL
    SELECT 'IMMUNO_GROUP', 'IMMRX_COD', 'medication', 'LATEST_SINCE', 'Immunosuppression medication codes'
    UNION ALL
    SELECT 'IMMUNO_GROUP', 'IMMADM_COD', 'observation', 'LATEST_SINCE', 'Immunosuppression administration codes'
    UNION ALL
    SELECT 'IMMUNO_GROUP', 'DXT_CHEMO_COD', 'observation', 'LATEST_SINCE', 'Chemotherapy/radiotherapy codes'
    UNION ALL
    -- Row 19: CNS_GROUP
    SELECT 'CNS_GROUP', 'CNSGROUP_COD', 'observation', 'EARLIEST', 'Chronic neurological disease codes'
    UNION ALL
    -- Row 20: ASPLENIA_GROUP
    SELECT 'ASPLENIA_GROUP', 'PNSPLEEN_COD', 'observation', 'EARLIEST', 'Asplenia/spleen dysfunction codes'
    UNION ALL
    -- Row 21-23: BMI_GROUP
    SELECT 'BMI_GROUP', 'BMI_COD', 'observation', 'LATEST', 'BMI value codes'
    UNION ALL
    SELECT 'BMI_GROUP', 'BMI_STAGE_COD', 'observation', 'LATEST', 'BMI stage codes'
    UNION ALL
    SELECT 'BMI_GROUP', 'SEV_OBESITY_COD', 'observation', 'LATEST', 'Severe obesity codes'
    UNION ALL
    -- Row 24-25: PREG_GROUP
    SELECT 'PREG_GROUP', 'PREGDEL_COD', 'observation', 'LATEST', 'Pregnancy and delivery codes'
    UNION ALL
    SELECT 'PREG_GROUP', 'PREG_COD', 'observation', 'LATEST', 'Pregnancy codes only'
    UNION ALL
    -- Row 26: LEARNDIS_GROUP
    SELECT 'LEARNDIS_GROUP', 'LEARNDIS_COD', 'observation', 'LATEST', 'Learning disability codes'
    UNION ALL
    -- Row 27-28: CARER_GROUP
    SELECT 'CARER_GROUP', 'CARER_COD', 'observation', 'LATEST', 'Carer status codes'
    UNION ALL
    SELECT 'CARER_GROUP', 'NOTCARER_COD', 'observation', 'LATEST', 'Not carer status codes'
    UNION ALL
    -- Row 29-30: HOMELESS_GROUP
    SELECT 'HOMELESS_GROUP', 'RESIDE_COD', 'observation', 'LATEST', 'All residential status codes'
    UNION ALL
    SELECT 'HOMELESS_GROUP', 'HOMELESS_COD', 'observation', 'LATEST', 'Homeless residential codes'
    UNION ALL
    -- Row 31-32: LONGRES_GROUP
    SELECT 'LONGRES_GROUP', 'RESIDE_COD', 'observation', 'LATEST', 'All residential status codes'
    UNION ALL
    SELECT 'LONGRES_GROUP', 'LONGRES_COD', 'observation', 'LATEST', 'Long term residential care codes'
    UNION ALL
    -- Row 33: HHLD_IMDEF_GROUP
    SELECT 'HHLD_IMDEF_GROUP', 'HHLD_IMDEF_COD', 'observation', 'LATEST', 'Household contact immunocompromised codes'
    UNION ALL
    -- Row 34-36: HCWORKER_GROUP
    SELECT 'HCWORKER_GROUP', 'CAREHOME_COD', 'observation', 'LATEST', 'Care home worker codes'
    UNION ALL
    SELECT 'HCWORKER_GROUP', 'NURSEHOME_COD', 'observation', 'LATEST', 'Nursing home worker codes'
    UNION ALL
    SELECT 'HCWORKER_GROUP', 'DOMCARE_COD', 'observation', 'LATEST', 'Domiciliary care worker codes'
    UNION ALL
    -- Row 37-38: FLUVAX_GROUP
    SELECT 'FLUVAX_GROUP', 'FLUVAX_COD', 'observation', 'LATEST_AFTER', 'Flu vaccination administration codes'
    UNION ALL
    SELECT 'FLUVAX_GROUP', 'FLURX_COD', 'medication', 'LATEST_AFTER', 'Flu vaccination medication codes'
    UNION ALL
    -- Row 39-40: FLUDECLINED_GROUP
    SELECT 'FLUDECLINED_GROUP', 'DECL_COD', 'observation', 'LATEST', 'Flu vaccination declined codes'
    UNION ALL
    SELECT 'FLUDECLINED_GROUP', 'NOCONS_COD', 'observation', 'LATEST', 'No consent for vaccination codes'
    UNION ALL
    -- Row 41-42: LAIV_GROUP
    SELECT 'LAIV_GROUP', 'LAIV_COD', 'observation', 'LATEST_AFTER', 'LAIV vaccination administration codes'
    UNION ALL
    SELECT 'LAIV_GROUP', 'LAIVRX_COD', 'medication', 'LATEST_AFTER', 'LAIV vaccination medication codes'
)

SELECT 
    rule_group_id,
    cluster_id,
    data_source_type,
    date_qualifier,
    cluster_description
FROM cluster_data
ORDER BY rule_group_id, cluster_id