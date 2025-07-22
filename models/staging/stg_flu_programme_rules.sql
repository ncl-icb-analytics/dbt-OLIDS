/*
Flu Programme Rules Configuration
Sources: 
- stg_flu_programme_logic
- stg_flu_code_clusters  
- stg_flu_campaign_dates

This unified staging model combines the three flu seed files into a single
view that provides all configuration needed by the flu eligibility system.

The model resolves date placeholders to actual campaign dates and joins
business logic with code clusters and campaign-specific dates.

Rule Types:
- SIMPLE: Single cluster logic
- COMBINATION: Multiple clusters with AND/OR logic  
- HIERARCHICAL: Complex multi-step logic (e.g., CKD staging, BMI)
- EXCLUSION: Latest code determines inclusion (e.g., diabetes, carer status)
- AGE_BASED: Age threshold rules
- AGE_BIRTH_RANGE: Birth date range rules

Data Source Types:
- observation: Clinical observations from get_observations() macro
- medication: Medication orders from get_medication_orders() macro  
- demographic: Age/birth date calculations
*/

WITH campaign_dates_pivot AS (
    SELECT 
        campaign_id,
        rule_group_id,
        MAX(CASE WHEN date_type = 'start_dat' THEN date_value END) AS start_dat,
        MAX(CASE WHEN date_type = 'ref_dat' THEN date_value END) AS ref_dat,
        MAX(CASE WHEN date_type = 'child_dat' THEN date_value END) AS child_dat,
        MAX(CASE WHEN date_type = 'audit_end_dat' THEN date_value END) AS audit_end_dat,
        MAX(CASE WHEN date_type = 'latest_since_date' THEN date_value END) AS latest_since_date,
        MAX(CASE WHEN date_type = 'latest_after_date' THEN date_value END) AS latest_after_date,
        MAX(CASE WHEN date_type = 'birth_start' THEN date_value END) AS birth_start,
        MAX(CASE WHEN date_type = 'birth_end' THEN date_value END) AS birth_end
    FROM {{ ref('stg_flu_campaign_dates') }}
    GROUP BY campaign_id, rule_group_id
),

logic_with_clusters AS (
    SELECT 
        l.campaign_id,
        l.rule_group_id,
        l.rule_group_name,
        l.rule_type,
        l.logic_expression,
        l.exclusion_groups,
        l.age_min_months,
        l.age_max_years,
        l.business_description,
        l.technical_description,
        c.cluster_id,
        c.data_source_type,
        c.date_qualifier,
        c.cluster_description
    FROM {{ ref('stg_flu_programme_logic') }} l
    LEFT JOIN {{ ref('stg_flu_code_clusters') }} c
        ON l.rule_group_id = c.rule_group_id
)

SELECT 
    lc.campaign_id,
    lc.rule_group_id,
    lc.rule_group_name,
    lc.rule_type,
    lc.cluster_id,
    lc.data_source_type,
    lc.date_qualifier,
    lc.logic_expression,
    lc.exclusion_groups,
    lc.age_min_months,
    lc.age_max_years,
    lc.business_description AS description,
    lc.technical_description,
    lc.cluster_description,
    
    -- Campaign dates (ALL rule_group_id dates apply to all rules)
    COALESCE(cd_specific.start_dat, cd_all.start_dat) AS start_dat,
    COALESCE(cd_specific.ref_dat, cd_all.ref_dat) AS ref_dat,
    COALESCE(cd_specific.child_dat, cd_all.child_dat) AS child_dat,
    COALESCE(cd_specific.audit_end_dat, cd_all.audit_end_dat) AS audit_end_dat,
    
    -- Rule-specific dates
    cd_specific.latest_since_date,
    cd_specific.latest_after_date,
    cd_specific.birth_start,
    cd_specific.birth_end,
    
    -- Resolve reference date based on date_qualifier and rule-specific dates
    CASE 
        WHEN lc.date_qualifier = 'LATEST_SINCE' AND cd_specific.latest_since_date IS NOT NULL 
            THEN cd_specific.latest_since_date
        WHEN lc.date_qualifier = 'LATEST_AFTER' AND cd_specific.latest_after_date IS NOT NULL 
            THEN cd_specific.latest_after_date
        WHEN lc.date_qualifier IN ('EARLIEST', 'LATEST') 
            THEN 'AUDITEND_DAT'  -- Will be passed as parameter
        ELSE 'AUDITEND_DAT'
    END AS reference_date,
    
    -- Resolved reference date for macro usage
    CASE 
        WHEN lc.date_qualifier = 'LATEST_SINCE' AND cd_specific.latest_since_date IS NOT NULL 
            THEN cd_specific.latest_since_date
        WHEN lc.date_qualifier = 'LATEST_AFTER' AND cd_specific.latest_after_date IS NOT NULL 
            THEN cd_specific.latest_after_date
        WHEN lc.date_qualifier IN ('EARLIEST', 'LATEST') 
            THEN COALESCE(cd_specific.audit_end_dat, cd_all.audit_end_dat)  -- Use audit end date instead of PARAMETER
        ELSE COALESCE(cd_specific.audit_end_dat, cd_all.audit_end_dat)
    END AS resolved_reference_date

FROM logic_with_clusters lc
-- Join campaign dates for this specific rule group
LEFT JOIN campaign_dates_pivot cd_specific
    ON lc.campaign_id = cd_specific.campaign_id 
    AND lc.rule_group_id = cd_specific.rule_group_id
-- Join campaign dates for ALL rule groups (fallback)
LEFT JOIN campaign_dates_pivot cd_all
    ON lc.campaign_id = cd_all.campaign_id 
    AND cd_all.rule_group_id = 'ALL'

ORDER BY lc.campaign_id, lc.rule_group_id, lc.cluster_id