/*
Flu Vaccination Uptake Fact Table (Row-Level)

This model combines eligibility and vaccination status at the person level
to provide comprehensive uptake analysis capabilities.

Key features:
- Combines eligibility information with vaccination status
- Includes practice and demographic information for segmentation
- Supports analysis of coverage gaps and vaccination patterns
- Works automatically with both current and previous campaigns

Usage:
- Filter by campaign_id to analyze specific campaigns
- Use vaccination_status to segment by outcome (administered, declined, no record)
- Analyze by practice, PCN, borough, or demographic characteristics
*/

{{ config(
    materialized='table',
    cluster_by=['campaign_id', 'practice_code', 'person_id']
) }}

WITH eligible_people AS (
    -- Get all eligible people with their primary eligibility reason
    SELECT DISTINCT
        campaign_id,
        person_id,
        FIRST_VALUE(rule_group_id) OVER (
            PARTITION BY campaign_id, person_id 
            ORDER BY eligibility_priority, rule_group_id
        ) AS primary_rule_group_id,
        FIRST_VALUE(rule_group_name) OVER (
            PARTITION BY campaign_id, person_id 
            ORDER BY eligibility_priority, rule_group_id
        ) AS primary_rule_group_name,
        FIRST_VALUE(eligibility_reason) OVER (
            PARTITION BY campaign_id, person_id 
            ORDER BY eligibility_priority, rule_group_id
        ) AS primary_eligibility_reason,
        FIRST_VALUE(rule_type) OVER (
            PARTITION BY campaign_id, person_id 
            ORDER BY eligibility_priority, rule_group_id
        ) AS primary_rule_type,
        COUNT(DISTINCT rule_group_id) OVER (
            PARTITION BY campaign_id, person_id
        ) AS eligibility_count
    FROM {{ ref('fct_flu_eligibility') }}
),

vaccination_status AS (
    -- Get vaccination status with priority (administered > declined > no record)
    SELECT 
        campaign_id,
        person_id,
        FIRST_VALUE(status_type) OVER (
            PARTITION BY campaign_id, person_id 
            ORDER BY status_priority, status_type
        ) AS vaccination_status,
        FIRST_VALUE(status_date) OVER (
            PARTITION BY campaign_id, person_id 
            ORDER BY status_priority, status_type
        ) AS vaccination_date,
        FIRST_VALUE(status_reason) OVER (
            PARTITION BY campaign_id, person_id 
            ORDER BY status_priority, status_type
        ) AS vaccination_status_reason,
        -- Check if LAIV was given
        MAX(CASE WHEN status_type = 'LAIV_ADMINISTERED' THEN 1 ELSE 0 END) OVER (
            PARTITION BY campaign_id, person_id
        ) AS laiv_given,
        is_eligible,
        eligibility_status,
        vaccinated_despite_ineligible
    FROM {{ ref('fct_flu_status') }}
    QUALIFY ROW_NUMBER() OVER (
        PARTITION BY campaign_id, person_id 
        ORDER BY status_priority, status_type
    ) = 1
),

combined_data AS (
    -- Combine eligibility and vaccination status
    SELECT 
        COALESCE(e.campaign_id, v.campaign_id) AS campaign_id,
        COALESCE(e.person_id, v.person_id) AS person_id,
        
        -- Eligibility information
        CASE 
            WHEN e.person_id IS NOT NULL THEN TRUE 
            ELSE FALSE 
        END AS is_eligible,
        e.primary_rule_group_id,
        e.primary_rule_group_name,
        e.primary_eligibility_reason,
        e.primary_rule_type,
        e.eligibility_count,
        
        -- Vaccination information
        v.vaccination_status,
        v.vaccination_date,
        v.vaccination_status_reason,
        v.laiv_given,
        v.vaccinated_despite_ineligible,
        
        -- Uptake flags
        CASE 
            WHEN v.vaccination_status IN ('VACCINATION_ADMINISTERED', 'LAIV_ADMINISTERED') THEN TRUE
            ELSE FALSE
        END AS vaccinated,
        CASE 
            WHEN v.vaccination_status = 'VACCINATION_DECLINED' THEN TRUE
            ELSE FALSE
        END AS declined,
        CASE 
            WHEN e.person_id IS NOT NULL 
                AND (v.vaccination_status = 'NO_VACCINATION_RECORD' OR v.vaccination_status IS NULL) THEN TRUE
            ELSE FALSE
        END AS eligible_no_record
        
    FROM eligible_people e
    FULL OUTER JOIN vaccination_status v
        ON e.campaign_id = v.campaign_id 
        AND e.person_id = v.person_id
),

-- Add demographics and practice information
final_uptake AS (
    SELECT 
        cd.campaign_id,
        cd.person_id,
        
        -- Demographics
        demo.is_active,
        demo.sex,
        demo.age,
        demo.age_band_5y,
        demo.age_band_10y,
        demo.ethnicity_category,
        demo.main_language,
        demo.interpreter_needed,
        
        -- Practice information
        demo.practice_code,
        demo.practice_name,
        demo.pcn_code,
        demo.pcn_name,
        demo.practice_borough,
        demo.practice_neighbourhood,
        -- removed local_authority; practice_borough is available
        
        -- Eligibility information
        cd.is_eligible,
        cd.primary_rule_group_id,
        cd.primary_rule_group_name,
        cd.primary_eligibility_reason,
        cd.primary_rule_type,
        cd.eligibility_count,
        
        -- Vaccination information
        cd.vaccination_status,
        cd.vaccination_date,
        cd.vaccination_status_reason,
        cd.laiv_given,
        cd.vaccinated_despite_ineligible,
        
        -- Uptake flags
        cd.vaccinated,
        cd.declined,
        cd.eligible_no_record,
        
        -- Uptake category
        CASE
            WHEN cd.is_eligible AND cd.vaccinated THEN 'Eligible - Vaccinated'
            WHEN cd.is_eligible AND cd.declined THEN 'Eligible - Declined'
            WHEN cd.is_eligible AND cd.eligible_no_record THEN 'Eligible - No Record'
            WHEN NOT cd.is_eligible AND cd.vaccinated THEN 'Not Eligible - Vaccinated'
            WHEN NOT cd.is_eligible AND cd.declined THEN 'Not Eligible - Declined'
            ELSE 'Not Eligible - No Activity'
        END AS uptake_category,
        
        -- Time to vaccination (days from campaign start)
        CASE 
            WHEN cd.vaccination_date IS NOT NULL AND cc.campaign_start_date IS NOT NULL 
            THEN DATEDIFF('day', cc.campaign_start_date, cd.vaccination_date)
            ELSE NULL
        END AS days_to_vaccination,
        
        -- Campaign dates for reference
        cc.campaign_start_date,
        cc.campaign_reference_date,
        cc.audit_end_date,
        
        CURRENT_TIMESTAMP() AS created_at
        
    FROM combined_data cd
    LEFT JOIN {{ ref('dim_person_demographics') }} demo
        ON cd.person_id = demo.person_id
    LEFT JOIN (
        SELECT DISTINCT campaign_id, campaign_start_date, campaign_reference_date, audit_end_date
        FROM ({{ flu_campaign_config(var('flu_current_campaign', 'flu_2024_25')) }})
        UNION ALL
        SELECT DISTINCT campaign_id, campaign_start_date, campaign_reference_date, audit_end_date  
        FROM ({{ flu_campaign_config(var('flu_previous_campaign', 'flu_2023_24')) }})
    ) cc
        ON cd.campaign_id = cc.campaign_id
)

SELECT * FROM final_uptake
ORDER BY campaign_id, practice_code, person_id