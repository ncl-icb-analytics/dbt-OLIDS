/*
Flu Vaccination Uptake Fact Table (Row-Level)

This model combines eligibility and vaccination status at the person level
to provide core uptake analysis capabilities.

Key features:
- Combines eligibility information with vaccination status
- Focused on uptake business process without demographics
- Supports analysis of coverage gaps and vaccination patterns
- Works automatically with both current and previous campaigns

Usage:
- Filter by campaign_id to analyze specific campaigns
- Use vaccination_status to segment by outcome (administered, declined, no record)
- Join with dim_person_demographics for demographic analysis
*/

{{ config(
    materialized='table',
    cluster_by=['campaign_id', 'person_id']
) }}

WITH eligible_people AS (
    -- Get all eligible people with ALL their eligibility reasons (not just primary)
    SELECT 
        campaign_id,
        person_id,
        campaign_category,
        risk_group,
        eligibility_reason,
        rule_type,
        eligibility_priority
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
        COALESCE(e.campaign_category, 'Not Eligible') AS campaign_category,
        COALESCE(e.risk_group, 'Vaccinated Despite Ineligibility') AS risk_group,
        e.eligibility_reason,
        e.rule_type,
        
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
    WHERE e.person_id IS NOT NULL  -- Keep all eligible people
        OR v.vaccination_status IN ('VACCINATION_ADMINISTERED', 'LAIV_ADMINISTERED')  -- Keep only vaccinated non-eligible people
),

-- Add campaign information and calculate uptake metrics
final_uptake AS (
    SELECT 
        cd.campaign_id,
        cd.person_id,
        
        -- Eligibility information
        cd.is_eligible,
        cd.campaign_category,
        cd.risk_group,
        cd.eligibility_reason,
        cd.rule_type,
        
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
        
        -- Time to vaccination (days from campaign start) - only for vaccinated people within campaign period
        CASE 
            WHEN cd.vaccinated = TRUE 
                AND cd.vaccination_date IS NOT NULL 
                AND cc.campaign_start_date IS NOT NULL 
                AND cc.campaign_end_date IS NOT NULL
                AND cd.vaccination_date >= cc.campaign_start_date
                AND cd.vaccination_date <= cc.campaign_end_date
            THEN DATEDIFF('day', cc.campaign_start_date, cd.vaccination_date)
            ELSE NULL
        END AS days_to_vaccination,
        
        -- Campaign dates for reference
        cc.campaign_start_date,
        cc.campaign_reference_date,
        cc.audit_end_date,
        
        CURRENT_TIMESTAMP() AS created_at
        
    FROM combined_data cd
    LEFT JOIN (
        SELECT DISTINCT campaign_id, campaign_start_date, campaign_end_date, campaign_reference_date, audit_end_date
        FROM ({{ flu_campaign_config(var('flu_current_campaign', 'flu_2024_25')) }})
        UNION ALL
        SELECT DISTINCT campaign_id, campaign_start_date, campaign_end_date, campaign_reference_date, audit_end_date  
        FROM ({{ flu_campaign_config(var('flu_previous_campaign', 'flu_2023_24')) }})
    ) cc
        ON cd.campaign_id = cc.campaign_id
)

SELECT * FROM final_uptake
ORDER BY campaign_id, person_id