/*
Flu Vaccination Status Intermediate Model

Tracks vaccination status for flu campaign monitoring and reporting.
These are not eligibility rules but status tracking for people who have:
- Already received flu vaccination
- Declined flu vaccination  
- Received LAIV (Live Attenuated Influenza Vaccine)

This information is used for:
- Coverage reporting
- Identifying people who still need vaccination
- Monitoring vaccination uptake
*/

{{ config(
    materialized='table',
    post_hook=[
        "COMMENT ON TABLE {{ this }} IS 'Intermediate: Flu Vaccination Status - Flu vaccination status tracking for programme monitoring and coverage reporting.

Clinical Purpose:
• NHS flu vaccination programme status monitoring and coverage reporting
• Vaccination uptake tracking and campaign effectiveness measurement
• Identification of vaccinated populations for programme planning
• LAIV (Live Attenuated Influenza Vaccine) administration tracking

Data Granularity:
• One row per person with vaccination status information
• Includes vaccination received, declined, and LAIV administration
• Uses campaign-specific date thresholds for status determination

Key Features:
• Comprehensive vaccination status tracking (received, declined, LAIV)
• Campaign-configurable date ranges for status validation
• Essential for coverage reporting and programme effectiveness monitoring
• Critical for identifying populations still requiring vaccination'"
    ]
) }}

{%- set current_campaign = var('flu_current_campaign') -%}

WITH campaign_config AS (
    SELECT 
        campaign_id,
        MAX(CASE WHEN rule_group_id = 'FLUVAX_GROUP' AND date_type = 'latest_after_date' THEN date_value END) AS fluvax_after_date,
        MAX(CASE WHEN rule_group_id = 'LAIV_GROUP' AND date_type = 'latest_after_date' THEN date_value END) AS laiv_after_date
    FROM {{ ref('stg_flu_campaign_dates') }}
    WHERE campaign_id = '{{ current_campaign }}'
    GROUP BY campaign_id
),

-- FLUVAX_GROUP: Flu vaccination given (observations)
flu_vaccination_obs AS (
    SELECT DISTINCT
        obs.person_id,
        MAX(obs.clinical_effective_date) AS latest_flu_vax_date,
        'Flu vaccination (observation)' AS vaccination_type
    FROM ({{ get_flu_observations_for_rule_group(current_campaign, 'FLUVAX_GROUP') }}) obs
    CROSS JOIN campaign_config cc
    WHERE obs.cluster_id = 'FLUVAX_COD'
        AND obs.clinical_effective_date IS NOT NULL
        AND obs.clinical_effective_date > cc.fluvax_after_date
        AND obs.clinical_effective_date <= {{ get_flu_audit_date(current_campaign) }}
    GROUP BY obs.person_id
),

-- FLUVAX_GROUP: Flu vaccination given (medications)
flu_vaccination_med AS (
    SELECT DISTINCT
        med.person_id,
        MAX(med.order_date) AS latest_flu_vax_date,
        'Flu vaccination (medication)' AS vaccination_type
    FROM ({{ get_flu_medications_for_rule_group(current_campaign, 'FLUVAX_GROUP') }}) med
    CROSS JOIN campaign_config cc
    WHERE med.cluster_id = 'FLURX_COD'
        AND med.order_date IS NOT NULL
        AND med.order_date > cc.fluvax_after_date
        AND med.order_date <= {{ get_flu_audit_date(current_campaign) }}
    GROUP BY med.person_id
),

-- FLUVAX_GROUP: Union flu vaccination evidence
flu_vaccination_all AS (
    SELECT person_id, latest_flu_vax_date, vaccination_type FROM flu_vaccination_obs
    UNION ALL
    SELECT person_id, latest_flu_vax_date, vaccination_type FROM flu_vaccination_med
),

-- FLUVAX_GROUP: Best flu vaccination evidence per person
flu_vaccination_best AS (
    SELECT 
        person_id,
        latest_flu_vax_date,
        vaccination_type,
        ROW_NUMBER() OVER (PARTITION BY person_id ORDER BY latest_flu_vax_date DESC, vaccination_type) AS rn
    FROM flu_vaccination_all
),

-- FLUVAX_GROUP: Final flu vaccination status
flu_vaccination_status AS (
    SELECT 
        '{{ current_campaign }}' AS campaign_id,
        'FLUVAX_GROUP' AS rule_group_id,
        'Flu Vaccination Given' AS rule_group_name,
        person_id,
        latest_flu_vax_date AS event_date,
        vaccination_type,
        'Already received flu vaccination this campaign' AS description,
        'vaccinated' AS status_type
    FROM flu_vaccination_best
    WHERE rn = 1
),

-- LAIV_GROUP: LAIV vaccination given (observations)
laiv_vaccination_obs AS (
    SELECT DISTINCT
        obs.person_id,
        MAX(obs.clinical_effective_date) AS latest_laiv_date,
        'LAIV vaccination (observation)' AS vaccination_type
    FROM ({{ get_flu_observations_for_rule_group(current_campaign, 'LAIV_GROUP') }}) obs
    CROSS JOIN campaign_config cc
    WHERE obs.cluster_id = 'LAIV_COD'
        AND obs.clinical_effective_date IS NOT NULL
        AND obs.clinical_effective_date > cc.laiv_after_date
        AND obs.clinical_effective_date <= {{ get_flu_audit_date(current_campaign) }}
    GROUP BY obs.person_id
),

-- LAIV_GROUP: LAIV vaccination given (medications)  
laiv_vaccination_med AS (
    SELECT DISTINCT
        med.person_id,
        MAX(med.order_date) AS latest_laiv_date,
        'LAIV vaccination (medication)' AS vaccination_type
    FROM ({{ get_flu_medications_for_rule_group(current_campaign, 'LAIV_GROUP') }}) med
    CROSS JOIN campaign_config cc
    WHERE med.cluster_id = 'LAIVRX_COD'
        AND med.order_date IS NOT NULL
        AND med.order_date > cc.laiv_after_date
        AND med.order_date <= {{ get_flu_audit_date(current_campaign) }}
    GROUP BY med.person_id
),

-- LAIV_GROUP: Union LAIV vaccination evidence
laiv_vaccination_all AS (
    SELECT person_id, latest_laiv_date, vaccination_type FROM laiv_vaccination_obs
    UNION ALL
    SELECT person_id, latest_laiv_date, vaccination_type FROM laiv_vaccination_med
),

-- LAIV_GROUP: Best LAIV vaccination evidence per person
laiv_vaccination_best AS (
    SELECT 
        person_id,
        latest_laiv_date,
        vaccination_type,
        ROW_NUMBER() OVER (PARTITION BY person_id ORDER BY latest_laiv_date DESC, vaccination_type) AS rn
    FROM laiv_vaccination_all
),

-- LAIV_GROUP: Final LAIV vaccination status
laiv_vaccination_status AS (
    SELECT 
        '{{ current_campaign }}' AS campaign_id,
        'LAIV_GROUP' AS rule_group_id,
        'LAIV Vaccination' AS rule_group_name,
        person_id,
        latest_laiv_date AS event_date,
        vaccination_type,
        'Received live attenuated influenza vaccine (nasal spray)' AS description,
        'laiv_vaccinated' AS status_type
    FROM laiv_vaccination_best
    WHERE rn = 1
),

-- FLUDECLINED_GROUP: Flu vaccination declined
flu_declined_obs AS (
    SELECT DISTINCT
        obs.person_id,
        MAX(obs.clinical_effective_date) AS latest_declined_date,
        'Flu vaccination declined' AS decline_type
    FROM ({{ get_flu_observations_for_rule_group(current_campaign, 'FLUDECLINED_GROUP') }}) obs
    WHERE obs.cluster_id = 'DECL_COD'
        AND obs.clinical_effective_date IS NOT NULL
        AND obs.clinical_effective_date <= {{ get_flu_audit_date(current_campaign) }}
    GROUP BY obs.person_id
    
    UNION ALL
    
    SELECT DISTINCT
        obs.person_id,
        MAX(obs.clinical_effective_date) AS latest_declined_date,
        'No consent for vaccination' AS decline_type
    FROM ({{ get_flu_observations_for_rule_group(current_campaign, 'FLUDECLINED_GROUP') }}) obs
    WHERE obs.cluster_id = 'NOCONS_COD'
        AND obs.clinical_effective_date IS NOT NULL
        AND obs.clinical_effective_date <= {{ get_flu_audit_date(current_campaign) }}
    GROUP BY obs.person_id
),

-- Check if declined people are actually vaccinated (exclusion logic)
declined_not_vaccinated AS (
    SELECT 
        fd.person_id,
        fd.latest_declined_date,
        fd.decline_type,
        fv.latest_flu_vax_date,
        lv.latest_laiv_date
    FROM flu_declined_obs fd
    LEFT JOIN flu_vaccination_best fv ON fd.person_id = fv.person_id AND fv.rn = 1
    LEFT JOIN laiv_vaccination_best lv ON fd.person_id = lv.person_id AND lv.rn = 1
    WHERE fv.person_id IS NULL AND lv.person_id IS NULL  -- NOT vaccinated
),

-- FLUDECLINED_GROUP: Final declined status
flu_declined_status AS (
    SELECT 
        '{{ current_campaign }}' AS campaign_id,
        'FLUDECLINED_GROUP' AS rule_group_id,
        'Flu Vaccination Declined' AS rule_group_name,
        person_id,
        latest_declined_date AS event_date,
        decline_type AS vaccination_type,
        'Declined flu vaccination this campaign' AS description,
        'declined' AS status_type
    FROM declined_not_vaccinated
),

-- Union all vaccination status
all_vaccination_status AS (
    SELECT 
        campaign_id, rule_group_id, rule_group_name, person_id, 
        event_date, vaccination_type, description, status_type
    FROM flu_vaccination_status
    
    UNION ALL
    
    SELECT 
        campaign_id, rule_group_id, rule_group_name, person_id,
        event_date, vaccination_type, description, status_type
    FROM laiv_vaccination_status
    
    UNION ALL
    
    SELECT 
        campaign_id, rule_group_id, rule_group_name, person_id,
        event_date, vaccination_type, description, status_type
    FROM flu_declined_status
)

-- Add demographic info
SELECT 
    avs.campaign_id,
    avs.rule_group_id,
    avs.rule_group_name,
    avs.person_id,
    avs.event_date,
    avs.vaccination_type,
    avs.description,
    avs.status_type,
    demo.birth_date_approx,
    DATEDIFF('year', demo.birth_date_approx, {{ get_flu_audit_date(current_campaign) }}) AS age_years,
    {{ get_flu_audit_date(current_campaign) }} AS created_at
FROM all_vaccination_status avs
JOIN {{ ref('dim_person_demographics') }} demo
    ON avs.person_id = demo.person_id

ORDER BY rule_group_id, person_id