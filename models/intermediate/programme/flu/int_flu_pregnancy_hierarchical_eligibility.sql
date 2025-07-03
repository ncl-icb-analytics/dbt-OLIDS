/*
Flu Pregnancy Hierarchical Eligibility Intermediate Model

Implements the complex hierarchical business logic for pregnancy-related flu vaccination eligibility.
This replaces the apply_pregnancy_hierarchical_rule macro functionality.

Business Logic:
- Group 1: Pregnancy since campaign start date (PREG2_DAT), OR
- Group 2: Latest pregnancy/delivery before start date where latest event is pregnancy (not delivery)

The hierarchy ensures that current pregnancies take priority, but also includes women who 
were pregnant at the start of the campaign period.

Age Restrictions: 12-64 years (144 months to 65 years)
*/

{{ config(
    materialized='table',
    post_hook=[
        "COMMENT ON TABLE {{ this }} IS 'Intermediate: Flu Pregnancy Hierarchical Eligibility - Determines flu vaccination eligibility for pregnant women using hierarchical pregnancy evidence.

Clinical Purpose:
• Identifies pregnant women who qualify for flu vaccination based on hierarchical pregnancy evidence
• Prioritises current pregnancies since campaign start over historical pregnancy status
• Supports clinical targeting for high-risk maternal and foetal health protection
• Ensures evidence-based flu vaccination for pregnant women with appropriate timing logic

Data Granularity:
• One row per eligible woman aged 12-64 years with documented pregnancy
• Group 1: Pregnancy since campaign start date (highest priority)
• Group 2: Latest pregnancy before start where pregnancy is more recent than delivery
• Filtered to current campaign with qualifying pregnancy evidence

Key Features:
• Hierarchical pregnancy logic prioritising current over historical pregnancies
• Campaign timing logic ensuring pregnancy relevance to flu season
• Age and sex restrictions: women aged 12-64 years (144 months to 65 years)
• Evidence dating logic comparing pregnancy vs delivery events for historical cases'"
    ]
) }}

{%- set current_campaign = var('flu_current_campaign') -%}

WITH campaign_config AS (
    SELECT 
        campaign_id,
        MAX(CASE WHEN rule_group_id = 'ALL' AND date_type = 'start_dat' THEN date_value END) AS campaign_start_date,
        MAX(CASE WHEN rule_group_id = 'ALL' AND date_type = 'ref_dat' THEN date_value END) AS reference_date
    FROM {{ ref('stg_flu_campaign_dates') }}
    WHERE campaign_id = '{{ current_campaign }}'
    GROUP BY campaign_id
),

-- Get pregnancy codes since campaign start (Group 1)
pregnancy_since_start AS (
    SELECT 
        person_id,
        MAX(clinical_effective_date) AS latest_pregnancy_since_start,
        'Pregnancy since campaign start' AS eligibility_group
    FROM ({{ get_flu_observations_for_rule_group(current_campaign, 'PREG_GROUP') }})
    CROSS JOIN campaign_config cc
    WHERE cluster_id = 'PREG_COD'
        AND clinical_effective_date IS NOT NULL
        AND clinical_effective_date >= cc.campaign_start_date
        AND clinical_effective_date <= {{ get_flu_audit_date(current_campaign) }}
    GROUP BY person_id
),

-- Get pregnancy and delivery codes before campaign start (Group 2)
pregnancy_before_start AS (
    SELECT 
        person_id,
        clinical_effective_date,
        cluster_id,
        CASE 
            WHEN cluster_id = 'PREG_COD' THEN 'pregnancy'
            WHEN cluster_id = 'PREGDEL_COD' THEN 'delivery'
            ELSE 'other'
        END AS event_type
    FROM ({{ get_flu_observations_for_rule_group(current_campaign, 'PREG_GROUP') }})
    CROSS JOIN campaign_config cc
    WHERE cluster_id IN ('PREG_COD', 'PREGDEL_COD')
        AND clinical_effective_date IS NOT NULL
        AND clinical_effective_date < cc.campaign_start_date
        AND clinical_effective_date <= {{ get_flu_audit_date(current_campaign) }}
),

-- Get latest pregnancy and delivery events before start for each person
latest_preg_del_before_start AS (
    SELECT 
        person_id,
        MAX(CASE WHEN event_type = 'pregnancy' THEN clinical_effective_date END) AS latest_pregnancy_before_start,
        MAX(CASE WHEN event_type = 'delivery' THEN clinical_effective_date END) AS latest_delivery_before_start
    FROM pregnancy_before_start
    GROUP BY person_id
),

-- Group 2: Include if latest event before start was pregnancy (not delivery)
pregnancy_group2_eligible AS (
    SELECT 
        person_id,
        latest_pregnancy_before_start AS qualifying_event_date,
        'Pregnant at campaign start (latest event was pregnancy)' AS eligibility_group,
        latest_pregnancy_before_start,
        latest_delivery_before_start
    FROM latest_preg_del_before_start
    WHERE latest_pregnancy_before_start IS NOT NULL
        AND (latest_delivery_before_start IS NULL OR latest_pregnancy_before_start > latest_delivery_before_start)
),

-- Union both groups
all_pregnancy_eligible AS (
    SELECT 
        person_id,
        latest_pregnancy_since_start AS qualifying_event_date,
        eligibility_group,
        latest_pregnancy_since_start AS pregnancy_date,
        NULL AS delivery_date
    FROM pregnancy_since_start
    
    UNION
    
    SELECT 
        person_id,
        qualifying_event_date,
        eligibility_group,
        latest_pregnancy_before_start AS pregnancy_date,
        latest_delivery_before_start AS delivery_date
    FROM pregnancy_group2_eligible
),

-- Remove duplicates and get the best qualifying event per person
best_pregnancy_eligible AS (
    SELECT 
        person_id,
        qualifying_event_date,
        eligibility_group,
        pregnancy_date,
        delivery_date,
        ROW_NUMBER() OVER (PARTITION BY person_id ORDER BY qualifying_event_date DESC, eligibility_group) AS rn
    FROM all_pregnancy_eligible
),

-- Add campaign information
pregnancy_campaign_eligible AS (
    SELECT 
        cc.campaign_id,
        'PREG_GROUP' AS rule_group_id,
        'Pregnant' AS rule_group_name,
        bpe.person_id,
        bpe.qualifying_event_date,
        bpe.eligibility_group,
        bpe.pregnancy_date,
        bpe.delivery_date,
        cc.reference_date,
        'Pregnant women aged 12-64' AS description
    FROM best_pregnancy_eligible bpe
    CROSS JOIN campaign_config cc
    WHERE bpe.rn = 1
)

-- Apply age restrictions and add demographic info (women only)
SELECT 
    pce.campaign_id,
    pce.rule_group_id,
    pce.rule_group_name,
    pce.person_id,
    pce.qualifying_event_date,
    pce.eligibility_group,
    pce.pregnancy_date,
    pce.delivery_date,
    pce.reference_date,
    pce.description,
    demo.birth_date_approx,
    demo.sex AS person_sex,
    DATEDIFF('month', demo.birth_date_approx, pce.reference_date) AS age_months_at_ref_date,
    DATEDIFF('year', demo.birth_date_approx, pce.reference_date) AS age_years_at_ref_date,
    {{ get_flu_audit_date(current_campaign) }} AS created_at
FROM pregnancy_campaign_eligible pce
JOIN {{ ref('dim_person_demographics') }} demo
    ON pce.person_id = demo.person_id
WHERE 1=1
    -- Age restrictions: 12-64 years (144 months to 65 years, as per flu_programme_logic.csv)
    AND DATEDIFF('month', demo.birth_date_approx, pce.reference_date) >= 144  -- 12 years
    AND DATEDIFF('year', demo.birth_date_approx, pce.reference_date) < 65
    -- Women only (pregnancy eligibility)
    AND UPPER(demo.sex) IN ('F', 'FEMALE', 'WOMAN')

ORDER BY person_id