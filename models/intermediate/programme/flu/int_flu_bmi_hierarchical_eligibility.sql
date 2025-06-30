/*
Flu BMI Hierarchical Eligibility Intermediate Model

Implements the complex hierarchical business logic for BMI-related flu vaccination eligibility.
This replaces the apply_bmi_hierarchical_rule macro functionality.

Business Logic (for severe obesity BMI 40+):
- BMI value >= 40 AND BMI >= stage code, OR
- (No stage code AND BMI >= 40), OR
- (Severe obesity code > BMI code OR no BMI code)

The hierarchy ensures that the most recent and severe obesity status is used.

Age Restrictions: 18-64 years (216 months to 65 years)
*/

{{ config(materialized='table') }}

{%- set current_campaign = var('flu_current_campaign') -%}

WITH campaign_config AS (
    SELECT 
        campaign_id,
        MAX(CASE WHEN rule_group_id = 'ALL' AND date_type = 'ref_dat' THEN date_value END) AS reference_date
    FROM {{ ref('stg_flu_campaign_dates') }}
    WHERE campaign_id = '{{ current_campaign }}'
    GROUP BY campaign_id
),

-- Get BMI values (latest occurrence)
bmi_values AS (
    SELECT 
        person_id,
        MAX(clinical_effective_date) AS latest_bmi_date,
        -- Extract BMI value from observation (assuming numeric value in result_value)
        MAX(result_value) AS latest_bmi_value
    FROM ({{ get_flu_observations_for_rule_group(current_campaign, 'BMI_GROUP') }})
    WHERE cluster_id = 'BMI_COD'
        AND clinical_effective_date IS NOT NULL
        AND clinical_effective_date <= {{ get_flu_audit_date(current_campaign) }}
        AND result_value IS NOT NULL
        AND result_value >= 15  -- Reasonable BMI range
        AND result_value <= 80
    GROUP BY person_id
),

-- Get BMI stage codes (latest occurrence)
bmi_stages AS (
    SELECT 
        person_id,
        MAX(clinical_effective_date) AS latest_stage_date
    FROM ({{ get_flu_observations_for_rule_group(current_campaign, 'BMI_GROUP') }})
    WHERE cluster_id = 'BMI_STAGE_COD'
        AND clinical_effective_date IS NOT NULL
        AND clinical_effective_date <= {{ get_flu_audit_date(current_campaign) }}
    GROUP BY person_id
),

-- Get severe obesity codes (latest occurrence)
severe_obesity_codes AS (
    SELECT 
        person_id,
        MAX(clinical_effective_date) AS latest_severe_obesity_date
    FROM ({{ get_flu_observations_for_rule_group(current_campaign, 'BMI_GROUP') }})
    WHERE cluster_id = 'SEV_OBESITY_COD'
        AND clinical_effective_date IS NOT NULL
        AND clinical_effective_date <= {{ get_flu_audit_date(current_campaign) }}
    GROUP BY person_id
),

-- Apply hierarchical BMI logic
bmi_eligible AS (
    -- Path 1: BMI >= 40 and BMI >= stage date (or no stage)
    SELECT 
        bv.person_id,
        bv.latest_bmi_date AS qualifying_event_date,
        'BMI >= 40 (measured)' AS eligibility_path,
        bv.latest_bmi_value,
        bs.latest_stage_date,
        NULL AS severe_obesity_date,
        'BMI ' || bv.latest_bmi_value || ' >= 40' AS eligibility_detail
    FROM bmi_values bv
    LEFT JOIN bmi_stages bs
        ON bv.person_id = bs.person_id
    WHERE bv.latest_bmi_value >= 40
        AND (bs.latest_stage_date IS NULL OR bv.latest_bmi_date >= bs.latest_stage_date)
    
    UNION
    
    -- Path 2: Severe obesity code more recent than BMI or no BMI
    SELECT 
        soc.person_id,
        soc.latest_severe_obesity_date AS qualifying_event_date,
        'Severe obesity code' AS eligibility_path,
        bv.latest_bmi_value,
        NULL AS latest_stage_date,
        soc.latest_severe_obesity_date,
        'Severe obesity code (' || soc.latest_severe_obesity_date || ') > BMI (' || COALESCE(bv.latest_bmi_date::VARCHAR, 'none') || ')' AS eligibility_detail
    FROM severe_obesity_codes soc
    LEFT JOIN bmi_values bv
        ON soc.person_id = bv.person_id
    WHERE soc.latest_severe_obesity_date > COALESCE(bv.latest_bmi_date, '1900-01-01'::DATE)
),

-- Remove duplicates and get the best qualifying event per person
best_bmi_eligible AS (
    SELECT 
        person_id,
        qualifying_event_date,
        eligibility_path,
        latest_bmi_value,
        eligibility_detail,
        ROW_NUMBER() OVER (PARTITION BY person_id ORDER BY qualifying_event_date DESC, eligibility_path) AS rn
    FROM bmi_eligible
),

-- Add campaign information
bmi_campaign_eligible AS (
    SELECT 
        cc.campaign_id,
        'BMI_GROUP' AS rule_group_id,
        'Morbid Obesity' AS rule_group_name,
        bbe.person_id,
        bbe.qualifying_event_date,
        bbe.eligibility_path,
        bbe.latest_bmi_value,
        bbe.eligibility_detail,
        cc.reference_date,
        'Adults aged 18-64 with severe obesity (BMI 40+)' AS description
    FROM best_bmi_eligible bbe
    CROSS JOIN campaign_config cc
    WHERE bbe.rn = 1
)

-- Apply age restrictions and add demographic info
SELECT 
    bce.campaign_id,
    bce.rule_group_id,
    bce.rule_group_name,
    bce.person_id,
    bce.qualifying_event_date,
    bce.eligibility_path,
    bce.latest_bmi_value,
    bce.eligibility_detail,
    bce.reference_date,
    bce.description,
    demo.birth_date_approx,
    DATEDIFF('month', demo.birth_date_approx, bce.reference_date) AS age_months_at_ref_date,
    DATEDIFF('year', demo.birth_date_approx, bce.reference_date) AS age_years_at_ref_date,
    {{ get_flu_audit_date(current_campaign) }} AS created_at
FROM bmi_campaign_eligible bce
JOIN {{ ref('dim_person_demographics') }} demo
    ON bce.person_id = demo.person_id
WHERE 1=1
    -- Age restrictions: 18-64 years (216 months to 65 years, as per flu_programme_logic.csv)
    AND DATEDIFF('month', demo.birth_date_approx, bce.reference_date) >= 216  -- 18 years
    AND DATEDIFF('year', demo.birth_date_approx, bce.reference_date) < 65

ORDER BY person_id