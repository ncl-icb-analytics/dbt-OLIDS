/*
Simplified Flu Vaccination Declined Rule

Business Rule: Person has declined flu vaccination if they have:
1. Vaccination declined code (DECL_COD) OR no consent code (NOCONS_COD) within the campaign period
2. AND have NOT been vaccinated (not in FLUVAX_GROUP) 
3. No age restrictions (applies to all ages)

Date Restrictions: Only considers declined codes after the campaign's vaccination tracking date
to avoid counting historical declines from previous campaigns.

Exclusion rule - tracks people with vaccination declination records.
This is used for reporting/exclusion purposes.
*/

{{ config(materialized='table') }}

WITH all_campaigns AS (
    -- Generate data for both current and previous campaigns automatically
    SELECT * FROM ({{ flu_campaign_config(var('flu_current_campaign', 'flu_2024_25')) }})
    UNION ALL
    SELECT * FROM ({{ flu_campaign_config(var('flu_previous_campaign', 'flu_2023_24')) }})
),

-- Step 1: Find people with vaccination declined codes (for all campaigns)
people_with_declined_codes AS (
    SELECT 
        cc.campaign_id,
        obs.person_id,
        MAX(obs.clinical_effective_date) AS latest_declined_date,
        'Vaccination declined' AS decline_type
    FROM ({{ get_observations("'DECL_COD'", 'UKHSA_FLU') }}) obs
    CROSS JOIN all_campaigns cc
    WHERE obs.clinical_effective_date IS NOT NULL
        -- Restrict to current campaign period (after previous campaign's vaccination tracking date)
        AND obs.clinical_effective_date > cc.flu_vaccination_after_date
        AND obs.clinical_effective_date <= cc.campaign_end_date
    GROUP BY cc.campaign_id, obs.person_id
),

-- Step 2: Find people with no consent codes (for all campaigns)
people_with_no_consent_codes AS (
    SELECT 
        cc.campaign_id,
        obs.person_id,
        MAX(obs.clinical_effective_date) AS latest_no_consent_date,
        'No consent for vaccination' AS decline_type
    FROM ({{ get_observations("'NOCONS_COD'", 'UKHSA_FLU') }}) obs
    CROSS JOIN all_campaigns cc
    WHERE obs.clinical_effective_date IS NOT NULL
        -- Restrict to current campaign period (after previous campaign's vaccination tracking date)
        AND obs.clinical_effective_date > cc.flu_vaccination_after_date
        AND obs.clinical_effective_date <= cc.campaign_end_date
    GROUP BY cc.campaign_id, obs.person_id
),

-- Step 3: Combine all declined vaccination evidence (for all campaigns)
all_declined_vaccination_evidence AS (
    SELECT campaign_id, person_id, latest_declined_date AS decline_date, decline_type
    FROM people_with_declined_codes
    
    UNION ALL
    
    SELECT campaign_id, person_id, latest_no_consent_date, decline_type
    FROM people_with_no_consent_codes
),

-- Step 4: Get the most recent declined vaccination per person per campaign
best_declined_vaccination_evidence AS (
    SELECT 
        campaign_id,
        person_id,
        decline_type,
        decline_date,
        ROW_NUMBER() OVER (PARTITION BY campaign_id, person_id ORDER BY decline_date DESC) AS rn
    FROM all_declined_vaccination_evidence
),

-- Step 5: Exclude people with vaccination records (per campaign)
people_who_declined_and_not_vaccinated AS (
    SELECT 
        bdve.campaign_id,
        bdve.person_id,
        bdve.decline_type,
        bdve.decline_date
    FROM best_declined_vaccination_evidence bdve
    WHERE bdve.rn = 1  -- Most recent decline per campaign
        AND NOT EXISTS (
            SELECT 1 
            FROM {{ ref('int_flu_vaccination_given') }} vg
            WHERE vg.person_id = bdve.person_id
                AND vg.campaign_id = bdve.campaign_id
        )
),

-- Step 6: Add demographics (no age restrictions for vaccination tracking)
final_eligibility AS (
    SELECT 
        pwdnv.campaign_id,
        'Vaccination Tracking' AS campaign_category,
        'Flu Vaccination Declined' AS risk_group,
        pwdnv.person_id,
        pwdnv.decline_date AS qualifying_event_date,
        pwdnv.decline_type,
        cc.campaign_reference_date AS reference_date,
        'People with flu vaccination declination records' AS description,
        demo.birth_date_approx,
        DATEDIFF('month', demo.birth_date_approx, cc.campaign_reference_date) AS age_months_at_ref_date,
        DATEDIFF('year', demo.birth_date_approx, cc.campaign_reference_date) AS age_years_at_ref_date,
        cc.audit_end_date AS created_at
    FROM people_who_declined_and_not_vaccinated pwdnv
    JOIN all_campaigns cc ON pwdnv.campaign_id = cc.campaign_id
    JOIN {{ ref('dim_person_demographics') }} demo
        ON pwdnv.person_id = demo.person_id
)

SELECT * FROM final_eligibility
ORDER BY person_id