/*
Simplified Flu Vaccination Given Rule

Business Rule: Person with flu vaccination status if they have:
1. Flu vaccination administration code (FLUVAX_COD) after specified date, OR
2. Flu vaccination medication (FLURX_COD) after specified date, OR  
3. LAIV vaccination administration code (LAIV_COD) after specified date, OR
4. LAIV vaccination medication (LAIVRX_COD) after specified date
5. No age restrictions (applies to all ages)

Combination rule - tracks people with flu vaccination records (including LAIV).
This is used for tracking/reporting, not eligibility determination.
Note: LAIV vaccinations are included here because LAIV is a type of flu vaccination.
*/

{{ config(materialized='table') }}

WITH all_campaigns AS (
    -- Generate data for both current and previous campaigns automatically
    SELECT * FROM ({{ flu_campaign_config(var('flu_current_campaign', 'flu_2024_25')) }})
    UNION ALL
    SELECT * FROM ({{ flu_campaign_config(var('flu_previous_campaign', 'flu_2023_24')) }})
),

-- Step 1: Find people with flu vaccination administration codes (for all campaigns)
people_with_flu_vaccination_admin AS (
    SELECT 
        cc.campaign_id,
        obs.person_id,
        MAX(obs.clinical_effective_date) AS latest_vaccination_admin_date,
        'Flu vaccination administration' AS vaccination_type
    FROM ({{ get_observations("'FLUVAX_COD'", 'UKHSA_FLU') }}) obs
    CROSS JOIN all_campaigns cc
    WHERE obs.clinical_effective_date IS NOT NULL
        AND obs.clinical_effective_date > cc.flu_vaccination_after_date
        AND obs.clinical_effective_date <= cc.campaign_end_date
    GROUP BY cc.campaign_id, obs.person_id
),

-- Step 2: Find people with flu vaccination medication orders (for all campaigns)
people_with_flu_vaccination_medication AS (
    SELECT 
        cc.campaign_id,
        med.person_id,
        MAX(med.order_date) AS latest_vaccination_medication_date,
        'Flu vaccination medication' AS vaccination_type
    FROM ({{ get_medication_orders(cluster_id='FLURX_COD', source='UKHSA_FLU') }}) med
    CROSS JOIN all_campaigns cc
    WHERE med.order_date IS NOT NULL
        AND med.order_date > cc.flu_vaccination_after_date
        AND med.order_date <= cc.campaign_end_date
    GROUP BY cc.campaign_id, med.person_id
),

-- Step 2b: Find people with LAIV vaccination administration codes (LAIV is also flu vaccination)
people_with_laiv_vaccination_admin AS (
    SELECT 
        cc.campaign_id,
        obs.person_id,
        MAX(obs.clinical_effective_date) AS latest_laiv_admin_date,
        'LAIV vaccination administration' AS vaccination_type
    FROM ({{ get_observations("'LAIV_COD'", 'UKHSA_FLU') }}) obs
    CROSS JOIN all_campaigns cc
    WHERE obs.clinical_effective_date IS NOT NULL
        AND obs.clinical_effective_date > cc.laiv_vaccination_after_date
        AND obs.clinical_effective_date <= cc.campaign_end_date
    GROUP BY cc.campaign_id, obs.person_id
),

-- Step 2c: Find people with LAIV vaccination medication orders (LAIV is also flu vaccination)
people_with_laiv_vaccination_medication AS (
    SELECT 
        cc.campaign_id,
        med.person_id,
        MAX(med.order_date) AS latest_laiv_medication_date,
        'LAIV vaccination medication' AS vaccination_type
    FROM ({{ get_medication_orders(cluster_id='LAIVRX_COD', source='UKHSA_FLU') }}) med
    CROSS JOIN all_campaigns cc
    WHERE med.order_date IS NOT NULL
        AND med.order_date > cc.laiv_vaccination_after_date
        AND med.order_date <= cc.campaign_end_date
    GROUP BY cc.campaign_id, med.person_id
),

-- Step 3: Combine all flu vaccination evidence (for all campaigns, including LAIV)
all_flu_vaccination_evidence AS (
    SELECT campaign_id, person_id, latest_vaccination_admin_date AS vaccination_date, vaccination_type
    FROM people_with_flu_vaccination_admin
    
    UNION ALL
    
    SELECT campaign_id, person_id, latest_vaccination_medication_date, vaccination_type
    FROM people_with_flu_vaccination_medication
    
    UNION ALL
    
    SELECT campaign_id, person_id, latest_laiv_admin_date, vaccination_type
    FROM people_with_laiv_vaccination_admin
    
    UNION ALL
    
    SELECT campaign_id, person_id, latest_laiv_medication_date, vaccination_type
    FROM people_with_laiv_vaccination_medication
),

-- Step 4: Get the most recent vaccination per person per campaign
best_flu_vaccination_evidence AS (
    SELECT 
        campaign_id,
        person_id,
        vaccination_type,
        vaccination_date,
        ROW_NUMBER() OVER (PARTITION BY campaign_id, person_id ORDER BY vaccination_date DESC) AS rn
    FROM all_flu_vaccination_evidence
),

-- Step 5: Add demographics (no age restrictions for vaccination tracking)
final_eligibility AS (
    SELECT 
        bfve.campaign_id,
        'Vaccination Tracking' AS campaign_category,
        'Flu Vaccination Given' AS risk_group,
        bfve.person_id,
        bfve.vaccination_date AS qualifying_event_date,
        bfve.vaccination_type,
        cc.campaign_reference_date AS reference_date,
        'People with flu vaccination administration records (including LAIV)' AS description,
        demo.birth_date_approx,
        DATEDIFF('month', demo.birth_date_approx, cc.campaign_reference_date) AS age_months_at_ref_date,
        DATEDIFF('year', demo.birth_date_approx, cc.campaign_reference_date) AS age_years_at_ref_date,
        cc.audit_end_date AS created_at
    FROM best_flu_vaccination_evidence bfve
    JOIN all_campaigns cc ON bfve.campaign_id = cc.campaign_id
    JOIN {{ ref('dim_person_demographics') }} demo
        ON bfve.person_id = demo.person_id
    WHERE bfve.rn = 1  -- Only the most recent vaccination per person per campaign
)

SELECT * FROM final_eligibility
ORDER BY person_id