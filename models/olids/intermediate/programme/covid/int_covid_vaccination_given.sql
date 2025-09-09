/*
COVID Vaccination Given Rule

Business Rule: Person with COVID vaccination status if they have:
1. COVID vaccination administration code (COVADM_COD) within campaign period, OR
2. COVID vaccination medication (COVRX_COD) within campaign period
3. No age restrictions (applies to all ages)

This is used for tracking/reporting vaccination uptake, not eligibility determination.
Tracks vaccinations by campaign period (autumn vs spring).
*/

{{ config(materialized='table') }}

WITH all_campaigns AS (
    -- Generate data for both current and previous campaigns automatically
    SELECT * FROM ({{ covid_campaign_config(var('covid_current_campaign', 'covid_2025_autumn')) }})
    UNION ALL
    SELECT * FROM ({{ covid_campaign_config(var('covid_previous_campaign', 'covid_2024_autumn')) }})
),

-- Step 1: Find people with COVID vaccination administration codes (for all campaigns)
people_with_covid_vaccination_admin AS (
    SELECT 
        cc.campaign_id,
        obs.person_id,
        MAX(obs.clinical_effective_date) AS latest_vaccination_admin_date,
        'COVID vaccination administration' AS vaccination_type
    FROM ({{ get_observations("'COVADM_COD'", 'UKHSA_COVID') }}) obs
    CROSS JOIN all_campaigns cc
    WHERE obs.clinical_effective_date IS NOT NULL
        AND obs.clinical_effective_date >= cc.vaccination_tracking_start
        AND obs.clinical_effective_date <= cc.vaccination_tracking_end
    GROUP BY cc.campaign_id, obs.person_id
),

-- Step 2: Find people with COVID vaccination medication orders (for all campaigns)
people_with_covid_vaccination_medication AS (
    SELECT 
        cc.campaign_id,
        med.person_id,
        MAX(med.order_date) AS latest_vaccination_medication_date,
        'COVID vaccination medication' AS vaccination_type
    FROM ({{ get_medication_orders(cluster_id='COVRX_COD', source='UKHSA_COVID') }}) med
    CROSS JOIN all_campaigns cc
    WHERE med.order_date IS NOT NULL
        AND med.order_date >= cc.vaccination_tracking_start
        AND med.order_date <= cc.vaccination_tracking_end
    GROUP BY cc.campaign_id, med.person_id
),

-- Step 3: Union all vaccination evidence
all_vaccination_evidence AS (
    SELECT 
        campaign_id, person_id, latest_vaccination_admin_date AS vaccination_date, vaccination_type
    FROM people_with_covid_vaccination_admin
    
    UNION ALL
    
    SELECT 
        campaign_id, person_id, latest_vaccination_medication_date AS vaccination_date, vaccination_type
    FROM people_with_covid_vaccination_medication
),

-- Step 4: Get latest vaccination date for each person/campaign
people_with_covid_vaccination AS (
    SELECT 
        ave.campaign_id,
        ave.person_id,
        MAX(ave.vaccination_date) AS latest_vaccination_date,
        LISTAGG(DISTINCT ave.vaccination_type, '; ') AS vaccination_types,
        cc.campaign_reference_date,
        cc.vaccination_tracking_start,
        cc.vaccination_tracking_end
    FROM all_vaccination_evidence ave
    LEFT JOIN all_campaigns cc ON ave.campaign_id = cc.campaign_id
    GROUP BY 
        ave.campaign_id, ave.person_id, cc.campaign_reference_date,
        cc.vaccination_tracking_start, cc.vaccination_tracking_end
),

-- Step 5: Add age information (for demographics, no age restrictions)
people_vaccinated_with_age AS (
    SELECT 
        pcv.campaign_id,
        pcv.person_id,
        demo.birth_date_approx,
        DATEDIFF('year', demo.birth_date_approx, pcv.campaign_reference_date) AS age_years_at_ref_date,
        DATEDIFF('month', demo.birth_date_approx, pcv.campaign_reference_date) AS age_months_at_ref_date,
        pcv.latest_vaccination_date AS qualifying_event_date,
        pcv.campaign_reference_date,
        pcv.vaccination_types
    FROM people_with_covid_vaccination pcv
    LEFT JOIN {{ ref('dim_person_demographics') }} demo 
        ON pcv.person_id = demo.person_id
    WHERE demo.is_active = TRUE
        AND demo.birth_date_approx IS NOT NULL
),

-- Step 6: Format for status table
final_vaccinated AS (
    SELECT 
        campaign_id,
        'VACCINATION_ADMINISTERED' AS campaign_category,
        'COVID Vaccination Given' AS risk_group,
        person_id,
        qualifying_event_date,
        campaign_reference_date AS reference_date,
        CONCAT('COVID vaccination administered: ', vaccination_types) AS description,
        birth_date_approx,
        age_months_at_ref_date,
        age_years_at_ref_date,
        CURRENT_TIMESTAMP() AS created_at
    FROM people_vaccinated_with_age
)

SELECT * FROM final_vaccinated
ORDER BY campaign_id, person_id