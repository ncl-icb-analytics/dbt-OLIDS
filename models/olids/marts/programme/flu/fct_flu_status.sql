/*
Flu Vaccination Status Fact Table

This model provides comprehensive vaccination status tracking including:
1. All vaccination activity (administered, declined, LAIV) with eligibility status
2. Eligible people with no vaccination records
3. Non-eligible people who received vaccinations

Key features:
- Complete vaccination coverage analysis
- Eligibility flags for all records
- Tracks gaps in vaccination (eligible but no record)
- Identifies non-eligible vaccinations
- Works automatically with both current and previous campaigns

This gives a complete picture of vaccination patterns across the population.
*/

{{ config(
    materialized='table',
    cluster_by=['campaign_id', 'person_id', 'status_type']
) }}

WITH
-- All eligible people (from eligibility table)
all_eligible_people AS (
    SELECT DISTINCT
        campaign_id,
        person_id,
        birth_date_approx,
        age_months,
        age_years,
        reference_date,
        created_at
    FROM {{ ref('fct_flu_eligibility') }}
),

-- All vaccination administration records (both campaigns automatically included from intermediate models)
vaccination_administration AS (
    -- Flu Vaccination Administration (includes LAIV)
    SELECT 
        campaign_id, campaign_category, risk_group, person_id, qualifying_event_date, reference_date,
        description, birth_date_approx, age_months_at_ref_date, age_years_at_ref_date,
        'VACCINATION_ADMINISTERED' AS status_type, 1 AS status_priority, created_at
    FROM {{ ref('int_flu_vaccination_given') }}
    
    UNION ALL
    
    -- LAIV Administration (separate tracking for LAIV-specific analysis)
    SELECT 
        campaign_id, campaign_category, risk_group, person_id, qualifying_event_date, reference_date,
        description, birth_date_approx, age_months_at_ref_date, age_years_at_ref_date,
        'LAIV_ADMINISTERED' AS status_type, 1 AS status_priority, created_at
    FROM {{ ref('int_flu_laiv_vaccination') }}
),

-- Vaccination declination records
vaccination_declined AS (
    SELECT 
        campaign_id, campaign_category, risk_group, person_id, qualifying_event_date, reference_date,
        description, birth_date_approx, age_months_at_ref_date, age_years_at_ref_date,
        'VACCINATION_DECLINED' AS status_type, 2 AS status_priority, created_at
    FROM {{ ref('int_flu_vaccination_declined') }}
),

-- All vaccination activity (administered + declined)
all_vaccination_activity AS (
    SELECT * FROM vaccination_administration
    UNION ALL
    SELECT * FROM vaccination_declined
),

-- Eligible people with no vaccination records
eligible_no_vaccination AS (
    SELECT 
        ep.campaign_id,
        'NO_VAX_RECORD' AS campaign_category,
        'No Vaccination Record' AS risk_group,
        ep.person_id,
        NULL AS qualifying_event_date,
        ep.reference_date,
        'Eligible person with no vaccination record' AS description,
        ep.birth_date_approx,
        ep.age_months AS age_months_at_ref_date,
        ep.age_years AS age_years_at_ref_date,
        'NO_VACCINATION_RECORD' AS status_type,
        3 AS status_priority,
        ep.created_at
    FROM all_eligible_people ep
    WHERE NOT EXISTS (
        SELECT 1 
        FROM all_vaccination_activity ava
        WHERE ava.person_id = ep.person_id 
            AND ava.campaign_id = ep.campaign_id
    )
),

-- Union all status types including eligible people with no vaccination records
all_vaccination_status AS (
    SELECT * FROM all_vaccination_activity
    UNION ALL
    SELECT * FROM eligible_no_vaccination
),

-- Final formatting with eligibility flags
final_status AS (
    SELECT 
        avs.campaign_id,
        avs.campaign_category,
        avs.risk_group,
        avs.person_id,
        avs.qualifying_event_date AS status_date,
        avs.reference_date,
        avs.description AS status_reason,
        avs.status_type,
        avs.status_priority,
        avs.birth_date_approx,
        avs.age_months_at_ref_date AS age_months,
        avs.age_years_at_ref_date AS age_years,
        -- Eligibility flags
        CASE 
            WHEN aep.person_id IS NOT NULL THEN TRUE 
            ELSE FALSE 
        END AS is_eligible,
        CASE 
            WHEN aep.person_id IS NOT NULL THEN 'Eligible'
            ELSE 'Not Eligible'
        END AS eligibility_status,
        CASE
            WHEN aep.person_id IS NULL AND avs.status_type IN ('VACCINATION_ADMINISTERED', 'LAIV_ADMINISTERED') 
            THEN TRUE 
            ELSE FALSE 
        END AS vaccinated_despite_ineligible,
        avs.created_at
    FROM all_vaccination_status avs
    LEFT JOIN all_eligible_people aep 
        ON avs.person_id = aep.person_id 
        AND avs.campaign_id = aep.campaign_id
)

SELECT * FROM final_status
ORDER BY person_id, status_priority, campaign_category