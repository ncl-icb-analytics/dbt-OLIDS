{{ config(
    materialized='table',
    description='Implements clinical decision logic for Valproate safety monitoring, determining recommended actions for each patient based on clinical status and dependencies.') }}

WITH db_scope AS (
    SELECT * FROM {{ ref('dim_valproate_db_scope') }}
),

ppp_status AS (
    SELECT * FROM {{ ref('dim_valproate_ppp_status') }}
),

araf AS (
    SELECT * FROM {{ ref('dim_valproate_araf') }}
),

araf_referral AS (
    SELECT * FROM {{ ref('dim_valproate_araf_referral') }}
),

neurology AS (
    SELECT * FROM {{ ref('dim_valproate_neurology') }}
),

psychiatry AS (
    SELECT * FROM {{ ref('dim_valproate_psychiatry') }}
),

pregnancy_status AS (
    SELECT
        p.person_id,
        
        -- Pregnancy logic: recent pregnancy code (last 9 months) after any delivery code
        COALESCE(
            preg.latest_preg_date IS NOT NULL
            AND preg.latest_preg_date >= DATEADD(MONTH, -9, CURRENT_DATE())
            AND (
                preg.latest_delivery_date IS NULL
                OR preg.latest_preg_date > preg.latest_delivery_date
            ), FALSE
        ) AS is_currently_pregnant,
        
        -- Permanent absence of pregnancy risk flag
        COALESCE(perm.person_id IS NOT NULL, FALSE) AS has_permanent_absence_of_pregnancy_risk
        
    FROM {{ ref('dim_person') }} AS p
    INNER JOIN {{ ref('dim_person_sex') }} AS sex ON p.person_id = sex.person_id
    LEFT JOIN (
        SELECT
            person_id,
            MAX(CASE WHEN is_pregnancy_code = TRUE THEN clinical_effective_date END) AS latest_preg_date,
            MAX(CASE WHEN is_delivery_outcome_code = TRUE THEN clinical_effective_date END) AS latest_delivery_date
        FROM {{ ref('int_pregnancy_observations_all') }}
        GROUP BY person_id
    ) AS preg ON p.person_id = preg.person_id
    LEFT JOIN (
        SELECT DISTINCT person_id
        FROM {{ ref('int_pregnancy_absence_risk_all') }}
    ) AS perm ON p.person_id = perm.person_id
    WHERE sex.sex != 'Male' -- Only non-male individuals
),

learning_disability AS (
    SELECT
        person_id,
        is_on_register
    FROM {{ ref('fct_person_learning_disability_register') }}
)

SELECT
    db.person_id,
    db.age,
    db.sex,
    
    -- PPP Status
    COALESCE(ppp.has_ppp_event, FALSE) AS has_ppp_status,
    COALESCE(ppp.is_currently_ppp_enrolled, FALSE) AS is_ppp_enrolled,
    COALESCE(ppp.is_ppp_non_enrolled, FALSE) AS is_ppp_non_enrolled,
    COALESCE(ppp.current_ppp_status_description, 'No - No entry found') AS ppp_status_description,
    
    -- ARAF Status
    COALESCE(araf.has_araf_event, FALSE) AS has_araf_event,
    COALESCE(araf.has_specific_araf_form_meeting_lookback, FALSE) AS has_current_araf,
    
    -- Risk Assessment Flags
    COALESCE(preg_status.has_permanent_absence_of_pregnancy_risk, FALSE) AS has_permanent_absence_pregnancy_risk,
    COALESCE(ld.is_on_register, FALSE) AS has_learning_disability,
    COALESCE(preg_status.is_currently_pregnant, FALSE) AS has_pregnancy,
    
    -- Condition Flags
    COALESCE(neu.has_neurology_event, FALSE) AS has_neurology,
    COALESCE(psych.has_psych_event, FALSE) AS has_psychiatry,
    
    -- Risk of Pregnancy Classification
    CASE
        WHEN db.age BETWEEN 0 AND 6 OR COALESCE(preg_status.has_permanent_absence_of_pregnancy_risk, FALSE) = TRUE THEN 'Low Risk'
        WHEN db.age BETWEEN 7 AND 12 THEN 'Medium Risk'
        ELSE 'High Risk'
    END AS risk_of_pregnancy,
    
    /*
    ACTION DETERMINATION LOGIC:
    
    This implements a clinical decision tree for Valproate safety monitoring.
    Actions are determined by safety documentation completeness, then prioritised by vulnerability.
    
    CLINICAL ACTIONS:
    - "Review or refer": Missing safety documentation or high-risk situations requiring immediate clinical review
    - "Keep under review": PPP declined/discontinued but documented - routine monitoring sufficient
    - "Consider expiry of ARAF": All safety measures present - check if ARAF needs renewal
    - "No action required": Low risk patients requiring minimal intervention
    
    SAFETY DOCUMENTATION REQUIREMENTS:
    - PPP (Pregnancy Prevention Programme): Must be enrolled or explicitly declined/discontinued
    - ARAF (Annual Risk Acknowledgement Form): Must be current (within lookback period)
    */
    CASE
        -- PRIORITY 1: Pregnancy detected - URGENT clinical review required
        WHEN COALESCE(preg_status.is_currently_pregnant, FALSE) = TRUE THEN 'Review or refer'
        
        -- PRIORITY 9: Low risk - minimal intervention needed
        WHEN db.age BETWEEN 0 AND 6 OR COALESCE(preg_status.has_permanent_absence_of_pregnancy_risk, FALSE) = TRUE THEN 'No action required'
        
        -- PRIORITY 5: PPP declined/discontinued - routine monitoring
        WHEN COALESCE(ppp.is_ppp_non_enrolled, FALSE) = TRUE THEN 'Keep under review'
        
        -- PRIORITIES 2-4: Missing safety documentation - clinical review required
        WHEN COALESCE(ppp.has_ppp_event, FALSE) = FALSE OR COALESCE(araf.has_specific_araf_form_meeting_lookback, FALSE) = FALSE THEN 'Review or refer'
        
        -- PRIORITIES 6-8: Complete safety documentation - routine ARAF monitoring
        WHEN COALESCE(ppp.is_currently_ppp_enrolled, FALSE) = TRUE AND COALESCE(araf.has_specific_araf_form_meeting_lookback, FALSE) = TRUE THEN 'Consider expiry of ARAF'
        
        ELSE 'Review or refer'
    END AS action,
    
    /*
    ACTION PRIORITY ORDER (1 = Most Urgent, 9 = Least Urgent):
    
    Prioritisation is based on clinical risk and vulnerability factors:
    
    1. PREGNANCY: Immediate teratogenic risk - urgent review required
    2. MISSING DOCUMENTATION + LEARNING DISABILITY: Vulnerable population without safety measures
    3. MISSING DOCUMENTATION + HIGH RISK AGE (13-60): Prime reproductive age without safety measures  
    4. MISSING DOCUMENTATION + MEDIUM RISK AGE (7-12): Approaching reproductive age without safety measures
    5. PPP DECLINED: Documented decision to decline PPP - routine monitoring
    6. COMPLETE DOCUMENTATION + LEARNING DISABILITY: Monitor ARAF expiry in vulnerable population
    7. COMPLETE DOCUMENTATION + MEDIUM RISK AGE (7-12): Monitor ARAF expiry 
    8. COMPLETE DOCUMENTATION + HIGH RISK AGE (13-60): Monitor ARAF expiry
    9. LOW RISK: Age 0-6 or permanent absence of pregnancy risk - minimal monitoring
    */
    CASE
        -- PRIORITY 1: Pregnancy - immediate clinical review
        WHEN COALESCE(preg_status.is_currently_pregnant, FALSE) = TRUE THEN 1
        
        -- PRIORITY 9: Low risk patients
        WHEN db.age BETWEEN 0 AND 6 OR COALESCE(preg_status.has_permanent_absence_of_pregnancy_risk, FALSE) = TRUE THEN 9
        
        -- PRIORITY 5: PPP non-enrolled (declined/discontinued)
        WHEN COALESCE(ppp.is_ppp_non_enrolled, FALSE) = TRUE THEN 5
        
        -- PRIORITIES 2-4: Missing safety documentation, stratified by vulnerability
        WHEN (COALESCE(ppp.has_ppp_event, FALSE) = FALSE OR COALESCE(araf.has_specific_araf_form_meeting_lookback, FALSE) = FALSE) 
             AND COALESCE(ld.is_on_register, FALSE) = TRUE THEN 2
        WHEN (COALESCE(ppp.has_ppp_event, FALSE) = FALSE OR COALESCE(araf.has_specific_araf_form_meeting_lookback, FALSE) = FALSE) 
             AND db.age BETWEEN 13 AND 60 THEN 3
        WHEN (COALESCE(ppp.has_ppp_event, FALSE) = FALSE OR COALESCE(araf.has_specific_araf_form_meeting_lookback, FALSE) = FALSE) 
             AND db.age BETWEEN 7 AND 12 THEN 4
        
        -- PRIORITIES 6-8: Complete safety documentation, stratified by vulnerability
        WHEN COALESCE(ppp.is_currently_ppp_enrolled, FALSE) = TRUE AND COALESCE(araf.has_specific_araf_form_meeting_lookback, FALSE) = TRUE 
             AND db.age BETWEEN 7 AND 60 AND COALESCE(ld.is_on_register, FALSE) = TRUE THEN 6
        WHEN COALESCE(ppp.is_currently_ppp_enrolled, FALSE) = TRUE AND COALESCE(araf.has_specific_araf_form_meeting_lookback, FALSE) = TRUE 
             AND db.age BETWEEN 7 AND 12 THEN 7
        WHEN COALESCE(ppp.is_currently_ppp_enrolled, FALSE) = TRUE AND COALESCE(araf.has_specific_araf_form_meeting_lookback, FALSE) = TRUE 
             AND db.age BETWEEN 13 AND 60 THEN 8
        
        ELSE NULL
    END AS action_order,
    
    -- Additional findings grouping
    CASE
        WHEN COALESCE(preg_status.is_currently_pregnant, FALSE) = FALSE AND COALESCE(ld.is_on_register, FALSE) = FALSE THEN ''
        WHEN COALESCE(preg_status.is_currently_pregnant, FALSE) = TRUE AND COALESCE(ld.is_on_register, FALSE) = TRUE THEN 'Pregnancy, Learning Disability'
        WHEN COALESCE(preg_status.is_currently_pregnant, FALSE) = TRUE THEN 'Pregnancy'
        WHEN COALESCE(ld.is_on_register, FALSE) = TRUE THEN 'Learning Disability'
        ELSE ''
    END AS additional_findings,
    
    -- Condition group logic
    CASE
        WHEN COALESCE(neu.has_neurology_event, FALSE) = FALSE AND COALESCE(psych.has_psych_event, FALSE) = FALSE THEN ''
        WHEN COALESCE(neu.has_neurology_event, FALSE) = TRUE AND COALESCE(psych.has_psych_event, FALSE) = TRUE THEN 'Neurology, Psychiatry'
        WHEN COALESCE(neu.has_neurology_event, FALSE) = TRUE THEN 'Neurology'
        WHEN COALESCE(psych.has_psych_event, FALSE) = TRUE THEN 'Psychiatry'
        ELSE ''
    END AS condition_group,
    
    CURRENT_DATE() AS calculation_date
    
FROM db_scope AS db
LEFT JOIN ppp_status AS ppp ON db.person_id = ppp.person_id
LEFT JOIN araf AS araf ON db.person_id = araf.person_id
LEFT JOIN araf_referral AS arref ON db.person_id = arref.person_id
LEFT JOIN neurology AS neu ON db.person_id = neu.person_id
LEFT JOIN psychiatry AS psych ON db.person_id = psych.person_id
LEFT JOIN pregnancy_status AS preg_status ON db.person_id = preg_status.person_id
LEFT JOIN learning_disability AS ld ON db.person_id = ld.person_id
