{{
    config(
        materialized='table',
        indexes=[
            {'columns': ['person_id'], 'unique': false},
            {'columns': ['person_id', 'is_overall_bp_controlled'], 'unique': false}
        ]
    )
}}

WITH latest_bp AS (
    -- Get most recent blood pressure reading for each person
    SELECT 
        person_id, 
        clinical_effective_date,
        systolic_value,
        diastolic_value
    FROM {{ ref('int_blood_pressure_latest') }}
),

patient_characteristics AS (
    -- Gather key patient characteristics for BP threshold determination
    SELECT
        bp.person_id,
        age.sk_patient_id,
        bp.clinical_effective_date AS latest_bp_date,
        bp.systolic_value AS latest_systolic_value,
        bp.diastolic_value AS latest_diastolic_value,
        age.age,
        
        -- Diabetes status (Type 2 specifically)
        COALESCE(dm.is_on_diabetes_register, FALSE) AS is_on_dm_register,
        dm.diabetes_type,
        
        -- CKD status and latest ACR
        CASE WHEN ckd.person_id IS NOT NULL THEN TRUE ELSE FALSE END AS has_ckd,
        ckd.latest_acr_value,
        
        -- Hypertension diagnosis status
        COALESCE(htn.is_on_hypertension_register, FALSE) AS is_diagnosed_htn
        
    FROM latest_bp bp
    JOIN {{ ref('dim_person_age') }} age ON bp.person_id = age.person_id
    LEFT JOIN {{ ref('fct_person_diabetes_register') }} dm ON bp.person_id = dm.person_id
    LEFT JOIN {{ ref('fct_person_ckd_register') }} ckd ON bp.person_id = ckd.person_id
    LEFT JOIN {{ ref('fct_person_hypertension_register') }} htn ON bp.person_id = htn.person_id
),

ranked_thresholds AS (
    -- Apply BP thresholds based on patient characteristics with priority ranking
    SELECT
        pc.*,
        thr.threshold_rule_id,
        thr.patient_group,
        thr.systolic_threshold,
        thr.diastolic_threshold,
        
        -- Priority ranking (lowest number = highest priority)
        CASE thr.patient_group
            WHEN 'CKD_ACR_GE_70' THEN 1  -- Most stringent
            WHEN 'T2DM' THEN 2
            WHEN 'CKD' THEN 3
            WHEN 'AGE_GE_80' THEN 4
            WHEN 'AGE_LT_80' THEN 5      -- Default/least stringent
            ELSE 99
        END AS priority_rank
        
    FROM patient_characteristics pc
    JOIN {{ ref('stg_rulesets_bp_thresholds') }} thr
        ON (
            -- Age-based thresholds
            (thr.patient_group = 'AGE_LT_80' AND pc.age < 80) OR
            (thr.patient_group = 'AGE_GE_80' AND pc.age >= 80) OR
            
            -- T2DM patients under 80
            (thr.patient_group = 'T2DM' AND pc.is_on_dm_register AND pc.diabetes_type = 'Type 2' AND pc.age < 80) OR
            
            -- CKD patients under 80 (general)
            (thr.patient_group = 'CKD' AND pc.has_ckd AND pc.age < 80) OR
            
            -- CKD patients under 80 with high ACR (≥70)
            (thr.patient_group = 'CKD_ACR_GE_70' AND pc.has_ckd AND pc.latest_acr_value >= 70 AND pc.age < 80)
        )
    WHERE thr.threshold_type = 'TARGET_UPPER' 
        AND thr.operator = 'BELOW'
    QUALIFY ROW_NUMBER() OVER (PARTITION BY pc.person_id ORDER BY priority_rank ASC) = 1
)

SELECT
    rt.person_id,
    rt.sk_patient_id,
    
    -- Latest BP reading details
    rt.latest_bp_date,
    rt.latest_systolic_value,
    rt.latest_diastolic_value,
    
    -- Patient characteristics
    rt.age,
    (rt.is_on_dm_register AND rt.diabetes_type = 'Type 2') AS has_t2dm,
    rt.has_ckd,
    rt.is_diagnosed_htn,
    rt.latest_acr_value,
    
    -- Applied threshold details
    rt.threshold_rule_id AS applied_threshold_rule_id,
    rt.patient_group AS applied_patient_group,
    rt.systolic_threshold AS applied_systolic_threshold,
    rt.diastolic_threshold AS applied_diastolic_threshold,
    
    -- BP control status calculations
    CASE 
        WHEN rt.latest_systolic_value IS NOT NULL AND rt.latest_systolic_value < rt.systolic_threshold 
        THEN TRUE 
        ELSE FALSE 
    END AS is_systolic_controlled,
    
    CASE 
        WHEN rt.latest_diastolic_value IS NOT NULL AND rt.latest_diastolic_value < rt.diastolic_threshold 
        THEN TRUE 
        ELSE FALSE 
    END AS is_diastolic_controlled,
    
    -- Overall control (both systolic AND diastolic controlled)
    CASE 
        WHEN (rt.latest_systolic_value IS NOT NULL AND rt.latest_systolic_value < rt.systolic_threshold)
             AND (rt.latest_diastolic_value IS NOT NULL AND rt.latest_diastolic_value < rt.diastolic_threshold)
        THEN TRUE 
        ELSE FALSE 
    END AS is_overall_bp_controlled,
    
    -- BP reading timeliness assessment
    DATEDIFF(month, rt.latest_bp_date, CURRENT_DATE()) AS latest_bp_reading_age_months,
    
    -- Recommended interval assessment based on risk factors
    CASE
        -- Tier 1: High risk (T2DM OR CKD OR diagnosed HTN) - check within 12 months
        WHEN ((rt.is_on_dm_register AND rt.diabetes_type = 'Type 2') OR rt.has_ckd OR rt.is_diagnosed_htn)
            THEN CASE WHEN DATEDIFF(month, rt.latest_bp_date, CURRENT_DATE()) <= 12 THEN TRUE ELSE FALSE END
            
        -- Tier 2: Medium risk (age ≥40, no high-risk conditions) - check within 24 months  
        WHEN (NOT (rt.is_on_dm_register AND rt.diabetes_type = 'Type 2') AND NOT rt.has_ckd AND NOT rt.is_diagnosed_htn AND rt.age >= 40)
            THEN CASE WHEN DATEDIFF(month, rt.latest_bp_date, CURRENT_DATE()) <= 24 THEN TRUE ELSE FALSE END
            
        -- Tier 3: Low risk (age <40, no high-risk conditions) - check within 60 months
        WHEN (NOT (rt.is_on_dm_register AND rt.diabetes_type = 'Type 2') AND NOT rt.has_ckd AND NOT rt.is_diagnosed_htn AND rt.age < 40)
            THEN CASE WHEN DATEDIFF(month, rt.latest_bp_date, CURRENT_DATE()) <= 60 THEN TRUE ELSE FALSE END
            
        ELSE FALSE
    END AS is_latest_bp_within_recommended_interval

FROM ranked_thresholds rt

ORDER BY rt.person_id 