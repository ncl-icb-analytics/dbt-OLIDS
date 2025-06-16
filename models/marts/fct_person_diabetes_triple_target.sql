{{
    config(
        materialized='table',
        indexes=[
            {'columns': ['person_id'], 'unique': false},
            {'columns': ['person_id', 'all_three_targets_met'], 'unique': false}
        ]
    )
}}

WITH twelve_months_ago AS (
    SELECT DATEADD(month, -12, CURRENT_DATE()) AS twelve_months_ago
),

diabetes_register AS (
    -- Base population: people on diabetes register
    SELECT 
        person_id,
        sk_patient_id,
        diabetes_type
    FROM {{ ref('int_diabetes_diagnoses_all') }}
    WHERE earliest_diabetes_date IS NOT NULL
)

SELECT
    dr.person_id,
    dr.sk_patient_id,
    dr.diabetes_type,
    
    -- HbA1c data
    hba.clinical_effective_date AS latest_hba1c_date,
    hba.result_value AS latest_hba1c_value,
    CASE
        WHEN hba.is_ifcc THEN 'IFCC'
        WHEN hba.is_dcct THEN 'DCCT'
        ELSE NULL
    END AS hba1c_type,
    
    -- Blood pressure data
    bp.clinical_effective_date AS latest_bp_date,
    bp.systolic_value AS latest_systolic,
    bp.diastolic_value AS latest_diastolic,
    
    -- Cholesterol data
    chol.clinical_effective_date AS latest_chol_date,
    chol.result_value AS latest_chol_value,
    
    -- Target achievement flags
    (
        hba.result_value IS NOT NULL AND (
            (hba.is_ifcc AND hba.result_value < 58)
            OR (hba.is_dcct AND hba.result_value < 7.5)
        )
    ) AS hba1c_in_target_range,
    
    (bp.systolic_value IS NOT NULL AND bp.systolic_value < 140 
     AND bp.diastolic_value IS NOT NULL AND bp.diastolic_value < 80) AS bp_in_target_range,
    
    (chol.result_value IS NOT NULL AND chol.result_value < 5) AS cholesterol_in_target_range,
    
    -- Overall target achievement
    (
        (
            hba.result_value IS NOT NULL AND (
                (hba.is_ifcc AND hba.result_value < 58)
                OR (hba.is_dcct AND hba.result_value < 7.5)
            )
        )
        AND (bp.systolic_value IS NOT NULL AND bp.systolic_value < 140 
             AND bp.diastolic_value IS NOT NULL AND bp.diastolic_value < 80)
        AND (chol.result_value IS NOT NULL AND chol.result_value < 5)
    ) AS all_three_targets_met,
    
    -- Recency flags (within last 12 months)
    (hba.clinical_effective_date IS NOT NULL 
     AND hba.clinical_effective_date >= t.twelve_months_ago) AS hba1c_measured_in_last_12m,
    
    (bp.clinical_effective_date IS NOT NULL 
     AND bp.clinical_effective_date >= t.twelve_months_ago) AS bp_measured_in_last_12m,
    
    (chol.clinical_effective_date IS NOT NULL 
     AND chol.clinical_effective_date >= t.twelve_months_ago) AS cholesterol_measured_in_last_12m,
    
    -- Recent but out of range flags
    (hba.clinical_effective_date IS NOT NULL 
     AND hba.clinical_effective_date >= t.twelve_months_ago
     AND NOT (
         hba.result_value IS NOT NULL AND (
             (hba.is_ifcc AND hba.result_value < 58)
             OR (hba.is_dcct AND hba.result_value < 7.5)
         )
     )) AS hba1c_recent_but_out_of_range,
    
    (bp.clinical_effective_date IS NOT NULL 
     AND bp.clinical_effective_date >= t.twelve_months_ago
     AND NOT (bp.systolic_value IS NOT NULL AND bp.systolic_value < 140 
              AND bp.diastolic_value IS NOT NULL AND bp.diastolic_value < 80)) AS bp_recent_but_out_of_range,
    
    (chol.clinical_effective_date IS NOT NULL 
     AND chol.clinical_effective_date >= t.twelve_months_ago
     AND NOT (chol.result_value IS NOT NULL AND chol.result_value < 5)) AS cholesterol_recent_but_out_of_range

FROM diabetes_register dr
CROSS JOIN twelve_months_ago t
LEFT JOIN {{ ref('int_hba1c_latest') }} hba
    ON dr.person_id = hba.person_id
LEFT JOIN {{ ref('int_blood_pressure_latest') }} bp
    ON dr.person_id = bp.person_id
LEFT JOIN {{ ref('int_cholesterol_latest') }} chol
    ON dr.person_id = chol.person_id

ORDER BY dr.person_id