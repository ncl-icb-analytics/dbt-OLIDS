{{
    config(
        materialized='table',
        indexes=[
            {'columns': ['person_id'], 'unique': false},
            {'columns': ['is_on_copd_register'], 'unique': false},
            {'columns': ['is_pre_april_2023_diagnosis'], 'unique': false},
            {'columns': ['is_post_april_2023_diagnosis'], 'unique': false}
        ]
    )
}}

/*
COPD Register - QOF Respiratory Quality Measures
Tracks all patients with chronic obstructive pulmonary disease diagnoses.
Applies QOF-specific business rules including spirometry confirmation requirements.

QOF Business Rules:
1. Must have an unresolved diagnosis
2. Pre-April 2023: COPD diagnosis alone sufficient for register  
3. Post-April 2023: Requires spirometry confirmation (FEV1/FVC <0.7) OR unable-to-have-spirometry status

Matches legacy fct_person_dx_copd business logic and field structure exactly.
*/

WITH base_diagnoses AS (
    -- Get all COPD diagnoses - matches legacy BaseDiagnoses CTE
    SELECT 
        d.person_id,
        age.sk_patient_id,
        age.age,
        
        -- Person-level aggregation from observation-level data
        MIN(CASE WHEN d.is_copd_diagnosis_code THEN d.clinical_effective_date END) AS earliest_diagnosis_date,
        MAX(CASE WHEN d.is_copd_diagnosis_code THEN d.clinical_effective_date END) AS latest_diagnosis_date,
        MAX(CASE WHEN d.is_copd_resolved_code THEN d.clinical_effective_date END) AS latest_resolution_date,
        
        -- Calculate earliest unresolved diagnosis date (key for QOF logic)
        MIN(CASE WHEN d.is_copd_diagnosis_code THEN d.clinical_effective_date END) AS earliest_unresolved_diagnosis_date
        
    FROM {{ ref('int_copd_diagnoses_all') }} d
    JOIN {{ ref('dim_person_age') }} age
        ON d.person_id = age.person_id
    GROUP BY 
        d.person_id,
        age.sk_patient_id,
        age.age
),

latest_spirometry AS (
    -- Get latest spirometry results for each person - matches legacy LatestSpirometry CTE
    SELECT
        person_id,
        MAX(clinical_effective_date) AS latest_spirometry_date,
        MAX(CASE WHEN is_below_0_7 THEN fev1_fvc_ratio ELSE NULL END) AS latest_spirometry_fev1_fvc_ratio,
        MAX(CASE WHEN is_below_0_7 THEN 1 ELSE 0 END) = 1 AS has_spirometry_confirmation
    FROM {{ ref('int_spirometry_all') }}
    GROUP BY person_id
),

unable_spirometry AS (
    -- Get latest unable-to-have-spirometry status for each person - matches legacy UnableSpirometry CTE
    SELECT
        person_id,
        MAX(clinical_effective_date) AS latest_unable_spirometry_date,
        COUNT(*) > 0 AS is_unable_spirometry
    FROM {{ ref('int_unable_spirometry_all') }}
    GROUP BY person_id
),

person_level_coding_aggregation AS (
    -- Aggregate all COPD concept codes and displays into arrays - matches legacy PersonLevelCodingAggregation CTE
    SELECT
        person_id,
        ARRAY_AGG(DISTINCT concept_code) AS all_copd_concept_codes,
        ARRAY_AGG(DISTINCT concept_display) AS all_copd_concept_displays
    FROM {{ ref('int_copd_diagnoses_all') }}
    WHERE source_cluster_id = 'COPD_COD'
    GROUP BY person_id
)

-- Final selection implementing business rules - matches legacy exactly
SELECT
    f.person_id,
    f.sk_patient_id,
    f.age,
    
    -- Business rules for register inclusion - matches legacy logic exactly:
    -- 1. Must have an unresolved diagnosis
    -- 2. For pre-April 2023: Just need the diagnosis
    -- 3. For post-April 2023: Need either spirometry confirmation or unable-to-have-spirometry status
    CASE
        WHEN f.earliest_unresolved_diagnosis_date IS NULL THEN FALSE
        WHEN f.earliest_unresolved_diagnosis_date < '2023-04-01' THEN TRUE
        WHEN f.earliest_unresolved_diagnosis_date >= '2023-04-01' 
            AND (COALESCE(s.has_spirometry_confirmation, FALSE) 
                 OR COALESCE(u.is_unable_spirometry, FALSE)) THEN TRUE
        ELSE FALSE
    END AS is_on_copd_register,
    
    f.earliest_diagnosis_date,
    f.latest_diagnosis_date,
    f.latest_resolution_date,
    f.earliest_unresolved_diagnosis_date,
    
    COALESCE(s.has_spirometry_confirmation, FALSE) AS has_spirometry_confirmation,
    s.latest_spirometry_date,
    s.latest_spirometry_fev1_fvc_ratio,
    
    COALESCE(u.is_unable_spirometry, FALSE) AS is_unable_spirometry,
    u.latest_unable_spirometry_date,
    
    -- QOF temporal flags - matches legacy exactly
    CASE WHEN f.earliest_unresolved_diagnosis_date < '2023-04-01' THEN TRUE ELSE FALSE END AS is_pre_april_2023_diagnosis,
    CASE WHEN f.earliest_unresolved_diagnosis_date >= '2023-04-01' THEN TRUE ELSE FALSE END AS is_post_april_2023_diagnosis,
    
    c.all_copd_concept_codes,
    c.all_copd_concept_displays

FROM base_diagnoses f
LEFT JOIN latest_spirometry s
    ON f.person_id = s.person_id
LEFT JOIN unable_spirometry u
    ON f.person_id = u.person_id
LEFT JOIN person_level_coding_aggregation c
    ON f.person_id = c.person_id

-- Sort for consistent output
ORDER BY person_id 