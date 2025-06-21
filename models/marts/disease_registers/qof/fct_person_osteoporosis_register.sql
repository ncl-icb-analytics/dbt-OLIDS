{{
    config(
        materialized='table',
        cluster_by=['person_id'],
        pre_hook="DROP TABLE IF EXISTS {{ this }}"
    )
}}

/*
Osteoporosis Register fact table - one row per person.
Applies QOF osteoporosis register inclusion criteria.

Clinical Purpose:
- QOF osteoporosis register for fracture prevention
- Bone health monitoring
- DXA scanning compliance

QOF Register Criteria (Complex Pattern):
- Age 50-74 years
- AND ALL of the following:
  1. Fragility fracture after April 2012
  2. Osteoporosis diagnosis (OSTEO_COD)
  3. DXA confirmation (DXA scan OR T-score ≤ -2.5)

Includes only active patients as per QOF population requirements.
This table provides one row per person for analytical use.
*/

WITH osteoporosis_diagnoses AS (
    SELECT
        person_id,
        
        -- Register inclusion dates  
        MIN(CASE WHEN is_osteoporosis_diagnosis_code THEN clinical_effective_date END) AS earliest_diagnosis_date,
        MAX(CASE WHEN is_osteoporosis_diagnosis_code THEN clinical_effective_date END) AS latest_diagnosis_date,
        
        -- Episode counts
        COUNT(CASE WHEN is_osteoporosis_diagnosis_code THEN 1 END) AS total_osteoporosis_episodes,
        
        -- Concept code arrays for traceability
        ARRAY_AGG(DISTINCT CASE WHEN is_osteoporosis_diagnosis_code THEN concept_code END) 
            AS osteoporosis_diagnosis_codes,
        ARRAY_AGG(DISTINCT CASE WHEN is_osteoporosis_diagnosis_code THEN concept_display END) 
            AS osteoporosis_diagnosis_displays,
        
        -- Latest observation details
        ARRAY_AGG(DISTINCT observation_id) AS all_observation_ids
            
    FROM {{ ref('int_osteoporosis_diagnoses_all') }}
    GROUP BY person_id
),

dxa_data AS (
    SELECT
        person_id,
        COUNT(CASE WHEN is_dxa_scan_procedure THEN 1 END) > 0 AS has_dxa_scan,
        COUNT(CASE WHEN is_dxa_t_score_measurement THEN 1 END) > 0 AS has_dxa_t_score,
        MIN(CASE WHEN is_dxa_scan_procedure THEN clinical_effective_date END) AS earliest_dxa_date,
        MAX(CASE WHEN is_dxa_scan_procedure THEN clinical_effective_date END) AS latest_dxa_date,
        MIN(CASE WHEN is_dxa_t_score_measurement THEN clinical_effective_date END) AS earliest_dxa_t_score_date,
        MAX(CASE WHEN is_dxa_t_score_measurement THEN clinical_effective_date END) AS latest_dxa_t_score_date,
        MAX(validated_t_score) AS latest_dxa_t_score,
        ARRAY_AGG(DISTINCT CASE WHEN is_dxa_scan_procedure THEN concept_code ELSE NULL END) AS all_dxa_concept_codes,
        ARRAY_AGG(DISTINCT CASE WHEN is_dxa_scan_procedure THEN concept_display ELSE NULL END) AS all_dxa_concept_displays
    FROM {{ ref('int_dxa_scans_all') }}
    GROUP BY person_id
),

fragility_fractures AS (
    SELECT
        person_id,
        COUNT(*) > 0 AS has_fragility_fracture,
        MIN(clinical_effective_date) AS earliest_fragility_fracture_date,
        MAX(clinical_effective_date) AS latest_fragility_fracture_date,
        COUNT(DISTINCT fracture_site) AS distinct_fracture_sites,
        COUNT(DISTINCT clinical_effective_date) AS distinct_fracture_dates,
        
        -- Aggregate arrays for comprehensive tracking
        ARRAY_AGG(DISTINCT concept_code) AS all_fragility_fracture_concept_codes,
        ARRAY_AGG(DISTINCT code_description) AS all_fragility_fracture_concept_displays,
        ARRAY_AGG(DISTINCT fracture_site) AS all_fracture_sites,
        
        -- Fracture site flags
        MAX(CASE WHEN fracture_site = 'Hip' THEN 1 ELSE 0 END) = 1 AS has_hip_fracture,
        MAX(CASE WHEN fracture_site = 'Wrist' THEN 1 ELSE 0 END) = 1 AS has_wrist_fracture,
        MAX(CASE WHEN fracture_site = 'Spine' THEN 1 ELSE 0 END) = 1 AS has_spine_fracture,
        MAX(CASE WHEN fracture_site = 'Humerus' THEN 1 ELSE 0 END) = 1 AS has_humerus_fracture
        
    FROM {{ ref('int_fragility_fractures_all') }}
    GROUP BY person_id
),

register_logic AS (
    SELECT
        p.person_id,
        
        -- Age restriction: 50-74 years for osteoporosis register
        CASE WHEN age.age BETWEEN 50 AND 74 THEN TRUE ELSE FALSE END AS meets_age_criteria,
        
        -- Component 1: Fragility fracture requirement
        COALESCE(frac.has_fragility_fracture, FALSE) AS has_fragility_fracture,
        
        -- Component 2: Osteoporosis diagnosis requirement
        CASE WHEN diag.earliest_diagnosis_date IS NOT NULL THEN TRUE ELSE FALSE END AS has_osteoporosis_diagnosis,
        
        -- Component 3: DXA confirmation requirement (scan OR T-score ≤ -2.5)
        CASE
            WHEN dxa.has_dxa_scan = TRUE THEN TRUE
            WHEN dxa.has_dxa_t_score = TRUE AND dxa.latest_dxa_t_score <= -2.5 THEN TRUE
            ELSE FALSE
        END AS has_valid_dxa_confirmation,
        
        -- Additional component flags for transparency
        COALESCE(dxa.has_dxa_scan, FALSE) AS has_dxa_scan,
        COALESCE(dxa.has_dxa_t_score, FALSE) AS has_dxa_t_score,
        
        -- Complex register inclusion: Age + ALL three components required
        CASE
            WHEN age.age BETWEEN 50 AND 74
                AND frac.has_fragility_fracture = TRUE
                AND diag.earliest_diagnosis_date IS NOT NULL
                AND (
                    dxa.has_dxa_scan = TRUE OR 
                    (dxa.has_dxa_t_score = TRUE AND dxa.latest_dxa_t_score <= -2.5)
                )
            THEN TRUE
            ELSE FALSE
        END AS is_on_register,
        
        -- Clinical dates - osteoporosis
        diag.earliest_diagnosis_date,
        diag.latest_diagnosis_date,
        diag.total_osteoporosis_episodes,
        
        -- Clinical dates - DXA
        dxa.earliest_dxa_date,
        dxa.latest_dxa_date,
        dxa.earliest_dxa_t_score_date,
        dxa.latest_dxa_t_score_date,
        dxa.latest_dxa_t_score,
        
        -- Clinical dates - fractures
        frac.earliest_fragility_fracture_date,
        frac.latest_fragility_fracture_date,
        
        -- Traceability - osteoporosis
        diag.osteoporosis_diagnosis_codes,
        diag.osteoporosis_diagnosis_displays,
        diag.all_observation_ids,
        
        -- Traceability - DXA
        dxa.all_dxa_concept_codes,
        dxa.all_dxa_concept_displays,
        
        -- Traceability - fractures
        frac.all_fragility_fracture_concept_codes,
        frac.all_fragility_fracture_concept_displays,
        frac.all_fracture_sites,
        
        -- Person demographics
        age.age
    FROM {{ ref('dim_person_active_patients') }} p
    INNER JOIN {{ ref('dim_person_age') }} age ON p.person_id = age.person_id
    LEFT JOIN osteoporosis_diagnoses diag ON p.person_id = diag.person_id
    LEFT JOIN dxa_data dxa ON p.person_id = dxa.person_id
    LEFT JOIN fragility_fractures frac ON p.person_id = frac.person_id
)

-- Final selection: Only individuals meeting all osteoporosis register criteria
SELECT
    person_id,
    age,
    is_on_register,
    
    -- Component criteria flags
    meets_age_criteria,
    has_fragility_fracture,
    has_osteoporosis_diagnosis,
    has_valid_dxa_confirmation,
    has_dxa_scan,
    has_dxa_t_score,
    
    -- Clinical dates - osteoporosis
    earliest_diagnosis_date,
    latest_diagnosis_date,
    total_osteoporosis_episodes,
    
    -- Clinical dates - DXA
    earliest_dxa_date,
    latest_dxa_date,
    earliest_dxa_t_score_date,
    latest_dxa_t_score_date,
    latest_dxa_t_score,
    
    -- Clinical dates - fractures
    earliest_fragility_fracture_date,
    latest_fragility_fracture_date,
    
    -- Traceability for audit
    osteoporosis_diagnosis_codes,
    osteoporosis_diagnosis_displays,
    all_observation_ids,
    all_dxa_concept_codes,
    all_dxa_concept_displays,
    all_fragility_fracture_concept_codes,
    all_fragility_fracture_concept_displays,
    all_fracture_sites
FROM register_logic
WHERE is_on_register = TRUE

ORDER BY earliest_diagnosis_date DESC, person_id 