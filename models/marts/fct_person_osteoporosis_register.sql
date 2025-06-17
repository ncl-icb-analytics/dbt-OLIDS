{{
    config(
        materialized='table',
        cluster_by=['person_id']
    )
}}

-- Osteoporosis Register (QOF Pattern 6: Complex Clinical Logic)
-- Business Logic: Age 50-74 + ALL of the following:
--   1. Fragility fracture after April 2012
--   2. Osteoporosis diagnosis  
--   3. DXA confirmation (DXA scan OR T-score ≤ -2.5)
-- Complex Logic: Multiple mandatory components with specific clinical thresholds

WITH osteoporosis_diagnoses AS (
    SELECT
        person_id,
        is_osteoporosis_diagnosis AS has_osteoporosis_diagnosis,
        earliest_osteoporosis_date,
        latest_osteoporosis_date,
        all_osteoporosis_concept_codes,
        all_osteoporosis_concept_displays
    FROM {{ ref('int_osteoporosis_diagnoses_all') }}
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
        earliest_fracture_date AS earliest_fragility_fracture_date,
        latest_fracture_date AS latest_fragility_fracture_date,
        all_fracture_concept_codes AS all_fragility_fracture_concept_codes,
        all_fracture_concept_displays AS all_fragility_fracture_concept_displays,
        all_fracture_sites
    FROM {{ ref('int_fragility_fractures_all') }}
    GROUP BY person_id, earliest_fracture_date, latest_fracture_date, all_fracture_concept_codes, all_fracture_concept_displays, all_fracture_sites
),

register_logic AS (
    SELECT
        p.person_id,
        
        -- Age restriction: 50-74 years for osteoporosis register
        CASE WHEN age.age BETWEEN 50 AND 74 THEN TRUE ELSE FALSE END AS meets_age_criteria,
        
        -- Component 1: Fragility fracture requirement
        COALESCE(frac.has_fragility_fracture, FALSE) AS has_fragility_fracture,
        
        -- Component 2: Osteoporosis diagnosis requirement
        COALESCE(diag.has_osteoporosis_diagnosis, FALSE) AS has_osteoporosis_diagnosis,
        
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
                AND diag.has_osteoporosis_diagnosis = TRUE
                AND (
                    dxa.has_dxa_scan = TRUE OR 
                    (dxa.has_dxa_t_score = TRUE AND dxa.latest_dxa_t_score <= -2.5)
                )
            THEN TRUE
            ELSE FALSE
        END AS is_on_osteoporosis_register,
        
        -- Clinical dates - osteoporosis
        diag.earliest_osteoporosis_date,
        diag.latest_osteoporosis_date,
        
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
        diag.all_osteoporosis_concept_codes,
        diag.all_osteoporosis_concept_displays,
        
        -- Traceability - DXA
        dxa.all_dxa_concept_codes,
        dxa.all_dxa_concept_displays,
        
        -- Traceability - fractures
        frac.all_fragility_fracture_concept_codes,
        frac.all_fragility_fracture_concept_displays,
        frac.all_fracture_sites,
        
        -- Person demographics
        age.age
    FROM {{ ref('dim_person') }} p
    INNER JOIN {{ ref('dim_person_age') }} age ON p.person_id = age.person_id
    LEFT JOIN osteoporosis_diagnoses diag ON p.person_id = diag.person_id
    LEFT JOIN dxa_data dxa ON p.person_id = dxa.person_id
    LEFT JOIN fragility_fractures frac ON p.person_id = frac.person_id
)

-- Final selection: Only individuals meeting all osteoporosis register criteria
SELECT
    person_id,
    age,
    is_on_osteoporosis_register,
    
    -- Component criteria flags
    meets_age_criteria,
    has_fragility_fracture,
    has_osteoporosis_diagnosis,
    has_valid_dxa_confirmation,
    has_dxa_scan,
    has_dxa_t_score,
    
    -- Clinical dates - osteoporosis
    earliest_osteoporosis_date,
    latest_osteoporosis_date,
    
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
    all_osteoporosis_concept_codes,
    all_osteoporosis_concept_displays,
    all_dxa_concept_codes,
    all_dxa_concept_displays,
    all_fragility_fracture_concept_codes,
    all_fragility_fracture_concept_displays,
    all_fracture_sites
FROM register_logic
WHERE is_on_osteoporosis_register = TRUE 