{{
    config(
        materialized='table',
        tags=['intermediate', 'observations', 'fractures'],
        cluster_by=['person_id', 'clinical_effective_date']
    )
}}

-- Intermediate Fragility Fractures Model
-- Collects all fragility fracture observations using QOF cluster FF_COD
-- Only includes fractures after April 2012 as per QOF guidelines
-- Provides foundation data for osteoporosis register

WITH fracture_observations AS (
    -- Get all fragility fracture observations using our macro
    SELECT 
        person_id,
        patient_id,
        observation_id,
        clinical_effective_date,
        source_cluster_id,
        concept_code,
        code_description,
        numeric_value,
        -- Extract fracture site from code description
        CASE 
            WHEN LOWER(code_description) LIKE '%hip%' THEN 'Hip'
            WHEN LOWER(code_description) LIKE '%wrist%' OR LOWER(code_description) LIKE '%radius%' THEN 'Wrist'
            WHEN LOWER(code_description) LIKE '%spine%' OR LOWER(code_description) LIKE '%vertebra%' THEN 'Spine'
            WHEN LOWER(code_description) LIKE '%humerus%' OR LOWER(code_description) LIKE '%shoulder%' THEN 'Humerus'
            WHEN LOWER(code_description) LIKE '%pelvis%' THEN 'Pelvis'
            WHEN LOWER(code_description) LIKE '%femur%' THEN 'Femur'
            ELSE 'Other'
        END AS fracture_site,
        -- Clinical flags
        source_cluster_id = 'FF_COD' AS is_fragility_fracture_code
    FROM ({{ get_observations("'FF_COD'") }}) obs
    -- Only include fractures after April 2012 as per QOF requirements
    WHERE clinical_effective_date >= '2012-04-01'
),

person_level_aggregates AS (
    -- Aggregate fracture data at person level
    SELECT 
        person_id,
        COUNT(*) AS total_fracture_observations,
        MIN(clinical_effective_date) AS earliest_fracture_date,
        MAX(clinical_effective_date) AS latest_fracture_date,
        COUNT(DISTINCT fracture_site) AS distinct_fracture_sites,
        COUNT(DISTINCT clinical_effective_date) AS distinct_fracture_dates,
        -- Aggregate arrays for comprehensive tracking
        ARRAY_AGG(DISTINCT concept_code) AS all_fracture_concept_codes,
        ARRAY_AGG(DISTINCT code_description) AS all_fracture_concept_displays,
        ARRAY_AGG(DISTINCT fracture_site) AS all_fracture_sites,
        -- Fracture site flags
        MAX(CASE WHEN fracture_site = 'Hip' THEN 1 ELSE 0 END) = 1 AS has_hip_fracture,
        MAX(CASE WHEN fracture_site = 'Wrist' THEN 1 ELSE 0 END) = 1 AS has_wrist_fracture,
        MAX(CASE WHEN fracture_site = 'Spine' THEN 1 ELSE 0 END) = 1 AS has_spine_fracture,
        MAX(CASE WHEN fracture_site = 'Humerus' THEN 1 ELSE 0 END) = 1 AS has_humerus_fracture
    FROM fracture_observations
    GROUP BY person_id
)

-- Final output: one row per observation with person-level enrichment
SELECT 
    fo.person_id,
    fo.patient_id,
    fo.observation_id,
    fo.clinical_effective_date,
    fo.cluster_id AS source_cluster_id,
    fo.mapped_concept_code AS concept_code,
    fo.code_description,
    fo.fracture_site,
    fo.is_fragility_fracture_code,
    fo.numeric_value,
    -- Person-level aggregates
    pla.total_fracture_observations,
    pla.earliest_fracture_date,
    pla.latest_fracture_date,
    pla.distinct_fracture_sites,
    pla.distinct_fracture_dates,
    pla.all_fracture_concept_codes,
    pla.all_fracture_concept_displays,
    pla.all_fracture_sites,
    -- Fracture site flags
    pla.has_hip_fracture,
    pla.has_wrist_fracture,
    pla.has_spine_fracture,
    pla.has_humerus_fracture,
    -- Timeline context
    DATEDIFF(month, pla.earliest_fracture_date, fo.clinical_effective_date) AS months_since_first_fracture,
    fo.clinical_effective_date = pla.earliest_fracture_date AS is_earliest_fracture,
    fo.clinical_effective_date = pla.latest_fracture_date AS is_latest_fracture
FROM fracture_observations fo
LEFT JOIN person_level_aggregates pla
    ON fo.person_id = pla.person_id 