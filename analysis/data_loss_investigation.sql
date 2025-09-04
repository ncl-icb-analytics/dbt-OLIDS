/*
Investigation: Track data loss at each join step
Purpose: Identify where observations are being filtered out in the modern approach
*/

-- Step 1: Count raw observations with BP_COD codes
WITH step1_raw_obs AS (
    SELECT COUNT(DISTINCT o.id) as total_obs
    FROM {{ ref('stg_olids_observation') }} o
    INNER JOIN {{ ref('stg_olids_terminology_concept_map') }} cm 
        ON o.observation_source_concept_id = cm.source_code_id
    INNER JOIN {{ ref('stg_olids_terminology_concept') }} c 
        ON cm.target_code_id = c.id
    INNER JOIN {{ ref('stg_reference_combined_codesets') }} cc 
        ON c.code = cc.code
    WHERE cc.cluster_id = 'BP_COD'
),

-- Step 2: After joining to patient_person
step2_with_patient_person AS (
    SELECT COUNT(DISTINCT o.id) as total_obs
    FROM {{ ref('stg_olids_observation') }} o
    INNER JOIN {{ ref('stg_olids_patient_person') }} pp 
        ON o.patient_id = pp.patient_id
    INNER JOIN {{ ref('stg_olids_terminology_concept_map') }} cm 
        ON o.observation_source_concept_id = cm.source_code_id
    INNER JOIN {{ ref('stg_olids_terminology_concept') }} c 
        ON cm.target_code_id = c.id
    INNER JOIN {{ ref('stg_reference_combined_codesets') }} cc 
        ON c.code = cc.code
    WHERE cc.cluster_id = 'BP_COD'
),

-- Step 3: After joining to person table
step3_with_person AS (
    SELECT COUNT(DISTINCT o.id) as total_obs
    FROM {{ ref('stg_olids_observation') }} o
    INNER JOIN {{ ref('stg_olids_patient_person') }} pp 
        ON o.patient_id = pp.patient_id
    INNER JOIN {{ ref('stg_olids_person') }} p 
        ON pp.person_id = p.id
    INNER JOIN {{ ref('stg_olids_terminology_concept_map') }} cm 
        ON o.observation_source_concept_id = cm.source_code_id
    INNER JOIN {{ ref('stg_olids_terminology_concept') }} c 
        ON cm.target_code_id = c.id
    INNER JOIN {{ ref('stg_reference_combined_codesets') }} cc 
        ON c.code = cc.code
    WHERE cc.cluster_id = 'BP_COD'
),

-- Step 4: After requiring birth year
step4_with_birth_year AS (
    SELECT COUNT(DISTINCT o.id) as total_obs
    FROM {{ ref('stg_olids_observation') }} o
    INNER JOIN {{ ref('stg_olids_patient_person') }} pp 
        ON o.patient_id = pp.patient_id
    INNER JOIN {{ ref('stg_olids_patient') }} pat 
        ON pp.patient_id = pat.id
    INNER JOIN {{ ref('stg_olids_terminology_concept_map') }} cm 
        ON o.observation_source_concept_id = cm.source_code_id
    INNER JOIN {{ ref('stg_olids_terminology_concept') }} c 
        ON cm.target_code_id = c.id
    INNER JOIN {{ ref('stg_reference_combined_codesets') }} cc 
        ON c.code = cc.code
    WHERE cc.cluster_id = 'BP_COD'
        AND pat.birth_year IS NOT NULL
),

-- Step 5: After requiring registration history
step5_with_registration AS (
    SELECT COUNT(DISTINCT o.id) as total_obs
    FROM {{ ref('stg_olids_observation') }} o
    INNER JOIN {{ ref('stg_olids_patient_person') }} pp 
        ON o.patient_id = pp.patient_id
    INNER JOIN {{ ref('stg_olids_patient') }} pat 
        ON pp.patient_id = pat.id
    INNER JOIN (
        SELECT DISTINCT patient_id 
        FROM {{ ref('stg_olids_patient_registered_practitioner_in_role') }}
        WHERE start_date IS NOT NULL
    ) prpr ON pp.patient_id = prpr.patient_id
    INNER JOIN {{ ref('stg_olids_terminology_concept_map') }} cm 
        ON o.observation_source_concept_id = cm.source_code_id
    INNER JOIN {{ ref('stg_olids_terminology_concept') }} c 
        ON cm.target_code_id = c.id
    INNER JOIN {{ ref('stg_reference_combined_codesets') }} cc 
        ON c.code = cc.code
    WHERE cc.cluster_id = 'BP_COD'
        AND pat.birth_year IS NOT NULL
),

-- Step 6: Using dim_person_demographics
step6_with_dim AS (
    SELECT COUNT(DISTINCT o.id) as total_obs
    FROM {{ ref('stg_olids_observation') }} o
    INNER JOIN {{ ref('stg_olids_patient_person') }} pp 
        ON o.patient_id = pp.patient_id
    INNER JOIN {{ ref('dim_person_demographics') }} d 
        ON pp.person_id = d.person_id
    INNER JOIN {{ ref('stg_olids_terminology_concept_map') }} cm 
        ON o.observation_source_concept_id = cm.source_code_id
    INNER JOIN {{ ref('stg_olids_terminology_concept') }} c 
        ON cm.target_code_id = c.id
    INNER JOIN {{ ref('stg_reference_combined_codesets') }} cc 
        ON c.code = cc.code
    WHERE cc.cluster_id = 'BP_COD'
),

-- Compare using int_observations_mapped
step7_modern_approach AS (
    SELECT COUNT(DISTINCT o.observation_id) as total_obs
    FROM {{ ref('int_observations_mapped') }} o
    INNER JOIN {{ ref('dim_person_demographics') }} d 
        ON o.person_id = d.person_id
    INNER JOIN {{ ref('stg_reference_combined_codesets') }} cc 
        ON o.mapped_concept_code = cc.code
    WHERE cc.cluster_id = 'BP_COD'
)

-- Summary of data loss at each step
SELECT 
    'Step 1: Raw observations with BP_COD' as step_description,
    (SELECT total_obs FROM step1_raw_obs) as observation_count,
    0 as observations_lost,
    0.0 as pct_lost
UNION ALL
SELECT 
    'Step 2: After patient_person join',
    (SELECT total_obs FROM step2_with_patient_person),
    (SELECT total_obs FROM step1_raw_obs) - (SELECT total_obs FROM step2_with_patient_person),
    ROUND(100.0 * ((SELECT total_obs FROM step1_raw_obs) - (SELECT total_obs FROM step2_with_patient_person)) / (SELECT total_obs FROM step1_raw_obs), 2)
UNION ALL
SELECT 
    'Step 3: After person join',
    (SELECT total_obs FROM step3_with_person),
    (SELECT total_obs FROM step2_with_patient_person) - (SELECT total_obs FROM step3_with_person),
    ROUND(100.0 * ((SELECT total_obs FROM step2_with_patient_person) - (SELECT total_obs FROM step3_with_person)) / (SELECT total_obs FROM step2_with_patient_person), 2)
UNION ALL
SELECT 
    'Step 4: After requiring birth year',
    (SELECT total_obs FROM step4_with_birth_year),
    (SELECT total_obs FROM step3_with_person) - (SELECT total_obs FROM step4_with_birth_year),
    ROUND(100.0 * ((SELECT total_obs FROM step3_with_person) - (SELECT total_obs FROM step4_with_birth_year)) / (SELECT total_obs FROM step3_with_person), 2)
UNION ALL
SELECT 
    'Step 5: After requiring registration',
    (SELECT total_obs FROM step5_with_registration),
    (SELECT total_obs FROM step4_with_birth_year) - (SELECT total_obs FROM step5_with_registration),
    ROUND(100.0 * ((SELECT total_obs FROM step4_with_birth_year) - (SELECT total_obs FROM step5_with_registration)) / (SELECT total_obs FROM step4_with_birth_year), 2)
UNION ALL
SELECT 
    'Step 6: Using dim_person_demographics',
    (SELECT total_obs FROM step6_with_dim),
    (SELECT total_obs FROM step5_with_registration) - (SELECT total_obs FROM step6_with_dim),
    ROUND(100.0 * ((SELECT total_obs FROM step5_with_registration) - (SELECT total_obs FROM step6_with_dim)) / (SELECT total_obs FROM step5_with_registration), 2)
UNION ALL
SELECT 
    'Step 7: Modern approach (int_observations_mapped)',
    (SELECT total_obs FROM step7_modern_approach),
    (SELECT total_obs FROM step6_with_dim) - (SELECT total_obs FROM step7_modern_approach),
    ROUND(100.0 * ((SELECT total_obs FROM step6_with_dim) - (SELECT total_obs FROM step7_modern_approach)) / (SELECT total_obs FROM step6_with_dim), 2)
ORDER BY 1;