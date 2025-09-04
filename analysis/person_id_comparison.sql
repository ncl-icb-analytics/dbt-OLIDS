/*
Analysis: Compare person_id availability and mapping between approaches
Purpose: Identify if person_id lookup differences are causing data loss
*/

-- Check how many observations have person_id populated vs need patient_person lookup
WITH observation_person_coverage AS (
    SELECT 
        'Direct person_id in observation' as source_type,
        COUNT(DISTINCT o.id) as observation_count,
        COUNT(DISTINCT o.person_id) as persons_with_obs,
        COUNT(DISTINCT CASE WHEN o.person_id IS NOT NULL THEN o.id END) as obs_with_person,
        COUNT(DISTINCT CASE WHEN o.person_id IS NULL THEN o.id END) as obs_without_person
    FROM {{ ref('stg_olids_observation') }} o
    INNER JOIN {{ ref('stg_olids_terminology_concept_map') }} cm 
        ON o.observation_source_concept_id = cm.source_code_id
    INNER JOIN {{ ref('stg_olids_terminology_concept') }} c 
        ON cm.target_code_id = c.id
    INNER JOIN {{ ref('stg_reference_combined_codesets') }} cc 
        ON c.code = cc.code
    WHERE cc.cluster_id = 'BP_COD'
),

-- Check patient_person mapping coverage
patient_person_coverage AS (
    SELECT 
        'Via patient_person mapping' as source_type,
        COUNT(DISTINCT o.id) as observation_count,
        COUNT(DISTINCT pp.person_id) as persons_with_obs,
        COUNT(DISTINCT CASE WHEN pp.person_id IS NOT NULL THEN o.id END) as obs_with_person,
        COUNT(DISTINCT CASE WHEN pp.person_id IS NULL THEN o.id END) as obs_without_person
    FROM {{ ref('stg_olids_observation') }} o
    LEFT JOIN {{ ref('stg_olids_patient_person') }} pp
        ON o.patient_id = pp.patient_id
    INNER JOIN {{ ref('stg_olids_terminology_concept_map') }} cm 
        ON o.observation_source_concept_id = cm.source_code_id
    INNER JOIN {{ ref('stg_olids_terminology_concept') }} c 
        ON cm.target_code_id = c.id
    INNER JOIN {{ ref('stg_reference_combined_codesets') }} cc 
        ON c.code = cc.code
    WHERE cc.cluster_id = 'BP_COD'
),

-- Check int_patient_person_unique deduplication impact
deduplicated_coverage AS (
    SELECT 
        'Via int_patient_person_unique' as source_type,
        COUNT(DISTINCT o.id) as observation_count,
        COUNT(DISTINCT ppu.person_id) as persons_with_obs,
        COUNT(DISTINCT CASE WHEN ppu.person_id IS NOT NULL THEN o.id END) as obs_with_person,
        COUNT(DISTINCT CASE WHEN ppu.person_id IS NULL THEN o.id END) as obs_without_person
    FROM {{ ref('stg_olids_observation') }} o
    LEFT JOIN {{ ref('int_patient_person_unique') }} ppu
        ON o.patient_id = ppu.patient_id
    INNER JOIN {{ ref('stg_olids_terminology_concept_map') }} cm 
        ON o.observation_source_concept_id = cm.source_code_id
    INNER JOIN {{ ref('stg_olids_terminology_concept') }} c 
        ON cm.target_code_id = c.id
    INNER JOIN {{ ref('stg_reference_combined_codesets') }} cc 
        ON c.code = cc.code
    WHERE cc.cluster_id = 'BP_COD'
),

-- Check what happens with dim_person_demographics filter
with_dim_demographics AS (
    SELECT 
        'With dim_person_demographics' as source_type,
        COUNT(DISTINCT o.id) as observation_count,
        COUNT(DISTINCT d.person_id) as persons_with_obs,
        COUNT(DISTINCT o.id) as obs_with_person,
        0 as obs_without_person
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
)

SELECT * FROM observation_person_coverage
UNION ALL
SELECT * FROM patient_person_coverage
UNION ALL
SELECT * FROM deduplicated_coverage
UNION ALL
SELECT * FROM with_dim_demographics
ORDER BY observation_count DESC;