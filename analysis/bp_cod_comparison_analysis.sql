/*
Analysis: Fair comparison of BP_COD observation counts between approaches
Purpose: Apply equivalent filters to both native and modern approaches for accurate comparison
*/

-- Native approach with equivalent filters to dim_person_demographics (using staging views)
WITH native_with_filters AS (
    SELECT 
        o.name as practice_name,
        o.organisation_code as practice_code,
        COUNT(DISTINCT obs.id) as observation_count
    FROM {{ ref('stg_olids_observation') }} obs
    INNER JOIN {{ ref('stg_olids_patient_person') }} pp 
        ON obs.patient_id = pp.patient_id
    INNER JOIN {{ ref('stg_olids_person') }} p 
        ON pp.person_id = p.id
    INNER JOIN {{ ref('stg_olids_patient') }} pat 
        ON pp.patient_id = pat.id
    -- Get registration history
    INNER JOIN (
        SELECT DISTINCT patient_id 
        FROM {{ ref('stg_olids_patient_registered_practitioner_in_role') }}
        WHERE start_date IS NOT NULL
    ) prpr ON pp.patient_id = prpr.patient_id
    -- Get organisation details
    INNER JOIN {{ ref('stg_olids_organisation') }} o
        ON obs.record_owner_organisation_code = o.organisation_code
    -- Concept mappings
    INNER JOIN {{ ref('stg_olids_terminology_concept_map') }} cm 
        ON obs.observation_source_concept_id = cm.source_code_id
    INNER JOIN {{ ref('stg_olids_terminology_concept') }} c 
        ON cm.target_code_id = c.id
    INNER JOIN {{ ref('stg_reference_combined_codesets') }} cc 
        ON c.code = cc.code
    WHERE cc.cluster_id = 'BP_COD'
        -- Apply equivalent filters to dim_person_demographics
        AND pat.birth_year IS NOT NULL  -- Has birth date
        AND pp.patient_id IS NOT NULL
        AND pp.person_id IS NOT NULL
    GROUP BY o.name, o.organisation_code
),

-- Modern approach using dim_person_demographics
modern_with_dim AS (
    SELECT 
        d.practice_name,
        d.practice_code,
        COUNT(DISTINCT o.observation_id) as observation_count
    FROM {{ ref('int_observations_mapped') }} o
    INNER JOIN {{ ref('dim_person_demographics') }} d 
        ON o.person_id = d.person_id
    INNER JOIN {{ ref('stg_reference_combined_codesets') }} cc 
        ON o.mapped_concept_code = cc.code
    WHERE cc.cluster_id = 'BP_COD'
    GROUP BY d.practice_name, d.practice_code
),

-- Hybrid approach: Staging tables with dim_person_demographics join
hybrid_approach AS (
    SELECT 
        d.practice_name,
        d.practice_code,
        COUNT(DISTINCT obs.id) as observation_count
    FROM {{ ref('stg_olids_observation') }} obs
    INNER JOIN {{ ref('stg_olids_patient_person') }} pp 
        ON obs.patient_id = pp.patient_id
    INNER JOIN {{ ref('dim_person_demographics') }} d 
        ON pp.person_id = d.person_id
    INNER JOIN {{ ref('stg_olids_terminology_concept_map') }} cm 
        ON obs.observation_source_concept_id = cm.source_code_id
    INNER JOIN {{ ref('stg_olids_terminology_concept') }} c 
        ON cm.target_code_id = c.id
    INNER JOIN {{ ref('stg_reference_combined_codesets') }} cc 
        ON c.code = cc.code
    WHERE cc.cluster_id = 'BP_COD'
    GROUP BY d.practice_name, d.practice_code
)

-- Comparison results
SELECT 
    COALESCE(n.practice_code, m.practice_code, h.practice_code) as practice_code,
    COALESCE(n.practice_name, m.practice_name, h.practice_name) as practice_name,
    n.observation_count as native_count,
    m.observation_count as modern_count,
    h.observation_count as hybrid_count,
    -- Calculate differences
    m.observation_count - n.observation_count as modern_vs_native_diff,
    h.observation_count - n.observation_count as hybrid_vs_native_diff,
    -- Percentage differences
    ROUND(100.0 * (m.observation_count - n.observation_count) / NULLIF(n.observation_count, 0), 2) as modern_pct_diff,
    ROUND(100.0 * (h.observation_count - n.observation_count) / NULLIF(n.observation_count, 0), 2) as hybrid_pct_diff
FROM native_with_filters n
FULL OUTER JOIN modern_with_dim m 
    ON n.practice_code = m.practice_code
FULL OUTER JOIN hybrid_approach h 
    ON n.practice_code = h.practice_code
ORDER BY ABS(COALESCE(modern_vs_native_diff, 0)) DESC;