/*
Analysis: Compare get_observations macro logic vs direct table joins
Purpose: Identify if the macro's QUALIFY deduplication or join logic causes data loss
*/

-- Replicate the get_observations macro logic manually
WITH macro_logic_replicated AS (
    -- This mimics what get_observations does
    WITH cluster_codes AS (
        SELECT DISTINCT 
            code as mapped_concept_code,
            cluster_id,
            cluster_description,
            code_description
        FROM {{ ref('stg_reference_combined_codesets') }}
        WHERE cluster_id = 'BP_COD'
    )
    SELECT 
        o.observation_id,
        o.patient_id,
        pp.person_id,
        o.clinical_effective_date,
        o.mapped_concept_code,
        cc.cluster_id
    FROM {{ ref('int_observations_mapped') }} o
    JOIN {{ ref('int_patient_person_unique') }} pp
        ON o.patient_id = pp.patient_id
    INNER JOIN cluster_codes cc
        ON o.mapped_concept_code = cc.mapped_concept_code
    -- The QUALIFY clause from the macro
    QUALIFY ROW_NUMBER() OVER (
        PARTITION BY o.observation_id, cc.cluster_id 
        ORDER BY o.mapped_concept_code
    ) = 1
),

-- Direct join without macro logic
direct_join AS (
    SELECT 
        o.observation_id,
        o.patient_id,
        pp.person_id,
        o.clinical_effective_date,
        o.mapped_concept_code,
        cc.cluster_id
    FROM {{ ref('int_observations_mapped') }} o
    JOIN {{ ref('int_patient_person_unique') }} pp
        ON o.patient_id = pp.patient_id
    INNER JOIN {{ ref('stg_reference_combined_codesets') }} cc
        ON o.mapped_concept_code = cc.code
    WHERE cc.cluster_id = 'BP_COD'
),

-- Compare counts
comparison AS (
    SELECT 
        'Macro logic (with QUALIFY)' as approach,
        COUNT(DISTINCT observation_id) as observation_count,
        COUNT(DISTINCT person_id) as person_count,
        COUNT(*) as row_count
    FROM macro_logic_replicated
    
    UNION ALL
    
    SELECT 
        'Direct join (no QUALIFY)' as approach,
        COUNT(DISTINCT observation_id) as observation_count,
        COUNT(DISTINCT person_id) as person_count,
        COUNT(*) as row_count
    FROM direct_join
)

SELECT * FROM comparison;

-- Check for duplicate observations that would be affected by QUALIFY
WITH duplicates_check AS (
    SELECT 
        o.observation_id,
        COUNT(DISTINCT cc.code) as unique_codes_per_obs,
        COUNT(*) as total_rows_per_obs
    FROM {{ ref('int_observations_mapped') }} o
    JOIN {{ ref('int_patient_person_unique') }} pp
        ON o.patient_id = pp.patient_id
    INNER JOIN {{ ref('stg_reference_combined_codesets') }} cc
        ON o.mapped_concept_code = cc.code
    WHERE cc.cluster_id = 'BP_COD'
    GROUP BY o.observation_id
    HAVING COUNT(*) > 1
)

SELECT 
    'Observations with multiple BP_COD mappings' as metric,
    COUNT(*) as count,
    AVG(unique_codes_per_obs) as avg_codes_per_obs,
    MAX(unique_codes_per_obs) as max_codes_per_obs,
    AVG(total_rows_per_obs) as avg_rows_per_obs,
    MAX(total_rows_per_obs) as max_rows_per_obs
FROM duplicates_check;

-- Check if int_observations_mapped is missing data
WITH raw_vs_mapped AS (
    SELECT 
        'Raw observations with BP codes' as source,
        COUNT(DISTINCT o.id) as observation_count
    FROM {{ ref('stg_olids_observation') }} o
    INNER JOIN {{ ref('stg_olids_terminology_concept_map') }} cm 
        ON o.observation_source_concept_id = cm.source_code_id
    INNER JOIN {{ ref('stg_olids_terminology_concept') }} c 
        ON cm.target_code_id = c.id
    INNER JOIN {{ ref('stg_reference_combined_codesets') }} cc 
        ON c.code = cc.code
    WHERE cc.cluster_id = 'BP_COD'
    
    UNION ALL
    
    SELECT 
        'int_observations_mapped with BP codes' as source,
        COUNT(DISTINCT observation_id) as observation_count
    FROM {{ ref('int_observations_mapped') }} o
    INNER JOIN {{ ref('stg_reference_combined_codesets') }} cc
        ON o.mapped_concept_code = cc.code
    WHERE cc.cluster_id = 'BP_COD'
)

SELECT * FROM raw_vs_mapped;

-- Check patient_person_unique coverage
WITH patient_coverage AS (
    SELECT 
        'Patients with BP observations' as metric,
        COUNT(DISTINCT o.patient_id) as count
    FROM {{ ref('int_observations_mapped') }} o
    INNER JOIN {{ ref('stg_reference_combined_codesets') }} cc
        ON o.mapped_concept_code = cc.code
    WHERE cc.cluster_id = 'BP_COD'
    
    UNION ALL
    
    SELECT 
        'Patients in int_patient_person_unique' as metric,
        COUNT(DISTINCT patient_id) as count
    FROM {{ ref('int_patient_person_unique') }}
    
    UNION ALL
    
    SELECT 
        'BP patients with person mapping' as metric,
        COUNT(DISTINCT o.patient_id) as count
    FROM {{ ref('int_observations_mapped') }} o
    INNER JOIN {{ ref('int_patient_person_unique') }} pp
        ON o.patient_id = pp.patient_id
    INNER JOIN {{ ref('stg_reference_combined_codesets') }} cc
        ON o.mapped_concept_code = cc.code
    WHERE cc.cluster_id = 'BP_COD'
)

SELECT * FROM patient_coverage;