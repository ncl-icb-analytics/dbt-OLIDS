{{
    config(
        materialized='table',
        tags=['intermediate', 'ethnicity', 'qof', 'demographics'],
        cluster_by=['person_id', 'clinical_effective_date'],
        post_hook=[
            "COMMENT ON TABLE {{ this }} IS 'Intermediate table containing ethnicity information from observations only, with BAME status flags for obesity register.'"
        ]
    )
}}

-- Intermediate Ethnicity QOF - QOF-specific ethnicity observations
-- Uses ETH2016*_COD cluster IDs only (QOF ethnicity codes) with LIKE pattern matching
-- Includes BAME classification for obesity register
-- Includes ALL persons regardless of active status

WITH mapped_observations AS (
    -- Get all observations with proper concept mapping
    SELECT 
        o.id AS observation_id,
        o.patient_id,
        pp.person_id,
        p.sk_patient_id,
        o.clinical_effective_date,
        o.observation_core_concept_id,
        mc.concept_id AS mapped_concept_id,
        mc.concept_code AS mapped_concept_code,
        mc.code_description AS mapped_concept_display,
        mc.cluster_id,
        mc.cluster_description
    FROM {{ ref('stg_olids_observation') }} o
    JOIN {{ ref('stg_olids_patient') }} p
        ON o.patient_id = p.id
    JOIN {{ ref('stg_olids_patient_person') }} pp 
        ON p.id = pp.patient_id
    JOIN {{ ref('stg_codesets_mapped_concepts') }} mc
        ON o.observation_core_concept_id = mc.source_code_id
        AND mc.cluster_id LIKE 'ETH2016%_COD'  -- Filter directly on mapped_concepts
    WHERE o.clinical_effective_date IS NOT NULL
),

qof_ethnicity_enriched AS (
    -- Add BAME classification based on specific cluster IDs
    SELECT 
        mo.*,
        -- BAME classification based on cluster IDs (same as legacy)
        CASE 
            WHEN mo.cluster_id IN (
                'ETH2016MWBC_COD', -- White and Black Caribbean
                'ETH2016MWBA_COD', -- White and Black African
                'ETH2016MWA_COD',  -- White and Asian
                'ETH2016AI_COD',   -- Indian
                'ETH2016AP_COD',   -- Pakistani
                'ETH2016AB_COD',   -- Bangladeshi
                'ETH2016AC_COD',   -- Chinese
                'ETH2016AO_COD',   -- Any other Asian background
                'ETH2016BA_COD',   -- African
                'ETH2016BC_COD',   -- Caribbean
                'ETH2016BO_COD',   -- Any other Black or African or Caribbean background
                'ETH2016OA_COD'    -- Arab
            ) THEN TRUE
            ELSE FALSE
        END AS is_bame
    FROM mapped_observations mo
),

person_level_aggregation AS (
    -- Aggregate all ethnicity concept codes and displays into arrays per person
    SELECT
        person_id,
        ARRAY_AGG(DISTINCT mapped_concept_code) AS all_ethnicity_concept_codes,
        ARRAY_AGG(DISTINCT mapped_concept_display) AS all_ethnicity_concept_displays,
        MAX(clinical_effective_date) AS latest_ethnicity_date,
        MAX(CASE WHEN is_bame THEN clinical_effective_date END) AS latest_bame_date
    FROM qof_ethnicity_enriched
    GROUP BY person_id
)

-- Final selection with QOF ethnicity data
SELECT
    qee.person_id,
    qee.sk_patient_id,
    qee.observation_id,
    qee.clinical_effective_date,
    qee.mapped_concept_code AS concept_code,
    qee.mapped_concept_display AS code_description,
    qee.cluster_id AS source_cluster_id,
    qee.is_bame,
    pla.latest_ethnicity_date,
    pla.latest_bame_date,
    pla.all_ethnicity_concept_codes,
    pla.all_ethnicity_concept_displays
FROM qof_ethnicity_enriched qee
LEFT JOIN person_level_aggregation pla
    ON qee.person_id = pla.person_id
-- Get one row per person (latest ethnicity record)
QUALIFY ROW_NUMBER() OVER (PARTITION BY qee.person_id ORDER BY qee.clinical_effective_date DESC) = 1
ORDER BY qee.person_id 