{{
    config(
        materialized='table'
    )
}}

-- Pregnancy Absence Risk Intermediate Model (Data Collection Layer)
-- Collects observations indicating permanent absence of pregnancy risk
-- Single Responsibility: Data collection for PREGRISK category codes only

WITH valproate_preg_risk_codes AS (
    SELECT 
        code,
        code_category,
        lookback_years_offset
    FROM {{ ref('stg_codesets_valproate_prog_codes') }}
    WHERE code_category = 'PREGRISK'
),

preg_risk_observations AS (
    SELECT
        pp.person_id,
        o.clinical_effective_date,
        mc.concept_code,
        mc.code_description AS concept_display,
        cc.cluster_id AS source_cluster_id,
        vpc.code_category,
        vpc.lookback_years_offset,
        o.date_recorded,
        o.lds_datetime_data_acquired
    FROM {{ ref('stg_olids_observation') }} o
    JOIN {{ ref('stg_olids_patient') }} p
        ON o.patient_id = p.id
    JOIN {{ ref('stg_olids_patient_person') }} pp 
        ON p.id = pp.patient_id
    INNER JOIN {{ ref('stg_codesets_mapped_concepts') }} mc
        ON o.observation_core_concept_id = mc.source_code_id
    INNER JOIN {{ ref('stg_codesets_combined_codesets') }} cc
        ON mc.concept_code = cc.code
    INNER JOIN valproate_preg_risk_codes vpc
        ON cc.code = vpc.code
    WHERE vpc.code_category = 'PREGRISK'
        AND (
            vpc.lookback_years_offset IS NULL 
            OR o.clinical_effective_date >= DATEADD(year, vpc.lookback_years_offset, CURRENT_DATE())
        )
)

SELECT
    person_id,
    clinical_effective_date,
    concept_code,
    concept_display,
    source_cluster_id,
    code_category,
    lookback_years_offset,
    
    -- Standard metadata fields
    date_recorded,
    lds_datetime_data_acquired
    
FROM preg_risk_observations 