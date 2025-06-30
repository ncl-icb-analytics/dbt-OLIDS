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
        code_category
    FROM {{ ref('stg_codesets_valproate_prog_codes') }}
    WHERE code_category = 'PREGRISK'
),

preg_risk_observations AS (
    SELECT
        pp.person_id,
        o.clinical_effective_date,
        mc.concept_code,
        vpc.code_category,
        o.date_recorded,
        o.lds_datetime_data_acquired,
        COALESCE(mc.code_description, 'Unknown PREGRISK Code')
            AS concept_display,
        COALESCE(mc.cluster_id, 'PREGRISK') AS source_cluster_id
    FROM {{ ref('stg_olids_observation') }} AS o
    INNER JOIN {{ ref('stg_olids_patient') }} AS p
        ON o.patient_id = p.id
    INNER JOIN {{ ref('stg_olids_patient_person') }} AS pp
        ON p.id = pp.patient_id
    INNER JOIN {{ ref('stg_codesets_mapped_concepts') }} AS mc
        ON o.observation_core_concept_id = mc.source_code_id
    INNER JOIN valproate_preg_risk_codes AS vpc
        ON mc.concept_code = vpc.code
    WHERE
        vpc.code_category = 'PREGRISK'
        AND o.clinical_effective_date IS NOT NULL  -- Ensure we have valid dates
)

SELECT
    person_id,
    clinical_effective_date,
    concept_code,
    concept_display,
    source_cluster_id,
    code_category,

    -- Standard metadata fields
    date_recorded,
    lds_datetime_data_acquired

FROM preg_risk_observations
