{{ config(
    materialized='table') }}

-- CYP_AST_61 case finding: Children and young people with asthma symptoms but no formal diagnosis
-- Identifies children (18 months to under 18 years) with asthma symptoms who need formal diagnosis
-- Based on legacy logic: medications OR observations in last 12 months, no formal diagnosis

WITH cyp_base_population AS (
    -- Children and young people aged 18 months to under 18 years
    SELECT
        base.person_id,
        age.age,
        age.age_days_approx
    FROM {{ ref('int_ltc_lcs_cf_base_population') }} AS base
    INNER JOIN
        {{ ref('dim_person_age') }} AS age
        ON base.person_id = age.person_id
    WHERE
        age.age_days_approx >= 547  -- 18 months
        AND age.age < 18  -- under 18 years
),

asthma_diagnosis AS (
    -- Patients with formal asthma diagnosis (excluding resolved asthma as latest)
    SELECT DISTINCT person_id
    FROM {{ ref('int_ltc_lcs_cyp_asthma_observations') }}
    WHERE cluster_id IN ('ASTHMA_DIAGNOSIS', 'ASTHMA_RESOLVED')
    QUALIFY ROW_NUMBER() OVER (PARTITION BY person_id ORDER BY clinical_effective_date DESC) = 1
        AND cluster_id != 'ASTHMA_RESOLVED'  -- Exclude those with resolved asthma as latest
),

asthma_symptoms AS (
    -- Get patients with asthma symptoms in last 12 months (medications OR observations)
    SELECT
        person_id,
        MAX(clinical_effective_date) AS latest_symptom_date
    FROM (
        -- Asthma medications in last 12 months
        SELECT
            person_id,
            order_date AS clinical_effective_date
        FROM {{ ref('int_ltc_lcs_cyp_asthma_medications') }}
        WHERE 
            cluster_id IN ('ASTHMA_MEDICATIONS', 'ASTHMA_PREDNISOLONE', 'MONTELUKAST_MEDICATIONS')
            AND order_date >= DATEADD(MONTH, -12, CURRENT_DATE())
        
        UNION ALL
        
        -- Suspected asthma or viral wheeze observations in last 12 months
        SELECT
            person_id,
            clinical_effective_date
        FROM {{ ref('int_ltc_lcs_cyp_asthma_observations') }}
        WHERE
            cluster_id IN ('SUSPECTED_ASTHMA', 'VIRAL_WHEEZE')
            AND clinical_effective_date >= DATEADD(MONTH, -12, CURRENT_DATE())
    )
    GROUP BY person_id
)

-- Final selection: CYP with asthma symptoms but no formal diagnosis
SELECT
    bp.person_id,
    bp.age,
    CASE
        WHEN symptoms.latest_symptom_date IS NOT NULL THEN TRUE
        ELSE FALSE
    END AS has_asthma_symptoms,
    symptoms.latest_symptom_date
FROM cyp_base_population AS bp
INNER JOIN asthma_symptoms AS symptoms ON bp.person_id = symptoms.person_id
WHERE NOT EXISTS (
    SELECT 1 FROM asthma_diagnosis AS ad
    WHERE ad.person_id = bp.person_id
)
