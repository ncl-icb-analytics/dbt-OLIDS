{{ config(
    materialized='table',
    post_hook="ALTER TABLE {{ this }} SET COMMENT = 'CYP_AST_61 case finding: Children and young people (18 months to under 18 years) with asthma symptoms but no formal diagnosis'"
) }}

-- CYP_AST_61 case finding: Children and young people with asthma symptoms but no formal diagnosis
-- Identifies children (18 months to under 18 years) with asthma symptoms who need formal diagnosis

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
-- Patients with formal asthma diagnosis (excluding resolved asthma)
    SELECT DISTINCT person_id
    FROM (
        SELECT
            person_id,
            cluster_id,
            ROW_NUMBER()
                OVER (
                    PARTITION BY person_id ORDER BY clinical_effective_date DESC
                )
                AS rn
        FROM {{ ref('int_ltc_lcs_cyp_asthma_observations') }}
        WHERE cluster_id IN ('ASTHMA_DIAGNOSIS', 'ASTHMA_RESOLVED')
    )
    WHERE rn = 1 AND cluster_id != 'ASTHMA_RESOLVED'  -- Exclude those with resolved asthma as latest
),

asthma_medications AS (
-- Patients with asthma medications in last 12 months
    SELECT
        person_id,
        MAX(order_date) AS latest_medication_date
    FROM {{ ref('int_ltc_lcs_cyp_asthma_medications') }}
    WHERE order_date >= DATEADD(MONTH, -12, CURRENT_DATE())
    GROUP BY person_id
),

asthma_symptoms_obs AS (
-- Patients with asthma symptom observations in last 12 months
    SELECT
        person_id,
        MAX(clinical_effective_date) AS latest_symptom_date
    FROM {{ ref('int_ltc_lcs_cyp_asthma_observations') }}
    WHERE
        cluster_id IN ('SUSPECTED_ASTHMA', 'VIRAL_WHEEZE')
        AND clinical_effective_date >= DATEADD(MONTH, -12, CURRENT_DATE())
    GROUP BY person_id
),

asthma_symptoms AS (
-- Combine medications and symptom observations
    SELECT
        cyp_base_population.person_id,
        GREATEST(
            COALESCE(meds.latest_medication_date, '1900-01-01'),
            COALESCE(obs.latest_symptom_date, '1900-01-01')
        ) AS latest_symptom_date
    FROM cyp_base_population
    LEFT JOIN
        asthma_medications AS meds
        ON cyp_base_population.person_id = meds.person_id
    LEFT JOIN asthma_symptoms_obs AS obs ON cyp_base_population.person_id = obs.person_id
    WHERE (meds.person_id IS NOT NULL OR obs.person_id IS NOT NULL)
),

eligible_patients AS (
-- CYP with asthma symptoms but no formal diagnosis
    SELECT
        cyp.person_id,
        cyp.age,
        symptoms.latest_symptom_date,
        TRUE AS has_asthma_symptoms
    FROM cyp_base_population AS cyp
    INNER JOIN asthma_symptoms AS symptoms ON cyp.person_id = symptoms.person_id
    WHERE NOT EXISTS (
        SELECT 1 FROM asthma_diagnosis AS ad
        WHERE ad.person_id = cyp.person_id
    )
)

-- Final selection: CYP with asthma symptoms but no formal diagnosis
SELECT
    ep.person_id,
    ep.age,
    ep.has_asthma_symptoms,
    ep.latest_symptom_date
FROM eligible_patients AS ep
