-- Intermediate model for LTC LCS Case Finding Exclusions
-- Identifies patients with any of the specified conditions that exclude them from the LTC LCS Case Finding programme.
-- Includes CKD, AF, COPD, Hypertension, CHD, Stroke/TIA, PAD, Heart Failure, Type 2 Diabetes, Hyperlipidaemia, NAFLD, and Asthma (adult and CYP).

WITH ltc_summary_conditions AS (
    SELECT
        person_id,
        condition_code,
        earliest_diagnosis_date,
        latest_diagnosis_date

    FROM {{ ref('fct_person_ltc_summary') }}
    WHERE condition_code IN (
        'CKD',
        'AF',
        'COPD',
        'HTN',
        'CHD',
        'STIA',
        'PAD',
        'HF',
        'FHYP',
        'NAF',
        'AST',
        'CYP_AST'
    )
    AND is_on_register = TRUE
),

type2_diabetes AS (
    SELECT
        person_id,
        earliest_type2_date AS earliest_diagnosis_date,
        latest_type2_date AS latest_diagnosis_date
    FROM {{ ref('fct_person_diabetes_register') }}
    WHERE diabetes_type = 'Type 2'

),

all_conditions AS (
    SELECT * FROM ltc_summary_conditions
    UNION ALL
    SELECT
        person_id,
        'DM2' AS condition_code,
        earliest_diagnosis_date,
        latest_diagnosis_date
    FROM type2_diabetes
),

person_level_aggregation AS (
    SELECT
        person_id,
        MIN(earliest_diagnosis_date) AS earliest_excluding_condition_date,
        BOOLOR_AGG(condition_code = 'CKD') AS has_ckd,
        BOOLOR_AGG(condition_code = 'AF') AS has_af,
        BOOLOR_AGG(condition_code = 'COPD') AS has_copd,
        BOOLOR_AGG(condition_code = 'HTN') AS has_hypertension,
        BOOLOR_AGG(condition_code = 'CHD') AS has_chd,
        BOOLOR_AGG(condition_code = 'STIA') AS has_stia,
        BOOLOR_AGG(condition_code = 'PAD') AS has_pad,
        BOOLOR_AGG(condition_code = 'HF') AS has_hf,
        BOOLOR_AGG(condition_code = 'DM2') AS has_type2_diabetes,
        BOOLOR_AGG(condition_code = 'FHYP') AS has_hyperlipidaemia,
        BOOLOR_AGG(condition_code = 'NAF') AS has_nafld,
        BOOLOR_AGG(condition_code = 'AST') AS has_asthma,
        BOOLOR_AGG(condition_code = 'CYP_AST') AS has_cyp_asthma
    FROM all_conditions
    GROUP BY person_id
)

SELECT
    person_id,
    has_ckd,
    has_af,
    has_copd,
    has_hypertension,
    has_chd,
    has_stia,
    has_pad,
    has_hf,
    has_type2_diabetes,
    has_hyperlipidaemia,
    has_nafld,
    has_asthma,
    has_cyp_asthma,
    earliest_excluding_condition_date,
    (
        has_ckd OR has_af OR has_copd OR has_hypertension OR has_chd
        OR has_stia OR has_pad OR has_hf OR has_type2_diabetes
        OR has_hyperlipidaemia OR has_nafld OR has_asthma OR has_cyp_asthma
    ) AS has_excluding_condition
FROM person_level_aggregation
