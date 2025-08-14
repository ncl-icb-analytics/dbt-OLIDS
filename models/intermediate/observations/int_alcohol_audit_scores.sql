{{
    config(
        materialized='table',
        cluster_by=['person_id', 'clinical_effective_date'])
}}

/*
All AUDIT and AUDIT-C alcohol screening scores from observations.
Includes ALL persons (active, inactive, deceased).
Captures both AUDIT-C (brief 3-question screen) and full AUDIT (10-question assessment).

UK AUDIT-C Scoring (0-12):
- 0: Non-drinker
- 1-2: Minimal/occasional drinking  
- 3-4: Lower risk drinking
- 5-7: Increasing risk
- 8-10: Higher risk
- 11-12: Possible dependence
- Score ≥5 is positive screen indicating potential harm

UK Full AUDIT Scoring (0-40):
- 0: Non-drinker
- 1-3: Minimal/occasional drinking
- 4-7: Lower risk drinking
- 8-15: Increasing risk
- 16-19: Higher risk
- 20+: Possible dependence
*/

WITH base_observations AS (
    SELECT
        obs.observation_id,
        obs.person_id,
        obs.clinical_effective_date,
        CAST(obs.result_value AS NUMBER(10,2)) AS audit_score,
        obs.result_unit_display,
        obs.mapped_concept_code AS concept_code,
        obs.mapped_concept_display AS concept_display,
        obs.cluster_id AS source_cluster_id,
        obs.result_value AS original_result_value

    FROM ({{ get_observations("'AUDITC_COD', 'AUDIT_COD'") }}) obs
    WHERE obs.clinical_effective_date IS NOT NULL
      AND obs.result_value IS NOT NULL
      AND obs.clinical_effective_date <= CURRENT_DATE() -- No future dates
),

observations_with_demographics AS (
    SELECT 
        bo.*,
        pd.sex
    FROM base_observations bo
    LEFT JOIN {{ ref('dim_person_demographics') }} pd
        ON bo.person_id = pd.person_id
)

SELECT
    person_id,
    observation_id,
    clinical_effective_date,
    audit_score,
    result_unit_display,
    concept_code,
    concept_display,
    source_cluster_id,
    original_result_value,
    sex,

    -- Determine AUDIT type
    CASE 
        WHEN source_cluster_id = 'AUDITC_COD' THEN 'AUDIT-C'
        WHEN source_cluster_id = 'AUDIT_COD' THEN 'Full AUDIT'
        ELSE 'Unknown'
    END AS audit_type,

    -- Data quality validation
    CASE
        WHEN source_cluster_id = 'AUDITC_COD' AND audit_score BETWEEN 0 AND 12 THEN TRUE
        WHEN source_cluster_id = 'AUDIT_COD' AND audit_score BETWEEN 0 AND 40 THEN TRUE
        ELSE FALSE
    END AS is_valid_score,

    -- Risk categorisation using UK AUDIT thresholds with detailed lower end
    CASE
        WHEN source_cluster_id = 'AUDITC_COD' THEN
            CASE
                WHEN audit_score NOT BETWEEN 0 AND 12 THEN 'Invalid'
                WHEN audit_score = 0 THEN 'Non-Drinker'
                WHEN audit_score <= 2 THEN 'Occasional Drinker'
                WHEN audit_score <= 4 THEN 'Lower Risk'
                WHEN audit_score <= 7 THEN 'Increasing Risk'
                WHEN audit_score <= 10 THEN 'Higher Risk'
                ELSE 'Possible Dependence'
            END
        WHEN source_cluster_id = 'AUDIT_COD' THEN
            CASE
                WHEN audit_score NOT BETWEEN 0 AND 40 THEN 'Invalid'
                WHEN audit_score = 0 THEN 'Non-Drinker'
                WHEN audit_score <= 3 THEN 'Occasional Drinker'
                WHEN audit_score <= 7 THEN 'Lower Risk'
                WHEN audit_score <= 15 THEN 'Increasing Risk'
                WHEN audit_score <= 19 THEN 'Higher Risk'
                ELSE 'Possible Dependence'
            END
        ELSE 'Unknown'
    END AS risk_category,

    -- Simplified risk level (for easier aggregation)
    CASE
        WHEN source_cluster_id IN ('AUDITC_COD', 'AUDIT_COD') THEN
            CASE
                WHEN audit_score = 0 THEN 'Non-Drinker'
                WHEN source_cluster_id = 'AUDITC_COD' THEN
                    CASE
                        WHEN audit_score <= 4 THEN 'Lower Risk'
                        WHEN audit_score <= 7 THEN 'Increasing Risk'
                        ELSE 'Higher Risk'
                    END
                WHEN source_cluster_id = 'AUDIT_COD' THEN
                    CASE
                        WHEN audit_score <= 7 THEN 'Lower Risk'
                        WHEN audit_score <= 15 THEN 'Increasing Risk'
                        ELSE 'Higher Risk'
                    END
            END
    END AS simplified_risk_level,

    -- AUDIT-C positive screen flag (score ≥5 indicates potential harm)
    CASE
        WHEN source_cluster_id = 'AUDITC_COD' AND audit_score >= 5 THEN TRUE
        ELSE FALSE
    END AS auditc_positive_screen,

    -- Additional flag for high-risk scores requiring specialist intervention
    CASE
        WHEN source_cluster_id = 'AUDITC_COD' AND audit_score >= 8 THEN TRUE
        WHEN source_cluster_id = 'AUDIT_COD' AND audit_score >= 16 THEN TRUE
        ELSE FALSE
    END AS requires_specialist_intervention,

    -- Flag for brief intervention threshold
    CASE
        WHEN source_cluster_id = 'AUDITC_COD' AND audit_score >= 5 THEN TRUE
        WHEN source_cluster_id = 'AUDIT_COD' AND audit_score >= 8 THEN TRUE
        ELSE FALSE
    END AS brief_intervention_indicated

FROM observations_with_demographics

-- Sort for consistent output
ORDER BY person_id, clinical_effective_date DESC