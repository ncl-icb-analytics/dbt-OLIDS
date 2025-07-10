{{
    config(
        materialized='table',
        cluster_by=['person_id', 'order_date'])
}}

/*
All diabetes medication orders including insulins, antidiabetic drugs, and hypoglycaemia treatments.
Uses BNF classification (6.1.x) with detailed medication type categorisation.
Includes ALL persons (active, inactive, deceased) following intermediate layer principles.
*/

SELECT
    person_id,
    medication_order_id,
    medication_statement_id,
    order_date,
    order_medication_name,
    order_dose,
    order_quantity_value,
    order_quantity_unit,
    order_duration_days,
    statement_medication_name,
    mapped_concept_code,
    mapped_concept_display,
    bnf_code,
    bnf_name,

    -- Diabetes medication type classification
    CASE
        WHEN bnf_code LIKE '060101%' THEN 'INSULIN'                    -- BNF 6.1.1: Insulins
        WHEN bnf_code LIKE '060102%' THEN 'ANTIDIABETIC'              -- BNF 6.1.2: Antidiabetic drugs
        WHEN bnf_code LIKE '060103%' THEN 'DIABETIC_KETOACIDOSIS'     -- BNF 6.1.3: Diabetic ketoacidosis
        WHEN bnf_code LIKE '060104%' THEN 'HYPOGLYCAEMIA_TREATMENT'   -- BNF 6.1.4: Treatment of hypoglycaemia
        WHEN bnf_code LIKE '060105%' THEN 'BLOOD_GLUCOSE_TESTING'     -- BNF 6.1.5: Blood glucose testing
        WHEN bnf_code LIKE '060106%' THEN 'MONITORING'                -- BNF 6.1.6: Diabetic diagnostic and monitoring agents
        ELSE 'OTHER_DIABETES'
    END AS diabetes_medication_type,

    -- Antidiabetic drug class classification (only for BNF 6.1.2)
    CASE
        WHEN bnf_code LIKE '06010201%' THEN 'SULPHONYLUREAS'
        WHEN bnf_code LIKE '06010202%' THEN 'BIGUANIDES'
        WHEN bnf_code LIKE '06010203%' THEN 'OTHER_ANTIDIABETICS'
        WHEN bnf_code LIKE '06010204%' THEN 'THIAZOLIDINEDIONES'
        WHEN bnf_code LIKE '06010205%' THEN 'MEGLITINIDES'
        WHEN bnf_code LIKE '06010206%' THEN 'ALPHA_GLUCOSIDASE_INHIBITORS'
        WHEN bnf_code LIKE '06010207%' THEN 'DPP4_INHIBITORS'
        WHEN bnf_code LIKE '06010208%' THEN 'SODIUM_GLUCOSE_COTRANSPORTER_2_INHIBITORS'
        WHEN bnf_code LIKE '06010209%' THEN 'GLP1_RECEPTOR_AGONISTS'
        ELSE NULL
    END AS antidiabetic_drug_class,

    -- Insulin type classification (only for BNF 6.1.1)
    CASE
        WHEN bnf_code LIKE '06010101%' THEN 'SHORT_ACTING'
        WHEN bnf_code LIKE '06010102%' THEN 'INTERMEDIATE_ACTING'
        WHEN bnf_code LIKE '06010103%' THEN 'LONG_ACTING'
        WHEN bnf_code LIKE '06010104%' THEN 'BIPHASIC'
        ELSE NULL
    END AS insulin_type,

    -- Key medication flags
    CASE WHEN bnf_code LIKE '060101%' THEN TRUE ELSE FALSE END AS is_insulin,
    CASE WHEN bnf_code LIKE '06010202%' THEN TRUE ELSE FALSE END AS is_metformin,
    CASE WHEN bnf_code LIKE '06010201%' THEN TRUE ELSE FALSE END AS is_sulphonylurea,
    CASE WHEN bnf_code LIKE '06010207%' THEN TRUE ELSE FALSE END AS is_dpp4_inhibitor,
    CASE WHEN bnf_code LIKE '06010208%' THEN TRUE ELSE FALSE END AS is_sglt2_inhibitor,
    CASE WHEN bnf_code LIKE '06010209%' THEN TRUE ELSE FALSE END AS is_glp1_agonist,

    -- Calculate time since order
    DATEDIFF(day, order_date, CURRENT_DATE()) AS days_since_order,

    -- Order recency flags
    CASE
        WHEN DATEDIFF(day, order_date, CURRENT_DATE()) <= 90 THEN TRUE
        ELSE FALSE
    END AS is_recent_3m,

    CASE
        WHEN DATEDIFF(day, order_date, CURRENT_DATE()) <= 180 THEN TRUE
        ELSE FALSE
    END AS is_recent_6m

FROM (
    {{ get_medication_orders(bnf_code='0601') }}
) base_orders
ORDER BY person_id, order_date DESC
