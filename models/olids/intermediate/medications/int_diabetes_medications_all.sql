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

    -- Diabetes medication type classification (corrected BNF codes)
    CASE
        WHEN bnf_code LIKE '0601011%' OR bnf_code LIKE '0601012%' THEN 'INSULIN'  -- BNF 6.1.1: Insulins
        WHEN bnf_code LIKE '0601021%' OR bnf_code LIKE '0601022%' OR bnf_code LIKE '0601023%' THEN 'ANTIDIABETIC'  -- BNF 6.1.2: Antidiabetic drugs
        WHEN bnf_code LIKE '0601040%' THEN 'HYPOGLYCAEMIA_TREATMENT'   -- BNF 6.1.4: Treatment of hypoglycaemia
        WHEN bnf_code LIKE '0601060%' THEN 'MONITORING'                -- BNF 6.1.6: Diabetic diagnostic and monitoring agents
        ELSE 'OTHER_DIABETES'
    END AS diabetes_medication_type,

    -- Antidiabetic drug class classification (corrected BNF 6.1.2 subcodes)
    CASE
        WHEN bnf_code LIKE '0601021%' THEN 'SULPHONYLUREAS'            -- 6.1.2.1: Sulphonylureas
        WHEN bnf_code LIKE '0601022%' THEN 'BIGUANIDES'                -- 6.1.2.2: Biguanides (metformin)
        WHEN bnf_code LIKE '0601023%' THEN 'OTHER_ANTIDIABETICS'       -- 6.1.2.3: Other antidiabetic drugs
        ELSE NULL
    END AS antidiabetic_drug_class,

    -- Insulin type classification (corrected BNF 6.1.1 subcodes)
    CASE
        WHEN bnf_code LIKE '0601011%' THEN 'SHORT_ACTING'              -- 6.1.1.1: Short-acting insulins
        WHEN bnf_code LIKE '0601012%' THEN 'INTERMEDIATE_LONG_ACTING'   -- 6.1.1.2: Intermediate and long-acting insulins
        ELSE NULL
    END AS insulin_type,

    -- Key medication flags (corrected BNF codes)
    CASE WHEN bnf_code LIKE '0601011%' OR bnf_code LIKE '0601012%' THEN TRUE ELSE FALSE END AS is_insulin,
    CASE WHEN bnf_code LIKE '0601022%' THEN TRUE ELSE FALSE END AS is_metformin,
    CASE WHEN bnf_code LIKE '0601021%' THEN TRUE ELSE FALSE END AS is_sulphonylurea,


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
