{{
    config(
        materialized='table',
        cluster_by=['person_id', 'order_date'],
        post_hook=[
            "COMMENT ON TABLE {{ this }} IS 'Intermediate: Diuretic Medication Orders - All recorded diuretic prescriptions.

Clinical Purpose:
• Tracks diuretic therapy for cardiovascular and fluid management
• Supports heart failure management and blood pressure control processes
• Enables analysis of diuretic combinations and potassium management

Data Granularity:
• One row per medication order
• Includes all patients regardless of status (active/inactive/deceased)
• Historical data maintained for longitudinal analysis

Key Features:
• Classification by diuretic type (thiazide, loop, potassium-sparing)
• Specific medication identification with evidence-based therapy flags
• Clinical trial evidence mapping (RALES, EPHESUS trials)
• Heart failure-specific diuretic indicators for quality monitoring'"
        ]
    )
}}

/*
All diuretic medication orders for cardiovascular and fluid management.
Uses BNF classification (2.2) for diuretics.
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

    -- Diuretic type classification
    CASE
        WHEN bnf_code LIKE '020201%' THEN 'THIAZIDE_RELATED'     -- Thiazides and related diuretics
        WHEN bnf_code LIKE '020202%' THEN 'LOOP'                -- Loop diuretics
        WHEN bnf_code LIKE '020203%' THEN 'POTASSIUM_SPARING'   -- Potassium-sparing diuretics and aldosterone antagonists
        WHEN bnf_code LIKE '020204%' THEN 'POTASSIUM_SPARING_WITH_THIAZIDE'  -- Potassium-sparing with thiazides
        WHEN bnf_code LIKE '020205%' THEN 'OSMOTIC'             -- Osmotic diuretics
        WHEN bnf_code LIKE '020206%' THEN 'MERCURIAL'           -- Mercurial diuretics
        WHEN bnf_code LIKE '020207%' THEN 'CARBONIC_ANHYDRASE'  -- Carbonic anhydrase inhibitors
        ELSE 'OTHER_DIURETIC'
    END AS diuretic_type,

    -- Specific diuretic classification
    CASE
        WHEN bnf_code LIKE '%BENDROFLUMETHIAZIDE%' OR bnf_code LIKE '0202010502%' THEN 'BENDROFLUMETHIAZIDE'
        WHEN bnf_code LIKE '%INDAPAMIDE%' OR bnf_code LIKE '0202010520%' THEN 'INDAPAMIDE'
        WHEN bnf_code LIKE '%FUROSEMIDE%' OR bnf_code LIKE '0202020510%' THEN 'FUROSEMIDE'
        WHEN bnf_code LIKE '%BUMETANIDE%' OR bnf_code LIKE '0202020502%' THEN 'BUMETANIDE'
        WHEN bnf_code LIKE '%AMILORIDE%' OR bnf_code LIKE '0202030502%' THEN 'AMILORIDE'
        WHEN bnf_code LIKE '%SPIRONOLACTONE%' OR bnf_code LIKE '0202030520%' THEN 'SPIRONOLACTONE'
        WHEN bnf_code LIKE '%EPLERENONE%' OR bnf_code LIKE '0202030510%' THEN 'EPLERENONE'
        ELSE 'OTHER_DIURETIC'
    END AS specific_diuretic,

    -- Evidence-based diuretics for heart failure
    CASE
        WHEN bnf_code LIKE '%SPIRONOLACTONE%' OR bnf_code LIKE '0202030520%' THEN TRUE  -- RALES trial
        WHEN bnf_code LIKE '%EPLERENONE%' OR bnf_code LIKE '0202030510%' THEN TRUE      -- EPHESUS trial
        WHEN bnf_code LIKE '%FUROSEMIDE%' OR bnf_code LIKE '0202020510%' THEN TRUE      -- Standard loop diuretic
        ELSE FALSE
    END AS is_evidence_based_hf,

    -- Common diuretics flags
    CASE WHEN bnf_code LIKE '%FUROSEMIDE%' OR bnf_code LIKE '0202020510%' THEN TRUE ELSE FALSE END AS is_furosemide,
    CASE WHEN bnf_code LIKE '%BENDROFLUMETHIAZIDE%' OR bnf_code LIKE '0202010502%' THEN TRUE ELSE FALSE END AS is_bendroflumethiazide,
    CASE WHEN bnf_code LIKE '%INDAPAMIDE%' OR bnf_code LIKE '0202010520%' THEN TRUE ELSE FALSE END AS is_indapamide,
    CASE WHEN bnf_code LIKE '%SPIRONOLACTONE%' OR bnf_code LIKE '0202030520%' THEN TRUE ELSE FALSE END AS is_spironolactone,
    CASE WHEN bnf_code LIKE '%AMILORIDE%' OR bnf_code LIKE '0202030502%' THEN TRUE ELSE FALSE END AS is_amiloride,

    -- Diuretic class flags
    CASE WHEN bnf_code LIKE '020201%' THEN TRUE ELSE FALSE END AS is_thiazide,
    CASE WHEN bnf_code LIKE '020202%' THEN TRUE ELSE FALSE END AS is_loop_diuretic,
    CASE WHEN bnf_code LIKE '020203%' THEN TRUE ELSE FALSE END AS is_potassium_sparing,

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
    END AS is_recent_6m,

    CASE
        WHEN DATEDIFF(day, order_date, CURRENT_DATE()) <= 365 THEN TRUE
        ELSE FALSE
    END AS is_recent_12m

FROM (
    {{ get_medication_orders(bnf_code='0202') }}
) base_orders
ORDER BY person_id, order_date DESC
