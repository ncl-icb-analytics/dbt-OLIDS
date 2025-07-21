{{
    config(
        materialized='table',
        cluster_by=['person_id', 'order_date'])
}}

/*
All ARB (Angiotensin Receptor Blocker) medication orders for cardiovascular and renal protection.
Uses BNF classification (2.5.5.2 - 0205052) for angiotensin-II receptor antagonists.
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
    bnf_name

FROM (
    {{ get_medication_orders(bnf_code='0205052') }}
) base_orders
ORDER BY person_id, order_date DESC
