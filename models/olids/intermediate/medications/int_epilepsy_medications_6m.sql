{{
    config(
        materialized='table',
        cluster_by=['person_id', 'order_date'],
        tags=['intermediate', 'medication', 'epilepsy', 'seizure_management'])
}}

/*
Epilepsy medication orders from the last 6 months for seizure management monitoring.
Uses cluster ID EPILDRUG_COD for anti-epileptic drugs.
Includes ALL persons (active, inactive, deceased) following intermediate layer principles.
*/

SELECT
    person_id,
    patient_id,
    sk_patient_id,
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
    cluster_id
FROM (
    {{ get_medication_orders(cluster_id='EPILDRUG_COD') }}
) base_orders
WHERE order_date >= CURRENT_DATE() - INTERVAL '6 months'
ORDER BY person_id, order_date DESC
