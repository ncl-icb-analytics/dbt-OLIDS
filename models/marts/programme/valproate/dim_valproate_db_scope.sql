{{ config(
    materialized='table',
    description='Cohort of non-male individuals aged 0-55 with a recent (last 6 months) valproate prescription, including details from their most recent order.') }}

-- Core cohort: non-male, age 0-55, recent valproate order
WITH child_bearing_age AS (
    SELECT
        person_id,
        age,
        sex,
        is_child_bearing_age_0_55
    FROM {{ ref('dim_person_women_child_bearing_age') }}
    WHERE is_child_bearing_age_0_55 = TRUE
),

recent_valproate AS (
    SELECT
        person_id,
        most_recent_order_date,
        medication_order_id,
        order_medication_name,
        valproate_product_term,
        recent_order_count,
        order_dose,
        order_quantity_value,
        order_quantity_unit,
        order_duration_days
    FROM {{ ref('int_valproate_medications_6m_latest') }}
)

SELECT
    cba.person_id,
    cba.age,
    cba.sex,
    cba.is_child_bearing_age_0_55,
    rv.most_recent_order_date AS most_recent_valproate_order_date,
    rv.medication_order_id AS valproate_medication_order_id,
    rv.order_medication_name AS valproate_order_medication_name,
    rv.valproate_product_term,
    rv.recent_order_count AS valproate_recent_order_count,
    rv.order_dose AS valproate_order_dose,
    rv.order_quantity_value AS valproate_order_quantity_value,
    rv.order_quantity_unit AS valproate_order_quantity_unit,
    rv.order_duration_days AS valproate_order_duration_days
FROM child_bearing_age AS cba
INNER JOIN recent_valproate AS rv
    ON cba.person_id = rv.person_id -- Only include those with both child-bearing age and recent valproate order
