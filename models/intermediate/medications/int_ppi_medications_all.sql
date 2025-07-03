{{
    config(
        materialized='table',
        cluster_by=['person_id', 'order_date'],
        post_hook=[
            "COMMENT ON TABLE {{ this }} IS 'Intermediate: PPI Medication Orders - All recorded proton pump inhibitor prescriptions.

Clinical Purpose:
• Tracks PPI therapy for gastric acid suppression and ulcer prevention
• Supports gastrointestinal medication management and safety monitoring
• Enables analysis of long-term PPI usage and dosing patterns

Data Granularity:
• One row per medication order
• Includes all patients regardless of status (active/inactive/deceased)
• Historical data maintained for longitudinal analysis

Key Features:
• Specific PPI identification (omeprazole, lansoprazole, esomeprazole, etc.)
• Dose categorisation for therapeutic indication assessment
• H. pylori eradication therapy identification
• Long-term therapy flags for medication review and deprescribing'"
        ]
    )
}}

/*
All PPI (Proton Pump Inhibitor) medication orders for gastric acid suppression.
Uses BNF classification (1.3.5) for proton pump inhibitors.
Includes ALL persons (active, inactive, deceased) following intermediate layer principles.
*/

SELECT
    base_orders.person_id,
    base_orders.medication_order_id,
    base_orders.medication_statement_id,
    base_orders.order_date,
    base_orders.order_medication_name,
    base_orders.order_dose,
    base_orders.order_quantity_value,
    base_orders.order_quantity_unit,
    base_orders.order_duration_days,
    base_orders.statement_medication_name,
    base_orders.mapped_concept_code,
    base_orders.mapped_concept_display,
    base_orders.bnf_code,
    base_orders.bnf_name,

    -- Specific PPI classification
    CASE
        WHEN base_orders.bnf_code LIKE '0103050E%' THEN 'ESOMEPRAZOLE'
        WHEN base_orders.bnf_code LIKE '0103050A%' THEN 'H_PYLORI_ERADICATION'
        WHEN base_orders.bnf_code LIKE '0103050L%' THEN 'LANSOPRAZOLE'
        WHEN base_orders.bnf_code LIKE '0103050P%' THEN 'OMEPRAZOLE'
        WHEN base_orders.bnf_code LIKE '0103050R%' THEN 'PANTOPRAZOLE'
        WHEN base_orders.bnf_code LIKE '0103050T%' THEN 'RABEPRAZOLE'
        ELSE 'OTHER_PPI'
    END AS ppi_type,

    -- Common PPIs flags
    CASE WHEN base_orders.bnf_code LIKE '0103050P%' THEN TRUE ELSE FALSE END AS is_omeprazole,
    CASE WHEN base_orders.bnf_code LIKE '0103050L%' THEN TRUE ELSE FALSE END AS is_lansoprazole,
    CASE WHEN base_orders.bnf_code LIKE '0103050E%' THEN TRUE ELSE FALSE END AS is_esomeprazole,
    CASE WHEN base_orders.bnf_code LIKE '0103050R%' THEN TRUE ELSE FALSE END AS is_pantoprazole,
    CASE WHEN base_orders.bnf_code LIKE '0103050T%' THEN TRUE ELSE FALSE END AS is_rabeprazole,

    -- H. pylori eradication flag
    CASE WHEN base_orders.bnf_code LIKE '0103050A%' THEN TRUE ELSE FALSE END AS is_h_pylori_eradication,

    -- High dose PPI flag (for bleeding prophylaxis)
    CASE
        WHEN (base_orders.bnf_code LIKE '0103050P%' AND base_orders.order_dose LIKE '%40%') OR  -- Omeprazole 40mg
             (base_orders.bnf_code LIKE '0103050L%' AND base_orders.order_dose LIKE '%30%') OR  -- Lansoprazole 30mg
             (base_orders.bnf_code LIKE '0103050E%' AND base_orders.order_dose LIKE '%40%')     -- Esomeprazole 40mg
        THEN TRUE
        ELSE FALSE
    END AS is_high_dose,

    -- Standard dose PPI flag
    CASE
        WHEN (base_orders.bnf_code LIKE '0103050P%' AND base_orders.order_dose LIKE '%20%') OR  -- Omeprazole 20mg
             (base_orders.bnf_code LIKE '0103050L%' AND base_orders.order_dose LIKE '%15%') OR  -- Lansoprazole 15mg
             (base_orders.bnf_code LIKE '0103050E%' AND base_orders.order_dose LIKE '%20%')     -- Esomeprazole 20mg
        THEN TRUE
        ELSE FALSE
    END AS is_standard_dose,

    -- Long-term therapy flag (duration > 8 weeks suggests maintenance therapy)
    CASE
        WHEN base_orders.order_duration_days > 56 THEN TRUE
        ELSE FALSE
    END AS is_long_term_therapy,

    -- Calculate time since order
    DATEDIFF(day, base_orders.order_date, CURRENT_DATE()) AS days_since_order,

    -- Order recency flags
    CASE
        WHEN DATEDIFF(day, base_orders.order_date, CURRENT_DATE()) <= 90 THEN TRUE
        ELSE FALSE
    END AS is_recent_3m,

    CASE
        WHEN DATEDIFF(day, base_orders.order_date, CURRENT_DATE()) <= 180 THEN TRUE
        ELSE FALSE
    END AS is_recent_6m,

    CASE
        WHEN DATEDIFF(day, base_orders.order_date, CURRENT_DATE()) <= 365 THEN TRUE
        ELSE FALSE
    END AS is_recent_12m

FROM ({{ get_medication_orders(bnf_code='0103050') }}) base_orders
ORDER BY base_orders.person_id, base_orders.order_date DESC
