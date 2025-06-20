{{
    config(
        materialized='table',
        cluster_by=['person_id', 'order_date']
    )
}}

/*
All antidepressant medication orders for mental health conditions.
Uses BNF classification (4.3) for antidepressant drugs.
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
    
    -- Antidepressant class classification
    CASE 
        WHEN bnf_code LIKE '040301%' THEN 'TRICYCLIC'                    -- Tricyclic and related antidepressants
        WHEN bnf_code LIKE '040302%' THEN 'MAOI'                        -- Monoamine-oxidase inhibitors
        WHEN bnf_code LIKE '040303%' THEN 'SSRI'                        -- Selective serotonin re-uptake inhibitors
        WHEN bnf_code LIKE '040304%' THEN 'OTHER_ANTIDEPRESSANTS'       -- Other antidepressant drugs
        ELSE 'UNKNOWN_ANTIDEPRESSANT'
    END AS antidepressant_class,
    
    -- Specific antidepressant classification
    CASE 
        -- SSRIs
        WHEN bnf_code LIKE '%CITALOPRAM%' OR bnf_code LIKE '0403030C0%' THEN 'CITALOPRAM'
        WHEN bnf_code LIKE '%ESCITALOPRAM%' OR bnf_code LIKE '0403030U0%' THEN 'ESCITALOPRAM'
        WHEN bnf_code LIKE '%FLUOXETINE%' OR bnf_code LIKE '0403030F0%' THEN 'FLUOXETINE'
        WHEN bnf_code LIKE '%PAROXETINE%' OR bnf_code LIKE '0403030P0%' THEN 'PAROXETINE'
        WHEN bnf_code LIKE '%SERTRALINE%' OR bnf_code LIKE '0403030S0%' THEN 'SERTRALINE'
        -- SNRIs and other newer antidepressants  
        WHEN bnf_code LIKE '%VENLAFAXINE%' OR bnf_code LIKE '0403040W0%' THEN 'VENLAFAXINE'
        WHEN bnf_code LIKE '%DULOXETINE%' OR bnf_code LIKE '0403040T0%' THEN 'DULOXETINE'
        WHEN bnf_code LIKE '%MIRTAZAPINE%' OR bnf_code LIKE '0403040S0%' THEN 'MIRTAZAPINE'
        -- Tricyclics
        WHEN bnf_code LIKE '%AMITRIPTYLINE%' OR bnf_code LIKE '0403010B0%' THEN 'AMITRIPTYLINE'
        WHEN bnf_code LIKE '%DOSULEPIN%' OR bnf_code LIKE '0403010E0%' THEN 'DOSULEPIN'
        WHEN bnf_code LIKE '%NORTRIPTYLINE%' OR bnf_code LIKE '0403010V0%' THEN 'NORTRIPTYLINE'
        ELSE 'OTHER_ANTIDEPRESSANT'
    END AS specific_antidepressant,
    
    -- Common antidepressants flags
    CASE WHEN bnf_code LIKE '%CITALOPRAM%' OR bnf_code LIKE '0403030C0%' THEN TRUE ELSE FALSE END AS is_citalopram,
    CASE WHEN bnf_code LIKE '%SERTRALINE%' OR bnf_code LIKE '0403030S0%' THEN TRUE ELSE FALSE END AS is_sertraline,
    CASE WHEN bnf_code LIKE '%FLUOXETINE%' OR bnf_code LIKE '0403030F0%' THEN TRUE ELSE FALSE END AS is_fluoxetine,
    CASE WHEN bnf_code LIKE '%VENLAFAXINE%' OR bnf_code LIKE '0403040W0%' THEN TRUE ELSE FALSE END AS is_venlafaxine,
    CASE WHEN bnf_code LIKE '%MIRTAZAPINE%' OR bnf_code LIKE '0403040S0%' THEN TRUE ELSE FALSE END AS is_mirtazapine,
    CASE WHEN bnf_code LIKE '%AMITRIPTYLINE%' OR bnf_code LIKE '0403010B0%' THEN TRUE ELSE FALSE END AS is_amitriptyline,
    
    -- Antidepressant class flags
    CASE WHEN bnf_code LIKE '040303%' THEN TRUE ELSE FALSE END AS is_ssri,
    CASE WHEN bnf_code LIKE '040301%' THEN TRUE ELSE FALSE END AS is_tricyclic,
    CASE WHEN bnf_code LIKE '040302%' THEN TRUE ELSE FALSE END AS is_maoi,
    CASE WHEN bnf_code LIKE '040304%' THEN TRUE ELSE FALSE END AS is_other_antidepressant,
    
    -- SNRI classification (subset of "other antidepressants")
    CASE 
        WHEN bnf_code LIKE '%VENLAFAXINE%' OR bnf_code LIKE '0403040W0%' OR
             bnf_code LIKE '%DULOXETINE%' OR bnf_code LIKE '0403040T0%'
        THEN TRUE
        ELSE FALSE
    END AS is_snri,
    
    -- First-line antidepressants (NICE guidance)
    CASE 
        WHEN bnf_code LIKE '040303%' OR  -- SSRIs
             bnf_code LIKE '%MIRTAZAPINE%' OR bnf_code LIKE '0403040S0%'  -- Mirtazapine
        THEN TRUE
        ELSE FALSE
    END AS is_first_line,
    
    -- Calculate time since order
    DATEDIFF(day, order_date, CURRENT_DATE()) AS days_since_order,
    
    -- Order recency flags (antidepressants are typically long-term therapy)
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
    {{ get_medication_orders(bnf_code='0403') }}
) base_orders
ORDER BY person_id, order_date DESC 