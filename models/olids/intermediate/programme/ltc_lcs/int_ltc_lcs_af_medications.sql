-- Intermediate model for LTC LCS AF Medications
-- Collects all AF-relevant medications needed for all AF case finding measures
-- Applies different time windows: 3 months for standard meds, 6 months for anticoagulants + protamine

WITH all_af_medications AS (
    {{ get_medication_orders(
        cluster_id="'ORANTICOAG_2.8.2','DRUGS_USED_IN_AF','DIGOXIN','CARDIAC_GLYCOSIDES','PROTAMINE_DRUGS'",
        source="LTC_LCS"
    ) }}
)
SELECT 
    *,
    CASE 
        WHEN cluster_id IN ('ORANTICOAG_2.8.2', 'PROTAMINE_DRUGS') 
            AND order_date >= dateadd(MONTH, -6, current_date())
        THEN TRUE
        WHEN cluster_id IN ('DRUGS_USED_IN_AF', 'DIGOXIN', 'CARDIAC_GLYCOSIDES')
            AND order_date >= dateadd(MONTH, -3, current_date())
        THEN TRUE
        ELSE FALSE
    END AS is_active_medication
FROM all_af_medications
WHERE 
    (cluster_id IN ('ORANTICOAG_2.8.2', 'PROTAMINE_DRUGS') 
        AND order_date >= dateadd(MONTH, -6, current_date()))
    OR 
    (cluster_id IN ('DRUGS_USED_IN_AF', 'DIGOXIN', 'CARDIAC_GLYCOSIDES')
        AND order_date >= dateadd(MONTH, -3, current_date()))
