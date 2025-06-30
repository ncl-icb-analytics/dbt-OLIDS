-- Intermediate model for LTC LCS AF Medications
-- Collects all AF-relevant medications needed for all AF case finding measures

-- Use the macro to fetch all medication orders for AF clusters
-- (Add or adjust cluster_ids as needed for all AF-relevant meds)

{{ get_medication_orders(
    cluster_id="'ORAL_ANTICOAGULANT_2_8_2','AF_MEDICATIONS','DIGOXIN_MEDICATIONS','CARDIAC_GLYCOSIDES'",
    source="LTC_LCS"
) }}
