-- Intermediate model for LTC LCS AF Observations
-- Collects all AF-relevant observations needed for all AF case finding measures

-- This intermediate fetches all AF-relevant observations for both AF_61 and AF_62 case finding measures
{{ get_observations(
    cluster_ids="'AF_OBSERVATIONS','AF_EXCLUSIONS','DEEP_VEIN_THROMBOSIS','ATRIAL_FLUTTER','ATRIAL_FIBRILLATION_61_EXCLUSIONS','LCS_PULSE_RATE','LCS_PULSE_RHYTHM'",
    source='LTC_LCS'
) }}
