{{ config(
    materialized='table') }}

-- Intermediate model for ethnicity observations for LTC LCS diabetes case finding
-- Used for BAME and excluded ethnicity classifications in diabetes screening

{{ get_observations(
    cluster_ids="'ETHNICITY_BAME', 'ETHNICITY_WHITE_BRITISH', 'DIABETES_EXCLUDED_ETHNICITY'",
    source='LTC_LCS'
) }}
