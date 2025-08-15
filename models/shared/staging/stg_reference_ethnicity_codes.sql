-- Staging model for reference.ETHNICITY_CODES
-- Source: "DATA_LAB_OLIDS_UAT"."REFERENCE"
-- Description: Reference data including terminologies, rulesets, and population health lookups

select
    "CODE" as code,
    "TERM" as term,
    "CATEGORY" as category,
    "SUBCATEGORY" as subcategory,
    "GRANULAR" as granular,
    "DEPRIORITISE_FLAG" as deprioritise_flag,
    "PREFERENCE_RANK" as preference_rank,
    "CATEGORY_SORT" as category_sort,
    "DISPLAY_SORT_KEY" as display_sort_key
from {{ source('reference', 'ETHNICITY_CODES') }}
