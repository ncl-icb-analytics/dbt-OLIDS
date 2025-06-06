-- Staging model for CODESETS.ETHNICITY_CODES
-- Source: "DATA_LAB_NCL_TRAINING_TEMP".CODESETS

select
    "CODE" as code,
    "TERM" as term,
    "CATEGORY" as category,
    "SUBCATEGORY" as subcategory,
    "GRANULAR" as granular
from {{ source('CODESETS', 'ETHNICITY_CODES') }}
