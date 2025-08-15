-- Staging model for reference.EFI2_COEFFICIENTS
-- Source: "DATA_LAB_OLIDS_UAT"."REFERENCE"
-- Description: Reference data including terminologies, rulesets, and population health lookups

select
    "MODEL_NAME" as model_name,
    "VARIABLE_NAME" as variable_name,
    "VARIABLE_CATEGORY" as variable_category,
    "COEFFICIENT" as coefficient,
    "TRANSFORMED_COEFFICIENT" as transformed_coefficient,
    "DESCRIPTION" as description,
    "VARIABLE_TYPE" as variable_type,
    "MODEL_DESCRIPTION" as model_description
from {{ source('reference', 'EFI2_COEFFICIENTS') }}
