-- Staging model for RULESETS.EFI2_COEFFICIENTS
-- Source: "DATA_LAB_NCL_TRAINING_TEMP".RULESETS

select
    "MODEL_NAME" as model_name,
    "VARIABLE_NAME" as variable_name,
    "VARIABLE_CATEGORY" as variable_category,
    "COEFFICIENT" as coefficient,
    "TRANSFORMED_COEFFICIENT" as transformed_coefficient,
    "DESCRIPTION" as description,
    "VARIABLE_TYPE" as variable_type,
    "MODEL_DESCRIPTION" as model_description
from {{ source('RULESETS', 'EFI2_COEFFICIENTS') }}
