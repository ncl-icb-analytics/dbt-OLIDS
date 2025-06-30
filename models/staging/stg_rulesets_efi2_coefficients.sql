-- Staging model for RULESETS.EFI2_COEFFICIENTS
-- Source: "DATA_LAB_NCL_TRAINING_TEMP".RULESETS

SELECT
    "MODEL_NAME" AS model_name,
    "VARIABLE_NAME" AS variable_name,
    "VARIABLE_CATEGORY" AS variable_category,
    "COEFFICIENT" AS coefficient,
    "TRANSFORMED_COEFFICIENT" AS transformed_coefficient,
    "DESCRIPTION" AS description,
    "VARIABLE_TYPE" AS variable_type,
    "MODEL_DESCRIPTION" AS model_description
FROM {{ source('RULESETS', 'EFI2_COEFFICIENTS') }}
