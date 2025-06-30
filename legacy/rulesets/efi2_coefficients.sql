CREATE OR REPLACE TABLE DATA_LAB_OLIDS_UAT.RULESETS.EFI2_COEFFICIENTS AS
WITH MODEL_COEFFICIENTS AS (
    -- Base model (eFI2) coefficients
    SELECT
        'EFI2' AS MODEL_NAME,
        'INTERCEPT' AS VARIABLE_NAME,
        NULL AS VARIABLE_CATEGORY,
        -7.254811 AS COEFFICIENT,
        NULL AS TRANSFORMED_COEFFICIENT,
        'Intercept term for eFI2 model' AS DESCRIPTION
    UNION ALL
    -- Age terms
    SELECT
        'EFI2',
        'AGE',
        'DEMOGRAPHIC',
        0.03073631,
        NULL,
        'Age coefficient'
    UNION ALL
    SELECT
        'EFI2',
        'AGE_SQUARED',
        'DEMOGRAPHIC',
        0.000002362568,
        NULL,
        'Age squared coefficient'
    UNION ALL
    SELECT
        'EFI2',
        'AGE_CUBED',
        'DEMOGRAPHIC',
        0.000001614001,
        NULL,
        'Age cubed coefficient'
    UNION ALL
    SELECT
        'EFI2',
        'AGE_INVERSE_SQUARED',
        'DEMOGRAPHIC',
        -0.3185429,
        NULL,
        'Age inverse squared coefficient'
    -- Polypharmacy terms
    UNION ALL
    SELECT
        'EFI2',
        'POLYPHARMACY',
        'DERIVED',
        0.04045506,
        NULL,
        'Polypharmacy coefficient'
    UNION ALL
    SELECT
        'EFI2',
        'POLYPHARMACY_SQUARED',
        'DERIVED',
        0.000442544,
        NULL,
        'Polypharmacy squared coefficient'
    UNION ALL
    SELECT
        'EFI2',
        'POLYPHARMACY_CUBED',
        'DERIVED',
        -0.00005374621,
        NULL,
        'Polypharmacy cubed coefficient'
    UNION ALL
    SELECT
        'EFI2',
        'POLYPHARMACY_LOG',
        'DERIVED',
        NULL,
        NULL,
        'Natural log of (Polypharmacy + 1)/10'
    UNION ALL
    SELECT
        'EFI2',
        'POLYPHARMACY_SCALED_SQUARED',
        'DERIVED',
        0.0962174,
        NULL,
        '((Polypharmacy + 1)/10)^2 coefficient'
    -- Sex terms
    UNION ALL
    SELECT
        'EFI2',
        'SEX_MALE',
        'DEMOGRAPHIC',
        0.4025955,
        NULL,
        'Male sex coefficient'
    UNION ALL
    SELECT
        'EFI2',
        'SEX_FEMALE',
        'DEMOGRAPHIC',
        -0.1535665,
        NULL,
        'Female sex coefficient'

    UNION ALL
    -- Falls model coefficients
    SELECT
        'FALLS',
        'INTERCEPT',
        NULL,
        -3.270836,
        NULL,
        'Intercept term for falls model'
    UNION ALL
    SELECT
        'FALLS',
        'AGE',
        'DEMOGRAPHIC',
        0.06817943,
        NULL,
        'Age coefficient for falls model'
    UNION ALL
    SELECT
        'FALLS',
        'POLYPHARMACY',
        'DERIVED',
        0.06615366,
        NULL,
        'Polypharmacy coefficient for falls model'
    UNION ALL
    SELECT
        'FALLS',
        'POLYPHARMACY_CUBED',
        'DERIVED',
        -0.00005374621,
        NULL,
        'Polypharmacy cubed coefficient for falls model'
    UNION ALL
    SELECT
        'FALLS',
        'SEX_MALE',
        'DEMOGRAPHIC',
        0.008719709,
        NULL,
        'Male sex coefficient for falls model'
    UNION ALL
    SELECT
        'FALLS',
        'SEX_FEMALE',
        'DEMOGRAPHIC',
        NULL,
        NULL,
        'Female sex coefficient for falls model'

    UNION ALL
    -- Care home model coefficients
    SELECT
        'CARE_HOME',
        'INTERCEPT',
        NULL,
        -10.25286,
        NULL,
        'Intercept term for care home model'
    UNION ALL
    SELECT
        'CARE_HOME',
        'AGE',
        'DEMOGRAPHIC',
        0.06817943,
        NULL,
        'Age coefficient for care home model'
    UNION ALL
    SELECT
        'CARE_HOME',
        'POLYPHARMACY',
        'DERIVED',
        0.06615366,
        NULL,
        'Polypharmacy coefficient for care home model'
    UNION ALL
    SELECT
        'CARE_HOME',
        'POLYPHARMACY_CUBED',
        'DERIVED',
        -0.00005374621,
        NULL,
        'Polypharmacy cubed coefficient for care home model'
    UNION ALL
    SELECT
        'CARE_HOME',
        'SEX_MALE',
        'DEMOGRAPHIC',
        0.008719709,
        NULL,
        'Male sex coefficient for care home model'
    UNION ALL
    SELECT
        'CARE_HOME',
        'SEX_FEMALE',
        'DEMOGRAPHIC',
        NULL,
        NULL,
        'Female sex coefficient for care home model'

    UNION ALL
    -- Mortality model coefficients
    SELECT
        'MORTALITY',
        'INTERCEPT',
        NULL,
        -7.254811,
        NULL,
        'Intercept term for mortality model'
    UNION ALL
    SELECT
        'MORTALITY',
        'AGE',
        'DEMOGRAPHIC',
        0.03073631,
        NULL,
        'Age coefficient for mortality model'
    UNION ALL
    SELECT
        'MORTALITY',
        'AGE_SQUARED',
        'DEMOGRAPHIC',
        0.000002362568,
        NULL,
        'Age squared coefficient for mortality model'
    UNION ALL
    SELECT
        'MORTALITY',
        'AGE_CUBED',
        'DEMOGRAPHIC',
        0.000001614001,
        NULL,
        'Age cubed coefficient for mortality model'
    UNION ALL
    SELECT
        'MORTALITY',
        'POLYPHARMACY',
        'DERIVED',
        0.04045506,
        NULL,
        'Polypharmacy coefficient for mortality model'
    UNION ALL
    SELECT
        'MORTALITY',
        'POLYPHARMACY_SQUARED',
        'DERIVED',
        0.000442544,
        NULL,
        'Polypharmacy squared coefficient for mortality model'
    UNION ALL
    SELECT
        'MORTALITY',
        'SEX_MALE',
        'DEMOGRAPHIC',
        0.4025955,
        NULL,
        'Male sex coefficient for mortality model'
    UNION ALL
    SELECT
        'MORTALITY',
        'SEX_FEMALE',
        'DEMOGRAPHIC',
        NULL,
        NULL,
        'Female sex coefficient for mortality model'

    UNION ALL
    -- Home care model coefficients
    SELECT
        'HOME_CARE',
        'INTERCEPT',
        NULL,
        -7.254811,
        NULL,
        'Intercept term for home care model'
    UNION ALL
    SELECT
        'HOME_CARE',
        'AGE',
        'DEMOGRAPHIC',
        0.03073631,
        NULL,
        'Age coefficient for home care model'
    UNION ALL
    SELECT
        'HOME_CARE',
        'POLYPHARMACY',
        'DERIVED',
        0.04045506,
        NULL,
        'Polypharmacy coefficient for home care model'
    UNION ALL
    SELECT
        'HOME_CARE',
        'SEX_MALE',
        'DEMOGRAPHIC',
        0.4025955,
        NULL,
        'Male sex coefficient for home care model'
    UNION ALL
    SELECT
        'HOME_CARE',
        'SEX_FEMALE',
        'DEMOGRAPHIC',
        NULL,
        NULL,
        'Female sex coefficient for home care model'

    UNION ALL
    -- Deficit coefficients from EFI2_RULES
    SELECT
        'EFI2' AS MODEL_NAME,
        E.DEFICIT AS VARIABLE_NAME,
        'DEFICIT' AS VARIABLE_CATEGORY,
        E.EFI2_COEFFICIENT AS COEFFICIENT,
        E.EFI2_TRANSFORMED_COEFFICIENT AS TRANSFORMED_COEFFICIENT,
        'eFI2 coefficient for ' || E.DEFICIT AS DESCRIPTION
    FROM DATA_LAB_OLIDS_UAT.RULESETS.EFI2_RULES AS E
    WHERE E.EFI2_COEFFICIENT IS NOT NULL

    UNION ALL
    SELECT
        'FALLS' AS MODEL_NAME,
        E.DEFICIT AS VARIABLE_NAME,
        'DEFICIT' AS VARIABLE_CATEGORY,
        E.EFI_PLUS_FALLS_COEFFICIENT AS COEFFICIENT,
        NULL AS TRANSFORMED_COEFFICIENT,
        'Falls model coefficient for ' || E.DEFICIT AS DESCRIPTION
    FROM DATA_LAB_OLIDS_UAT.RULESETS.EFI2_RULES AS E
    WHERE E.EFI_PLUS_FALLS_COEFFICIENT IS NOT NULL

    UNION ALL
    SELECT
        'CARE_HOME' AS MODEL_NAME,
        E.DEFICIT AS VARIABLE_NAME,
        'DEFICIT' AS VARIABLE_CATEGORY,
        E.EFI_PLUS_CARE_HOME_COEFFICIENT AS COEFFICIENT,
        NULL AS TRANSFORMED_COEFFICIENT,
        'Care home model coefficient for ' || E.DEFICIT AS DESCRIPTION
    FROM DATA_LAB_OLIDS_UAT.RULESETS.EFI2_RULES AS E
    WHERE E.EFI_PLUS_CARE_HOME_COEFFICIENT IS NOT NULL

    UNION ALL
    SELECT
        'MORTALITY' AS MODEL_NAME,
        E.DEFICIT AS VARIABLE_NAME,
        'DEFICIT' AS VARIABLE_CATEGORY,
        E.EFI_PLUS_MORTALITY_COEFFICIENT AS COEFFICIENT,
        NULL AS TRANSFORMED_COEFFICIENT,
        'Mortality model coefficient for ' || E.DEFICIT AS DESCRIPTION
    FROM DATA_LAB_OLIDS_UAT.RULESETS.EFI2_RULES AS E
    WHERE E.EFI_PLUS_MORTALITY_COEFFICIENT IS NOT NULL

    UNION ALL
    SELECT
        'HOME_CARE' AS MODEL_NAME,
        E.DEFICIT AS VARIABLE_NAME,
        'DEFICIT' AS VARIABLE_CATEGORY,
        E.EFI_PLUS_HOME_CARE_COEFFICIENT AS COEFFICIENT,
        NULL AS TRANSFORMED_COEFFICIENT,
        'Home care model coefficient for ' || E.DEFICIT AS DESCRIPTION
    FROM DATA_LAB_OLIDS_UAT.RULESETS.EFI2_RULES AS E
    WHERE E.EFI_PLUS_HOME_CARE_COEFFICIENT IS NOT NULL
)

SELECT DISTINCT
    MODEL_NAME,
    VARIABLE_NAME,
    VARIABLE_CATEGORY,
    COEFFICIENT,
    TRANSFORMED_COEFFICIENT,
    DESCRIPTION,
    CASE
        WHEN
            VARIABLE_CATEGORY = 'DEMOGRAPHIC'
            THEN 'Demographic variable (age, sex)'
        WHEN
            VARIABLE_CATEGORY = 'DERIVED'
            THEN 'Derived variable (polypharmacy)'
        WHEN VARIABLE_CATEGORY = 'DEFICIT' THEN 'Health deficit coefficient'
        WHEN VARIABLE_NAME = 'INTERCEPT' THEN 'Model intercept'
        ELSE 'Other coefficient'
    END AS VARIABLE_TYPE,
    CASE
        WHEN MODEL_NAME = 'EFI2' THEN 'Electronic Frailty Index 2'
        WHEN MODEL_NAME = 'FALLS' THEN 'Falls prediction model'
        WHEN
            MODEL_NAME = 'CARE_HOME'
            THEN 'Care home admission prediction model'
        WHEN MODEL_NAME = 'MORTALITY' THEN 'Mortality prediction model'
        WHEN MODEL_NAME = 'HOME_CARE' THEN 'Home care package prediction model'
    END AS MODEL_DESCRIPTION
FROM MODEL_COEFFICIENTS
ORDER BY
    CASE MODEL_NAME
        WHEN 'EFI2' THEN 1
        WHEN 'FALLS' THEN 2
        WHEN 'CARE_HOME' THEN 3
        WHEN 'MORTALITY' THEN 4
        WHEN 'HOME_CARE' THEN 5
    END,
    CASE VARIABLE_CATEGORY
        WHEN NULL THEN 1
        WHEN 'DEMOGRAPHIC' THEN 2
        WHEN 'DERIVED' THEN 3
        WHEN 'DEFICIT' THEN 4
    END,
    VARIABLE_NAME;
