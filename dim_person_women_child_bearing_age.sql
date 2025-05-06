CREATE OR REPLACE DYNAMIC TABLE DATA_LAB_NCL_TRAINING_TEMP.HEI_MIGRATION.DIM_PERSON_WOMEN_CHILD_BEARING_AGE (
    PERSON_ID VARCHAR,
    AGE NUMBER,
    SEX VARCHAR, -- Sex from DIM_PERSON_SEX ('Female' or 'Unknown' based on WHERE clause)
    IS_CHILD_BEARING_AGE_12_55 BOOLEAN, -- Standard demographic range (12-55 inclusive)
    IS_CHILD_BEARING_AGE_0_55 BOOLEAN  -- Range used for Valproate PPP (0-55 inclusive)
)
TARGET_LAG = '4 hours'
REFRESH_MODE = AUTO
INITIALIZE = ON_CREATE
WAREHOUSE = NCL_ANALYTICS_XS
AS
SELECT
    age.PERSON_ID,
    age.AGE,
    sex.SEX, -- Included for confirmation, will be 'Female' or 'Unknown'
    -- Flag for age 12-55 inclusive: Standard demographic definition
    (age.AGE >= 12 AND age.AGE <= 55) AS IS_CHILD_BEARING_AGE_12_55,
    
    -- Flag for age 0-55 inclusive: Used for Valproate safety programs
    -- This flag will always be TRUE for rows included in this table due to the WHERE clause below
    (age.AGE <= 55) AS IS_CHILD_BEARING_AGE_0_55
FROM
    -- Source table containing calculated age information
    DATA_LAB_NCL_TRAINING_TEMP.HEI_MIGRATION.DIM_PERSON_AGE age
INNER JOIN
    -- Source table containing resolved sex (using hardcoded logic currently)
    DATA_LAB_NCL_TRAINING_TEMP.HEI_MIGRATION.DIM_PERSON_SEX sex ON age.PERSON_ID = sex.PERSON_ID
WHERE
    -- Filter for individuals NOT identified as Male (includes 'Female' and 'Unknown')
    -- Not male is used from a clinical safety perspective as more robust than specifically including Female.
    sex.SEX != 'Male'
    -- AND only include non-males who are aged 55 or younger (i.e., fall into at least the 0-55 group)
    AND age.AGE <= 55;

