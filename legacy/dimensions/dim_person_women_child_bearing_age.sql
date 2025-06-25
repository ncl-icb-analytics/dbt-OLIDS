CREATE OR REPLACE DYNAMIC TABLE DATA_LAB_NCL_TRAINING_TEMP.HEI_MIGRATION.DIM_PERSON_WOMEN_CHILD_BEARING_AGE (
    PERSON_ID VARCHAR, -- Unique identifier for a person
    AGE NUMBER, -- Age of the person
    SEX VARCHAR, -- Sex of the person, will be 'Female' or 'Unknown' due to WHERE clause filtering
    IS_CHILD_BEARING_AGE_12_55 BOOLEAN, -- Flag: TRUE if age is 12-55 inclusive (standard demographic range)
    IS_CHILD_BEARING_AGE_0_55 BOOLEAN  -- Flag: TRUE if age is 0-55 inclusive (wider range, e.g., for Valproate safety programs)
)
COMMENT = 'Dimension table identifying individuals who are not male and are aged 55 or younger. It includes flags for standard child-bearing age ranges.'
TARGET_LAG = '4 hours'
REFRESH_MODE = AUTO
INITIALIZE = ON_CREATE
WAREHOUSE = NCL_ANALYTICS_XS
AS
-- Selects non-male individuals (Female or Unknown sex) aged 55 or younger.
-- Calculates boolean flags for different child-bearing age ranges.
SELECT
    age.PERSON_ID,
    age.AGE,
    sex.SEX, -- Sex is included for confirmation; it's filtered to be 'Female' or 'Unknown' by the WHERE clause.
    -- Flag for age 12-55 inclusive: Standard demographic definition for child-bearing age.
    (age.AGE >= 12 AND age.AGE <= 55) AS IS_CHILD_BEARING_AGE_12_55,

    -- Flag for age 0-55 inclusive: Used for specific safety programs like Valproate.
    -- This flag will always be TRUE for rows in this table because the WHERE clause (age.AGE <= 55) ensures it.
    (age.AGE <= 55) AS IS_CHILD_BEARING_AGE_0_55
FROM
    -- Source table containing calculated age information for each person.
    DATA_LAB_NCL_TRAINING_TEMP.HEI_MIGRATION.DIM_PERSON_AGE age
INNER JOIN
    -- Source table containing resolved sex for each person.
    DATA_LAB_NCL_TRAINING_TEMP.HEI_MIGRATION.DIM_PERSON_SEX sex ON age.PERSON_ID = sex.PERSON_ID
WHERE
    -- Filter for individuals NOT identified as Male.
    -- This approach ('Not male') is often used for clinical safety to be more inclusive than specifically selecting 'Female'.
    sex.SEX != 'Male'
    -- Further filter to include only these non-males who are aged 55 or younger,
    -- ensuring they fall into at least the broader 0-55 child-bearing age definition used in this table.
    AND age.AGE <= 55;
