{{
    config(
        materialized='table',
        tags=['dimension', 'person', 'women', 'child_bearing_age'],
        cluster_by=['person_id'])
}}

-- Person Women Child Bearing Age Dimension Table
-- Identifies non-male individuals (Female or Unknown sex) aged 55 or younger
-- Calculates boolean flags for different child-bearing age ranges

SELECT
    age.person_id,
    age.age,
    sex.sex, -- Sex is included for confirmation; filtered to be 'Female' or 'Unknown' by the WHERE clause
    -- Flag for age 12-55 inclusive: Standard demographic definition for child-bearing age
    (age.age >= 12 AND age.age <= 55) AS is_child_bearing_age_12_55,
    -- Flag for age 0-55 inclusive: Used for specific safety programs like Valproate
    -- This flag will always be TRUE for rows in this table because the WHERE clause (age <= 55) ensures it
    (age.age <= 55) AS is_child_bearing_age_0_55
FROM {{ ref('dim_person_age') }} AS age
INNER JOIN {{ ref('dim_person_sex') }} AS sex
    ON age.person_id = sex.person_id
WHERE
    -- Filter for individuals NOT identified as Male
    -- This approach ('Not male') is often used for clinical safety to be more inclusive than specifically selecting 'Female'
    sex.sex != 'Male'
    -- Exclude NULL or invalid ages first
    AND age.age IS NOT NULL
    AND age.age >= 0
    -- Further filter to include only these non-males who are aged 55 or younger
    -- ensuring they fall into at least the broader 0-55 child-bearing age definition used in this table
    AND age.age <= 55
