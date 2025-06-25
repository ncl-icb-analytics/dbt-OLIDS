CREATE OR REPLACE DYNAMIC TABLE DATA_LAB_NCL_TRAINING_TEMP.HEI_MIGRATION.DIM_PROG_VALPROATE_DB_SCOPE (
    -- Core Identifiers
    PERSON_ID VARCHAR, -- Unique identifier for a person
    SK_PATIENT_ID VARCHAR, -- Surrogate key for the patient
    AGE NUMBER, -- Age of the person (0-55 for this cohort)
    SEX VARCHAR, -- Sex of the person (non-'Male')
    IS_CHILD_BEARING_AGE_0_55 BOOLEAN, -- From DIM_PERSON_WOMEN_CHILD_BEARING_AGE, will be TRUE for all rows, indicating age 0-55

    -- Valproate Order Details (from INTERMEDIATE_VALPROATE_ORDERS_6M_LATEST)
    MOST_RECENT_VALPROATE_ORDER_DATE DATE, -- Date of the most recent Valproate order in the last 6 months
    VALPROATE_MEDICATION_ORDER_ID VARCHAR, -- Identifier for the most recent Valproate medication order
    VALPROATE_ORDER_MEDICATION_NAME VARCHAR, -- Name of the medication on the most recent Valproate order
    VALPROATE_PRODUCT_TERM VARCHAR, -- Specific Valproate product term from the recent order
    VALPROATE_RECENT_ORDER_COUNT NUMBER, -- Total count of Valproate orders for this person in the last 6 months
    VALPROATE_ORDER_DOSE VARCHAR, -- Dosage information from the most recent Valproate order
    VALPROATE_ORDER_QUANTITY_VALUE FLOAT, -- Numeric quantity from the most recent Valproate order
    VALPROATE_ORDER_QUANTITY_UNIT VARCHAR, -- Unit for the quantity on the most recent Valproate order
    VALPROATE_ORDER_DURATION_DAYS NUMBER -- Duration in days of the most recent Valproate prescription
)
COMMENT = 'Dimension table defining the scope for the Valproate Dashboard. Includes non-male individuals aged 0-55 who have a Valproate medication order in the last 6 months.'
TARGET_LAG = '4 hours'
REFRESH_MODE = AUTO
INITIALIZE = ON_CREATE
WAREHOUSE = NCL_ANALYTICS_XS
AS
-- Selects non-male individuals aged 0-55 who are on Valproate (order in last 6 months).
-- This cohort is defined by joining women of child-bearing age with recent Valproate orders.
-- Includes key details from their most recent Valproate prescription.
SELECT
    -- Core Identifiers
    wcba.person_id,
    pat."sk_patient_id" AS sk_patient_id,
    wcba.AGE,
    wcba.SEX,
    wcba.IS_CHILD_BEARING_AGE_0_55,

    -- Valproate Order Details
    valp.MOST_RECENT_ORDER_DATE AS MOST_RECENT_VALPROATE_ORDER_DATE,
    valp.MEDICATION_ORDER_ID AS VALPROATE_MEDICATION_ORDER_ID,
    valp.ORDER_MEDICATION_NAME AS VALPROATE_ORDER_MEDICATION_NAME,
    valp.VALPROATE_PRODUCT_TERM,
    valp.RECENT_ORDER_COUNT AS VALPROATE_RECENT_ORDER_COUNT,
    valp.ORDER_DOSE AS VALPROATE_ORDER_DOSE,
    valp.ORDER_QUANTITY_VALUE AS VALPROATE_ORDER_QUANTITY_VALUE,
    valp.ORDER_QUANTITY_UNIT AS VALPROATE_ORDER_QUANTITY_UNIT,
    valp.ORDER_DURATION_DAYS AS VALPROATE_ORDER_DURATION_DAYS

FROM
    DATA_LAB_NCL_TRAINING_TEMP.HEI_MIGRATION.DIM_PERSON_WOMEN_CHILD_BEARING_AGE wcba
INNER JOIN
    DATA_LAB_NCL_TRAINING_TEMP.HEI_MIGRATION.INTERMEDIATE_VALPROATE_ORDERS_6M_LATEST valp
    ON wcba.PERSON_ID = valp.PERSON_ID
INNER JOIN
    "Data_Store_OLIDS_Dummy"."OLIDS_MASKED"."PATIENT_PERSON" pp
    ON wcba.PERSON_ID = pp."person_id"
INNER JOIN
    "Data_Store_OLIDS_Dummy"."OLIDS_MASKED"."PATIENT" pat
    ON pp."patient_id" = pat."id"
JOIN DATA_LAB_NCL_TRAINING_TEMP.HEI_MIGRATION.DIM_PERSON_ACTIVE_PATIENTS ap
    ON wcba.PERSON_ID = ap.PERSON_ID
WHERE
    -- Ensures the cohort is restricted to the 0-55 age range for non-males as defined in DIM_PERSON_WOMEN_CHILD_BEARING_AGE.
    wcba.IS_CHILD_BEARING_AGE_0_55 = TRUE;
