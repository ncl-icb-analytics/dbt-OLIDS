CREATE OR REPLACE DYNAMIC TABLE DATA_LAB_NCL_TRAINING_TEMP.HEI_MIGRATION.FCT_CLINICAL_SAFETY_ON_VALPROATE_AND_PREGNANT (
    -- Core Identifiers
    PERSON_ID VARCHAR, -- Unique identifier for a person
    SK_PATIENT_ID VARCHAR, -- Surrogate key for the patient
    AGE NUMBER, -- Age of the person (0-55 for this cohort)
    SEX VARCHAR, -- Sex of the person (non-'Male')
    IS_CHILD_BEARING_AGE_0_55 BOOLEAN, -- From DIM_PERSON_WOMEN_CHILD_BEARING_AGE, will be TRUE for all rows, indicating age 0-55

    -- Pregnancy Details (from FCT_PERSON_CURRENT_PREGNANT)
    LATEST_PREG_COD_DATE DATE, -- Latest date of a pregnancy code (PREG_COD)
    LATEST_PREGDEL_COD_DATE DATE, -- Latest date of a pregnancy ended/delivery code (PREGDEL_COD)
    ALL_PREG_OBSERVATION_IDS ARRAY, -- Array of all observation IDs related to pregnancy status
    ALL_PREG_CONCEPT_CODES ARRAY, -- Array of all pregnancy-related concept codes
    ALL_PREG_CONCEPT_DISPLAYS ARRAY, -- Array of display terms for pregnancy-related codes
    ALL_PREG_SOURCE_CLUSTER_IDS ARRAY, -- Array of source cluster IDs (PREG_COD, PREGDEL_COD)

    -- Valproate Order Details (from INTERMEDIATE_VALPROATE_ORDERS_6M_LATEST)
    MOST_RECENT_VALPROATE_ORDER_DATE DATE, -- Date of the most recent Valproate order in the last 6 months
    VALPROATE_MEDICATION_ORDER_ID VARCHAR, -- Identifier for the most recent Valproate medication order
    VALPROATE_MEDICATION_STATEMENT_ID VARCHAR, -- Identifier for the linked statement of the most recent Valproate order
    VALPROATE_ORDER_MEDICATION_NAME VARCHAR, -- Name of the medication on the most recent Valproate order
    VALPROATE_ORDER_DOSE VARCHAR, -- Dosage information from the most recent Valproate order
    VALPROATE_ORDER_QUANTITY_VALUE FLOAT, -- Numeric quantity from the most recent Valproate order
    VALPROATE_ORDER_QUANTITY_UNIT VARCHAR, -- Unit for the quantity on the most recent Valproate order
    VALPROATE_ORDER_DURATION_DAYS NUMBER, -- Duration in days of the most recent Valproate prescription
    VALPROATE_STATEMENT_MEDICATION_NAME VARCHAR, -- Medication name from the statement of the most recent Valproate order
    VALPROATE_MAPPED_CONCEPT_CODE VARCHAR, -- Mapped concept code for the Valproate product from the recent order
    VALPROATE_MAPPED_CONCEPT_DISPLAY VARCHAR, -- Display term for the mapped Valproate concept from the recent order
    VALPROATE_PRODUCT_TERM VARCHAR, -- Specific Valproate product term from the recent order
    VALPROATE_RECENT_ORDER_COUNT NUMBER -- Total count of Valproate orders for this person in the last 6 months
)
COMMENT = 'Clinical safety fact table identifying non-male individuals aged 0-55 who are currently pregnant AND have a recent (last 6 months) Valproate medication order. Highlights a high-risk cohort.'
TARGET_LAG = '4 hours'
REFRESH_MODE = AUTO
INITIALIZE = ON_CREATE
WAREHOUSE = NCL_ANALYTICS_XS
AS
-- Selects individuals meeting three key criteria for clinical safety monitoring:
-- 1. Non-male and aged 0-55 (from DIM_PERSON_WOMEN_CHILD_BEARING_AGE where IS_CHILD_BEARING_AGE_0_55 = TRUE).
-- 2. Currently pregnant (from FCT_PERSON_CURRENT_PREGNANT).
-- 3. Have a recent Valproate order (from INTERMEDIATE_VALPROATE_ORDERS_6M_LATEST).
SELECT
    -- Core Identifiers from joined tables
    wcba.PERSON_ID,
    preg.SK_PATIENT_ID, -- SK_PATIENT_ID from FCT_PERSON_CURRENT_PREGNANT
    wcba.AGE,
    wcba.SEX,
    wcba.IS_CHILD_BEARING_AGE_0_55,

    -- Pregnancy Details
    preg.LATEST_PREG_COD_DATE,
    preg.LATEST_PREGDEL_COD_DATE,
    preg.ALL_PREG_OBSERVATION_IDS,
    preg.ALL_PREG_CONCEPT_CODES,
    preg.ALL_PREG_CONCEPT_DISPLAYS,
    preg.ALL_PREG_SOURCE_CLUSTER_IDS,

    -- Valproate Order Details
    valp.MOST_RECENT_ORDER_DATE AS MOST_RECENT_VALPROATE_ORDER_DATE,
    valp.MEDICATION_ORDER_ID AS VALPROATE_MEDICATION_ORDER_ID,
    valp.MEDICATION_STATEMENT_ID AS VALPROATE_MEDICATION_STATEMENT_ID,
    valp.ORDER_MEDICATION_NAME AS VALPROATE_ORDER_MEDICATION_NAME,
    valp.ORDER_DOSE AS VALPROATE_ORDER_DOSE,
    valp.ORDER_QUANTITY_VALUE AS VALPROATE_ORDER_QUANTITY_VALUE,
    valp.ORDER_QUANTITY_UNIT AS VALPROATE_ORDER_QUANTITY_UNIT,
    valp.ORDER_DURATION_DAYS AS VALPROATE_ORDER_DURATION_DAYS,
    valp.STATEMENT_MEDICATION_NAME AS VALPROATE_STATEMENT_MEDICATION_NAME,
    valp.MAPPED_CONCEPT_CODE AS VALPROATE_MAPPED_CONCEPT_CODE,
    valp.MAPPED_CONCEPT_DISPLAY AS VALPROATE_MAPPED_CONCEPT_DISPLAY,
    valp.VALPROATE_PRODUCT_TERM,
    valp.RECENT_ORDER_COUNT AS VALPROATE_RECENT_ORDER_COUNT

FROM
    DATA_LAB_NCL_TRAINING_TEMP.HEI_MIGRATION.DIM_PERSON_WOMEN_CHILD_BEARING_AGE wcba
JOIN
    DATA_LAB_NCL_TRAINING_TEMP.HEI_MIGRATION.FCT_PERSON_CURRENT_PREGNANT preg
    ON wcba.PERSON_ID = preg.PERSON_ID
JOIN
    DATA_LAB_NCL_TRAINING_TEMP.HEI_MIGRATION.INTERMEDIATE_VALPROATE_ORDERS_6M_LATEST valp
    ON preg.PERSON_ID = valp.PERSON_ID -- Could also join on wcba.PERSON_ID, as PERSON_ID is the key across these tables for an individual
WHERE
    -- Ensures the cohort is restricted to women of child-bearing age (0-55) as defined in the dimension.
    -- This filter is technically redundant if FCT_PERSON_CURRENT_PREGNANT already sources from a similarly filtered DIM_PERSON_WOMEN_CHILD_BEARING_AGE,
    -- but kept for explicit clarity of the cohort definition at this table's level.
    wcba.IS_CHILD_BEARING_AGE_0_55 = TRUE; 