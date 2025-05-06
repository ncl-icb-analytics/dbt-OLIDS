CREATE OR REPLACE DYNAMIC TABLE DATA_LAB_NCL_TRAINING_TEMP.HEI_MIGRATION.INTERMEDIATE_VALPROATE_ORDERS_6M_LATEST (
    -- Identifiers
    PERSON_ID VARCHAR,
    -- Details from the MOST RECENT order within the last 6 months
    MOST_RECENT_ORDER_DATE DATE,
    MEDICATION_ORDER_ID VARCHAR,
    MEDICATION_STATEMENT_ID VARCHAR,
    ORDER_MEDICATION_NAME VARCHAR,
    ORDER_DOSE VARCHAR,
    ORDER_QUANTITY_VALUE FLOAT,
    ORDER_QUANTITY_UNIT VARCHAR,
    ORDER_DURATION_DAYS NUMBER(38,0),
    STATEMENT_MEDICATION_NAME VARCHAR, -- From linked statement
    MAPPED_CONCEPT_CODE VARCHAR,
    MAPPED_CONCEPT_DISPLAY VARCHAR,
    MAPPED_CONCEPT_ID VARCHAR,
    VALPROATE_PRODUCT_TERM VARCHAR,
    MATCHED_ON_NAME BOOLEAN, -- How the upstream record was matched
    MATCHED_ON_CONCEPT_ID BOOLEAN, -- How the upstream record was matched
    -- Count of qualifying orders in the last 6 months for this person
    RECENT_ORDER_COUNT NUMBER
)
TARGET_LAG = '4 hours' -- Align with or make slightly longer than the source table's lag
REFRESH_MODE = AUTO
INITIALIZE = ON_CREATE
WAREHOUSE = NCL_ANALYTICS_XS -- Adjust as needed
AS
-- Select details from the single most recent order per person within the last 6 months
SELECT
    vmoa.PERSON_ID,
    vmoa.ORDER_CLINICAL_EFFECTIVE_DATE::DATE AS MOST_RECENT_ORDER_DATE,
    -- Include details from the most recent order
    vmoa.MEDICATION_ORDER_ID,
    vmoa.MEDICATION_STATEMENT_ID,
    vmoa.ORDER_MEDICATION_NAME,
    vmoa.ORDER_DOSE,
    vmoa.ORDER_QUANTITY_VALUE,
    vmoa.ORDER_QUANTITY_UNIT,
    vmoa.ORDER_DURATION_DAYS,
    vmoa.STATEMENT_MEDICATION_NAME,
    vmoa.MAPPED_CONCEPT_CODE,
    vmoa.MAPPED_CONCEPT_DISPLAY,
    vmoa.MAPPED_CONCEPT_ID,
    vmoa.VALPROATE_PRODUCT_TERM,
    vmoa.MATCHED_ON_NAME,
    vmoa.MATCHED_ON_CONCEPT_ID,
    -- Count the total number of recent orders for this person using a window function
    COUNT(*) OVER (PARTITION BY vmoa.PERSON_ID) as RECENT_ORDER_COUNT
FROM
    DATA_LAB_NCL_TRAINING_TEMP.HEI_MIGRATION.INTERMEDIATE_VALPROATE_ORDERS_ALL vmoa
WHERE
    -- Filter for orders where the clinical effective date is on or after
    -- the date exactly 6 months before the current date.
    vmoa.ORDER_CLINICAL_EFFECTIVE_DATE >= DATEADD(month, -6, CURRENT_DATE())
    AND vmoa.ORDER_CLINICAL_EFFECTIVE_DATE <= CURRENT_DATE()
-- Keep only the single most recent order (rn=1) for each person using QUALIFY
QUALIFY ROW_NUMBER() OVER (PARTITION BY vmoa.PERSON_ID ORDER BY vmoa.ORDER_CLINICAL_EFFECTIVE_DATE DESC) = 1;