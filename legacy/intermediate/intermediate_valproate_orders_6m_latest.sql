CREATE OR REPLACE DYNAMIC TABLE DATA_LAB_NCL_TRAINING_TEMP.HEI_MIGRATION.INTERMEDIATE_VALPROATE_ORDERS_6M_LATEST (
    PERSON_ID VARCHAR, -- Unique identifier for a person
    MOST_RECENT_ORDER_DATE DATE, -- Date of the most recent Valproate order in the last 6 months
    MEDICATION_ORDER_ID VARCHAR, -- Identifier for the medication order
    MEDICATION_STATEMENT_ID VARCHAR, -- Identifier for the linked medication statement, if any
    ORDER_MEDICATION_NAME VARCHAR, -- Name of the medication as on the order
    ORDER_DOSE VARCHAR, -- Dosage information from the order
    ORDER_QUANTITY_VALUE FLOAT, -- Numeric value of the quantity ordered
    ORDER_QUANTITY_UNIT VARCHAR, -- Unit for the quantity ordered (e.g., tablets, ml)
    ORDER_DURATION_DAYS NUMBER(38,0), -- Duration of the prescription in days
    STATEMENT_MEDICATION_NAME VARCHAR, -- Name of the medication from the linked statement, if available
    MAPPED_CONCEPT_CODE VARCHAR, -- Mapped concept code for the Valproate product
    MAPPED_CONCEPT_DISPLAY VARCHAR, -- Display term for the mapped Valproate concept
    MAPPED_CONCEPT_ID VARCHAR, -- Identifier for the mapped concept
    VALPROATE_PRODUCT_TERM VARCHAR, -- Specific Valproate product term identified
    MATCHED_ON_NAME BOOLEAN, -- Flag: TRUE if the upstream record was matched based on medication name
    MATCHED_ON_CONCEPT_ID BOOLEAN, -- Flag: TRUE if the upstream record was matched based on concept ID
    RECENT_ORDER_COUNT NUMBER -- Total count of Valproate orders for this person in the last 6 months
)
COMMENT = 'Intermediate table capturing details of the single most recent Valproate medication order for each person within the last 6 months. Also includes a count of all their Valproate orders in this period.'
TARGET_LAG = '4 hours' -- Align with or make slightly longer than the source INTERMEDIATE_VALPROATE_ORDERS_ALL table's lag
REFRESH_MODE = AUTO
INITIALIZE = ON_CREATE
WAREHOUSE = NCL_ANALYTICS_XS -- Adjust warehouse as needed for performance and cost
AS
-- Selects and flattens details for Valproate orders placed in the last 6 months.
-- For each person, it identifies the single most recent order and counts all their orders in this period.
SELECT
    vmoa.PERSON_ID,
    vmoa.ORDER_CLINICAL_EFFECTIVE_DATE::DATE AS MOST_RECENT_ORDER_DATE,
    -- Details from the most recent Valproate order:
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
    -- Counts the total number of Valproate orders for this person within the last 6 months.
    COUNT(*) OVER (PARTITION BY vmoa.PERSON_ID) as RECENT_ORDER_COUNT
FROM
    DATA_LAB_NCL_TRAINING_TEMP.HEI_MIGRATION.INTERMEDIATE_VALPROATE_ORDERS_ALL vmoa
WHERE
    -- Filters for Valproate orders with a clinical effective date within the last 6 months (inclusive of today).
    vmoa.ORDER_CLINICAL_EFFECTIVE_DATE >= DATEADD(month, -6, CURRENT_DATE())
    AND vmoa.ORDER_CLINICAL_EFFECTIVE_DATE <= CURRENT_DATE()
-- Filters the results to keep only the single most recent order for each person based on the ORDER_CLINICAL_EFFECTIVE_DATE.
-- If multiple orders exist on the same most recent date for a person, one is chosen arbitrarily by ROW_NUMBER().
QUALIFY ROW_NUMBER() OVER (PARTITION BY vmoa.PERSON_ID ORDER BY vmoa.ORDER_CLINICAL_EFFECTIVE_DATE DESC) = 1;
