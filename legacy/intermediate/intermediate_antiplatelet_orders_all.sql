CREATE OR REPLACE DYNAMIC TABLE DATA_LAB_OLIDS_UAT.HEI_MIGRATION.INTERMEDIATE_ANTIPLATELET_ORDERS_ALL (
    PERSON_ID VARCHAR, -- Unique identifier for the person
    SK_PATIENT_ID VARCHAR, -- Surrogate key for the patient
    MEDICATION_ORDER_ID VARCHAR, -- Unique identifier for the medication order
    MEDICATION_STATEMENT_ID VARCHAR, -- Unique identifier for the medication statement
    ORDER_DATE DATE, -- Date the medication was ordered
    ORDER_MEDICATION_NAME VARCHAR, -- Name of the medication as recorded in the order
    ORDER_DOSE VARCHAR, -- Dose of the medication as recorded in the order
    ORDER_QUANTITY_VALUE NUMBER, -- Quantity value of the medication order
    ORDER_QUANTITY_UNIT VARCHAR, -- Unit of the quantity (e.g., tablets, ml)
    ORDER_DURATION_DAYS NUMBER, -- Duration of the medication order in days
    STATEMENT_MEDICATION_NAME VARCHAR, -- Name of the medication as recorded in the statement
    MAPPED_CONCEPT_CODE VARCHAR, -- The mapped concept code for the medication
    MAPPED_CONCEPT_DISPLAY VARCHAR, -- The display term for the mapped concept code
    BNF_CODE VARCHAR, -- The BNF code from BNF_LATEST
    BNF_NAME VARCHAR, -- The BNF name from BNF_LATEST
    RECENT_ORDER_COUNT NUMBER -- Count of orders in the last 6 months
)
COMMENT = 'Intermediate table containing all antiplatelet medication orders (BNF section 2.9). Includes orders for Aspirin, Clopidogrel, Prasugrel, Ticagrelor, and Dipyridamole.'
TARGET_LAG = '4 hours'
REFRESH_MODE = AUTO
INITIALIZE = ON_CREATE
WAREHOUSE = NCL_ANALYTICS_XS
AS
WITH BaseAntiplateletOrders AS (
    -- Get all medication orders for antiplatelets
    SELECT
        mo."id" AS MEDICATION_ORDER_ID,
        ms."id" AS MEDICATION_STATEMENT_ID,
        PP."person_id" AS PERSON_ID,
        P."sk_patient_id" AS SK_PATIENT_ID,
        mo."clinical_effective_date"::DATE AS ORDER_DATE,
        mo."medication_name" AS ORDER_MEDICATION_NAME,
        mo."dose" AS ORDER_DOSE,
        mo."quantity_value" AS ORDER_QUANTITY_VALUE,
        mo."quantity_unit" AS ORDER_QUANTITY_UNIT,
        mo."duration_days" AS ORDER_DURATION_DAYS,
        ms."medication_name" AS STATEMENT_MEDICATION_NAME,
        MC.CONCEPT_CODE AS MAPPED_CONCEPT_CODE,
        MC.CODE_DESCRIPTION AS MAPPED_CONCEPT_DISPLAY,
        bnf.BNF_CODE,
        bnf.BNF_NAME
    FROM "Data_Store_OLIDS_Dummy"."OLIDS_MASKED"."MEDICATION_STATEMENT" ms
    JOIN "Data_Store_OLIDS_Dummy"."OLIDS_MASKED"."MEDICATION_ORDER" mo
        ON ms."id" = mo."medication_statement_id"
    JOIN DATA_LAB_OLIDS_UAT.REFERENCE.MAPPED_CONCEPTS MC
        ON ms."medication_statement_core_concept_id" = MC.SOURCE_CODE_ID
    JOIN DATA_LAB_OLIDS_UAT.REFERENCE.BNF_LATEST bnf
        ON MC.CONCEPT_CODE = bnf.SNOMED_CODE
    JOIN "Data_Store_OLIDS_Dummy"."OLIDS_MASKED"."PATIENT_PERSON" PP
        ON mo."patient_id" = PP."patient_id"
    JOIN "Data_Store_OLIDS_Dummy"."OLIDS_MASKED"."PATIENT" P
        ON mo."patient_id" = P."id"
    WHERE bnf.BNF_CODE LIKE '0209%' -- Antiplatelets
),
OrderCounts AS (
    -- Counts the number of orders per person in the last 6 months
    SELECT
        PERSON_ID,
        COUNT(*) as RECENT_ORDER_COUNT
    FROM BaseAntiplateletOrders
    WHERE ORDER_DATE >= DATEADD(month, -6, CURRENT_DATE())
    GROUP BY PERSON_ID
)
-- Final selection combining order details with the count
SELECT
    bso.*,
    COALESCE(oc.RECENT_ORDER_COUNT, 0) as RECENT_ORDER_COUNT
FROM BaseAntiplateletOrders bso
LEFT JOIN OrderCounts oc
    ON bso.PERSON_ID = oc.PERSON_ID;
