CREATE OR REPLACE DYNAMIC TABLE DATA_LAB_NCL_TRAINING_TEMP.HEI_MIGRATION.INTERMEDIATE_PPI_ORDERS_ALL (
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
    SPECIFIC_AGENT VARCHAR, -- Specific PPI agent (ESOMEPRAZOLE, LANSOPRAZOLE, OMEPRAZOLE, PANTOPRAZOLE, RABEPRAZOLE)
    RECENT_ORDER_COUNT NUMBER -- Count of orders in the last 6 months
)
COMMENT = 'Intermediate table containing all proton pump inhibitor (PPI) medication orders (BNF section 1.3.5). Includes:
- Esomeprazole (BNF 0103050E0)
- Helicobacter pylori eradication therapy (BNF 0103050A0)
- Lansoprazole (BNF 0103050L0)
- Omeprazole (BNF 0103050P0)
- Pantoprazole (BNF 0103050R0)
- Rabeprazole sodium (BNF 0103050T0)'
TARGET_LAG = '4 hours'
REFRESH_MODE = AUTO
INITIALIZE = ON_CREATE
WAREHOUSE = NCL_ANALYTICS_XS
AS
WITH BasePPIOrders AS (
    -- Get all medication orders for PPIs
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
        bnf.BNF_NAME,
        CASE
            WHEN bnf.BNF_CODE LIKE '0103050E%' THEN 'ESOMEPRAZOLE'
            WHEN bnf.BNF_CODE LIKE '0103050A%' THEN 'H_PYLORI_ERADICATION'
            WHEN bnf.BNF_CODE LIKE '0103050L%' THEN 'LANSOPRAZOLE'
            WHEN bnf.BNF_CODE LIKE '0103050P%' THEN 'OMEPRAZOLE'
            WHEN bnf.BNF_CODE LIKE '0103050R%' THEN 'PANTOPRAZOLE'
            WHEN bnf.BNF_CODE LIKE '0103050T%' THEN 'RABEPRAZOLE'
            ELSE 'OTHER_PPI'
        END AS SPECIFIC_AGENT
    FROM "Data_Store_OLIDS_Dummy"."OLIDS_MASKED"."MEDICATION_STATEMENT" ms
    JOIN "Data_Store_OLIDS_Dummy"."OLIDS_MASKED"."MEDICATION_ORDER" mo
        ON ms."id" = mo."medication_statement_id"
    JOIN DATA_LAB_NCL_TRAINING_TEMP.CODESETS.MAPPED_CONCEPTS MC
        ON ms."medication_statement_core_concept_id" = MC.SOURCE_CODE_ID
    JOIN DATA_LAB_NCL_TRAINING_TEMP.CODESETS.BNF_LATEST bnf
        ON MC.CONCEPT_CODE = bnf.SNOMED_CODE
    JOIN "Data_Store_OLIDS_Dummy"."OLIDS_MASKED"."PATIENT_PERSON" PP
        ON mo."patient_id" = PP."patient_id"
    JOIN "Data_Store_OLIDS_Dummy"."OLIDS_MASKED"."PATIENT" P
        ON mo."patient_id" = P."id"
    WHERE bnf.BNF_CODE LIKE '0103050%' -- All PPIs
),
OrderCounts AS (
    -- Counts the number of orders per person in the last 6 months
    SELECT
        PERSON_ID,
        COUNT(*) as RECENT_ORDER_COUNT
    FROM BasePPIOrders
    WHERE ORDER_DATE >= DATEADD(month, -6, CURRENT_DATE())
    GROUP BY PERSON_ID
)
-- Final selection combining order details with the count
SELECT
    bso.*,
    COALESCE(oc.RECENT_ORDER_COUNT, 0) as RECENT_ORDER_COUNT
FROM BasePPIOrders bso
LEFT JOIN OrderCounts oc
    ON bso.PERSON_ID = oc.PERSON_ID;
