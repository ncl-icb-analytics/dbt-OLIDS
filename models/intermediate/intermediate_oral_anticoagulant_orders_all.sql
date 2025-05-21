CREATE OR REPLACE DYNAMIC TABLE DATA_LAB_NCL_TRAINING_TEMP.HEI_MIGRATION.INTERMEDIATE_ORAL_ANTICOAGULANT_ORDERS_ALL (
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
    ANTICOAGULANT_TYPE VARCHAR, -- Type of anticoagulant (DOAC, VKA, OTHER)
    SPECIFIC_AGENT VARCHAR, -- Specific anticoagulant agent (APIXABAN, DABIGATRAN, EDOXABAN, RIVAROXABAN, WARFARIN, ACENOCOUMAROL, PHENINDIONE, PHENPROCOUMON, PENTOSAN, OTHER)
    RECENT_ORDER_COUNT NUMBER -- Count of orders in the last 6 months
)
COMMENT = 'Intermediate table containing all oral anticoagulant medication orders (BNF section 2.8.2). Includes:
- Direct Oral Anticoagulants (DOACs):
  * Apixaban (BNF 0208020Z0)
  * Dabigatran etexilate (BNF 0208020X0)
  * Edoxaban (BNF 0208020AA)
  * Rivaroxaban (BNF 0208020Y0)
- Vitamin K Antagonists (VKAs):
  * Warfarin sodium (BNF 0208020V0) - Most commonly used VKA
  * Acenocoumarol (BNF 0208020H0) - Less commonly used
  * Phenindione (BNF 0208020N0) - Rarely used
  * Phenprocoumon (BNF 0208020S0) - Rarely used
- Other agents:
  * Pentosan polysulfate sodium (BNF 0208020I0)
  * Other oral anticoagulant preparations (BNF 020802000)
Excludes INR blood testing reagents (BNF 0208020W0)'
TARGET_LAG = '4 hours'
REFRESH_MODE = AUTO
INITIALIZE = ON_CREATE
WAREHOUSE = NCL_ANALYTICS_XS
AS
WITH BaseAnticoagulantOrders AS (
    -- Get all medication orders for oral anticoagulants
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
            -- DOACs
            WHEN bnf.BNF_CODE LIKE '0208020Z%' THEN 'DOAC'
            WHEN bnf.BNF_CODE LIKE '0208020X%' THEN 'DOAC'
            WHEN bnf.BNF_CODE LIKE '0208020AA%' THEN 'DOAC'
            WHEN bnf.BNF_CODE LIKE '0208020Y%' THEN 'DOAC'
            -- VKAs
            WHEN bnf.BNF_CODE LIKE '0208020V%' THEN 'VKA'
            WHEN bnf.BNF_CODE LIKE '0208020H%' THEN 'VKA'
            WHEN bnf.BNF_CODE LIKE '0208020N%' THEN 'VKA'
            WHEN bnf.BNF_CODE LIKE '0208020S%' THEN 'VKA'
            ELSE 'OTHER'
        END AS ANTICOAGULANT_TYPE,
        CASE 
            WHEN bnf.BNF_CODE LIKE '0208020Z%' THEN 'APIXABAN'
            WHEN bnf.BNF_CODE LIKE '0208020X%' THEN 'DABIGATRAN'
            WHEN bnf.BNF_CODE LIKE '0208020AA%' THEN 'EDOXABAN'
            WHEN bnf.BNF_CODE LIKE '0208020Y%' THEN 'RIVAROXABAN'
            WHEN bnf.BNF_CODE LIKE '0208020V%' THEN 'WARFARIN'
            WHEN bnf.BNF_CODE LIKE '0208020H%' THEN 'ACENOCOUMAROL'
            WHEN bnf.BNF_CODE LIKE '0208020N%' THEN 'PHENINDIONE'
            WHEN bnf.BNF_CODE LIKE '0208020S%' THEN 'PHENPROCOUMON'
            WHEN bnf.BNF_CODE LIKE '0208020I%' THEN 'PENTOSAN'
            ELSE 'OTHER_AGENT'
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
    WHERE bnf.BNF_CODE LIKE '020802%' -- All oral anticoagulants
        AND bnf.BNF_CODE NOT LIKE '0208020W%' -- Exclude INR testing reagents
),
OrderCounts AS (
    -- Counts the number of orders per person in the last 6 months
    SELECT
        PERSON_ID,
        COUNT(*) as RECENT_ORDER_COUNT
    FROM BaseAnticoagulantOrders
    WHERE ORDER_DATE >= DATEADD(month, -6, CURRENT_DATE())
    GROUP BY PERSON_ID
)
-- Final selection combining order details with the count
SELECT
    bso.*,
    COALESCE(oc.RECENT_ORDER_COUNT, 0) as RECENT_ORDER_COUNT
FROM BaseAnticoagulantOrders bso
LEFT JOIN OrderCounts oc
    ON bso.PERSON_ID = oc.PERSON_ID; 