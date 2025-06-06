CREATE OR REPLACE DYNAMIC TABLE DATA_LAB_NCL_TRAINING_TEMP.HEI_MIGRATION.INTERMEDIATE_DIABETES_ORDERS_ALL (
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
    DIABETES_MEDICATION_TYPE VARCHAR, -- Type of diabetes medication (INSULIN, ANTIDIABETIC, HYPOGLYCAEMIA_TREATMENT, MONITORING)
    ANTIDIABETIC_CLASS VARCHAR, -- Specific class of antidiabetic medication (only populated when DIABETES_MEDICATION_TYPE = 'ANTIDIABETIC')
    RECENT_ORDER_COUNT NUMBER -- Count of orders in the last 6 months
)
COMMENT = 'Intermediate table containing all diabetes-related medication orders (BNF section 6.1). Includes:
- BNF 6.1.1: Insulins (e.g., Insulin aspart, Insulin glargine, Insulin lispro)
- BNF 6.1.2: Antidiabetic drugs, categorised as:
  * Biguanides (e.g., Metformin)
  * Sulfonylureas (e.g., Gliclazide, Glibenclamide, Glimepiride)
  * Thiazolidinediones (e.g., Pioglitazone, Rosiglitazone)
  * DPP-4 inhibitors (e.g., Sitagliptin, Linagliptin, Saxagliptin)
  * SGLT2 inhibitors (e.g., Dapagliflozin, Empagliflozin, Canagliflozin)
  * GLP-1 receptor agonists (e.g., Liraglutide, Dulaglutide, Semaglutide)
  * Meglitinides (e.g., Repaglinide, Nateglinide)
  * Alpha-glucosidase inhibitors (e.g., Acarbose)
  * Combination products (e.g., Metformin combinations)
- BNF 6.1.4: Treatment of hypoglycaemia (e.g., Glucagon)
- BNF 6.1.6: Diabetic diagnostic and monitoring agents (e.g., Blood glucose testing strips)'
TARGET_LAG = '4 hours'
REFRESH_MODE = AUTO
INITIALIZE = ON_CREATE
WAREHOUSE = NCL_ANALYTICS_XS
AS
WITH BaseDiabetesOrders AS (
    -- Get all medication orders for diabetes-related medications
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
            WHEN bnf.BNF_CODE LIKE '060101%' THEN 'INSULIN'
            WHEN bnf.BNF_CODE LIKE '060102%' THEN 'ANTIDIABETIC'
            WHEN bnf.BNF_CODE LIKE '060104%' THEN 'HYPOGLYCAEMIA_TREATMENT'
            WHEN bnf.BNF_CODE LIKE '060106%' THEN 'MONITORING'
            ELSE 'OTHER'
        END AS DIABETES_MEDICATION_TYPE,
        CASE 
            -- Only categorize when it's an antidiabetic drug
            WHEN bnf.BNF_CODE LIKE '060102%' THEN
                CASE
                    -- Biguanides
                    WHEN bnf.BNF_CODE LIKE '0601022%' THEN 'BIGUANIDE'
                    -- Sulfonylureas
                    WHEN bnf.BNF_CODE LIKE '0601021%' THEN 'SULFONYLUREA'
                    -- Thiazolidinediones
                    WHEN bnf.BNF_CODE LIKE '0601023B%' OR bnf.BNF_CODE LIKE '0601023S%' THEN 'THIAZOLIDINEDIONE'
                    -- DPP-4 inhibitors
                    WHEN bnf.BNF_CODE LIKE '0601023X%' OR bnf.BNF_CODE LIKE '0601023AE%' OR bnf.BNF_CODE LIKE '0601023AC%' 
                         OR bnf.BNF_CODE LIKE '0601023AK%' OR bnf.BNF_CODE LIKE '0601023AA%' THEN 'DPP4_INHIBITOR'
                    -- SGLT2 inhibitors
                    WHEN bnf.BNF_CODE LIKE '0601023AG%' OR bnf.BNF_CODE LIKE '0601023AN%' OR bnf.BNF_CODE LIKE '0601023AM%' 
                         OR bnf.BNF_CODE LIKE '0601023AX%' THEN 'SGLT2_INHIBITOR'
                    -- GLP-1 receptor agonists
                    WHEN bnf.BNF_CODE LIKE '0601023Y%' OR bnf.BNF_CODE LIKE '0601023AB%' OR bnf.BNF_CODE LIKE '0601023AI%' 
                         OR bnf.BNF_CODE LIKE '0601023AQ%' OR bnf.BNF_CODE LIKE '0601023AW%' OR bnf.BNF_CODE LIKE '0601023AS%' 
                         OR bnf.BNF_CODE LIKE '0601023AZ%' THEN 'GLP1_AGONIST'
                    -- Meglitinides
                    WHEN bnf.BNF_CODE LIKE '0601023R%' OR bnf.BNF_CODE LIKE '0601023U%' THEN 'MEGLITINIDE'
                    -- Alpha-glucosidase inhibitors
                    WHEN bnf.BNF_CODE LIKE '0601023A%' THEN 'ALPHA_GLUCOSIDASE_INHIBITOR'
                    -- Combination products
                    WHEN bnf.BNF_CODE LIKE '0601023AJ%' OR bnf.BNF_CODE LIKE '0601023AP%' OR bnf.BNF_CODE LIKE '0601023AL%' 
                         OR bnf.BNF_CODE LIKE '0601023AY%' OR bnf.BNF_CODE LIKE '0601023AR%' OR bnf.BNF_CODE LIKE '0601023W%' 
                         OR bnf.BNF_CODE LIKE '0601023V%' OR bnf.BNF_CODE LIKE '0601023AD%' OR bnf.BNF_CODE LIKE '0601023Z%' 
                         OR bnf.BNF_CODE LIKE '0601023AF%' OR bnf.BNF_CODE LIKE '0601023AH%' OR bnf.BNF_CODE LIKE '0601023AU%' 
                         OR bnf.BNF_CODE LIKE '0601023AV%' THEN 'COMBINATION_PRODUCT'
                    ELSE 'OTHER_ANTIDIABETIC'
                END
            ELSE NULL
        END AS ANTIDIABETIC_CLASS
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
    WHERE bnf.BNF_CODE LIKE '0601%' -- All diabetes-related medications
        AND bnf.BNF_CODE NOT LIKE '060103%' -- Exclude BNF 6.1.3 (Other antidiabetic drugs) as it's deprecated
        AND bnf.BNF_CODE NOT LIKE '060105%' -- Exclude BNF 6.1.5 (Treatment of diabetic nephropathy and neuropathy) as these are condition-specific
),
OrderCounts AS (
    -- Counts the number of orders per person in the last 6 months
    SELECT
        PERSON_ID,
        COUNT(*) as RECENT_ORDER_COUNT
    FROM BaseDiabetesOrders
    WHERE ORDER_DATE >= DATEADD(month, -6, CURRENT_DATE())
    GROUP BY PERSON_ID
)
-- Final selection combining order details with the count
SELECT
    bso.*,
    COALESCE(oc.RECENT_ORDER_COUNT, 0) as RECENT_ORDER_COUNT
FROM BaseDiabetesOrders bso
LEFT JOIN OrderCounts oc
    ON bso.PERSON_ID = oc.PERSON_ID; 