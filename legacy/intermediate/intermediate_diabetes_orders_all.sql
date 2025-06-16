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
        
        -- Diabetes medication type classification
        CASE 
            WHEN bnf.BNF_CODE LIKE '060101%' THEN 'INSULIN'                    -- BNF 6.1.1: Insulins
            WHEN bnf.BNF_CODE LIKE '060102%' THEN 'ANTIDIABETIC'              -- BNF 6.1.2: Antidiabetic drugs
            WHEN bnf.BNF_CODE LIKE '060104%' THEN 'HYPOGLYCAEMIA_TREATMENT'   -- BNF 6.1.4: Treatment of hypoglycaemia
            WHEN bnf.BNF_CODE LIKE '060106%' THEN 'MONITORING'                -- BNF 6.1.6: Diabetic diagnostic and monitoring agents
            ELSE 'OTHER'
        END AS DIABETES_MEDICATION_TYPE,
        
        -- Antidiabetic drug class classification (only for BNF 6.1.2)
        CASE 
            WHEN bnf.BNF_CODE LIKE '060102%' THEN
                CASE
                    -- Biguanides (BNF 6.1.2.2)
                    WHEN bnf.BNF_CODE LIKE '0601022B0%' THEN 'BIGUANIDE'  -- Metformin hydrochloride
                    
                    -- Sulfonylureas (BNF 6.1.2.1)
                    WHEN bnf.BNF_CODE LIKE '0601021A0%'   -- Glimepiride
                      OR bnf.BNF_CODE LIKE '0601021E0%'   -- Chlorpropamide
                      OR bnf.BNF_CODE LIKE '0601021H0%'   -- Glibenclamide
                      OR bnf.BNF_CODE LIKE '0601021M0%'   -- Gliclazide
                      OR bnf.BNF_CODE LIKE '0601021P0%'   -- Glipizide
                      OR bnf.BNF_CODE LIKE '0601021X0%'   -- Tolbutamide
                      THEN 'SULFONYLUREA'
                    
                    -- Thiazolidinediones (BNF 6.1.2.3)
                    WHEN bnf.BNF_CODE LIKE '0601023B0%'   -- Pioglitazone hydrochloride
                      OR bnf.BNF_CODE LIKE '0601023S0%'   -- Rosiglitazone
                      THEN 'THIAZOLIDINEDIONE'
                    
                    -- DPP-4 inhibitors (BNF 6.1.2.3)
                    WHEN bnf.BNF_CODE LIKE '0601023X0%'   -- Sitagliptin
                      OR bnf.BNF_CODE LIKE '0601023AE%'   -- Linagliptin
                      OR bnf.BNF_CODE LIKE '0601023AC%'   -- Saxagliptin
                      OR bnf.BNF_CODE LIKE '0601023AA%'   -- Vildagliptin
                      OR bnf.BNF_CODE LIKE '0601023AK%'   -- Alogliptin
                      THEN 'DPP4_INHIBITOR'
                    
                    -- SGLT2 inhibitors (BNF 6.1.2.3)
                    WHEN bnf.BNF_CODE LIKE '0601023AG%'   -- Dapagliflozin
                      OR bnf.BNF_CODE LIKE '0601023AN%'   -- Empagliflozin
                      OR bnf.BNF_CODE LIKE '0601023AM%'   -- Canagliflozin
                      OR bnf.BNF_CODE LIKE '0601023AX%'   -- Ertugliflozin
                      THEN 'SGLT2_INHIBITOR'
                    
                    -- GLP-1 receptor agonists (BNF 6.1.2.3)
                    WHEN bnf.BNF_CODE LIKE '0601023Y0%'   -- Exenatide
                      OR bnf.BNF_CODE LIKE '0601023AB%'   -- Liraglutide
                      OR bnf.BNF_CODE LIKE '0601023AQ%'   -- Dulaglutide
                      OR bnf.BNF_CODE LIKE '0601023AW%'   -- Semaglutide
                      OR bnf.BNF_CODE LIKE '0601023AI%'   -- Lixisenatide
                      OR bnf.BNF_CODE LIKE '0601023AS%'   -- Albiglutide
                      OR bnf.BNF_CODE LIKE '0601023AZ%'   -- Tirzepatide
                      THEN 'GLP1_AGONIST'
                    
                    -- Meglitinides (BNF 6.1.2.3)
                    WHEN bnf.BNF_CODE LIKE '0601023R0%'   -- Repaglinide
                      OR bnf.BNF_CODE LIKE '0601023U0%'   -- Nateglinide
                      THEN 'MEGLITINIDE'
                    
                    -- Alpha-glucosidase inhibitors (BNF 6.1.2.3)
                    WHEN bnf.BNF_CODE LIKE '0601023A0%' THEN 'ALPHA_GLUCOSIDASE_INHIBITOR'  -- Acarbose
                    
                    -- Dietary fibre/absorption modulators
                    WHEN bnf.BNF_CODE LIKE '0601023I0%' THEN 'DIETARY_FIBRE_MODULATOR'  -- Guar gum
                    
                    -- Biguanide + DPP-4 inhibitor combinations
                    WHEN bnf.BNF_CODE LIKE '0601023AJ%'   -- Alogliptin/metformin
                      OR bnf.BNF_CODE LIKE '0601023AF%'   -- Linagliptin/metformin
                      OR bnf.BNF_CODE LIKE '0601023AD%'   -- Metformin hydrochloride/sitagliptin
                      OR bnf.BNF_CODE LIKE '0601023Z0%'   -- Metformin hydrochloride/vildagliptin
                      OR bnf.BNF_CODE LIKE '0601023AH%'   -- Saxagliptin/metformin
                      THEN 'BIGUANIDE_DPP4_COMBINATION'
                    
                    -- Biguanide + SGLT2 inhibitor combinations
                    WHEN bnf.BNF_CODE LIKE '0601023AP%'   -- Canagliflozin/metformin
                      OR bnf.BNF_CODE LIKE '0601023AL%'   -- Dapagliflozin/metformin
                      OR bnf.BNF_CODE LIKE '0601023AR%'   -- Empagliflozin/metformin
                      THEN 'BIGUANIDE_SGLT2_COMBINATION'
                    
                    -- Biguanide + Thiazolidinedione combinations
                    WHEN bnf.BNF_CODE LIKE '0601023W0%'   -- Metformin hydrochloride/pioglitazone
                      OR bnf.BNF_CODE LIKE '0601023V0%'   -- Metformin hydrochloride/rosiglitazone
                      THEN 'BIGUANIDE_THIAZOLIDINEDIONE_COMBINATION'
                    
                    -- DPP-4 + SGLT2 inhibitor combinations
                    WHEN bnf.BNF_CODE LIKE '0601023AV%'   -- Saxagliptin/dapagliflozin
                      THEN 'DPP4_SGLT2_COMBINATION'
                    
                    -- SGLT2 + DPP-4 inhibitor combinations
                    WHEN bnf.BNF_CODE LIKE '0601023AY%'   -- Empagliflozin/linagliptin
                      THEN 'SGLT2_DPP4_COMBINATION'
                    
                    -- Insulin + GLP-1 agonist combinations
                    WHEN bnf.BNF_CODE LIKE '0601023AU%'   -- Ins degludec/liraglutide
                      THEN 'INSULIN_GLP1_COMBINATION'
                    
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