CREATE OR REPLACE DYNAMIC TABLE DATA_LAB_NCL_TRAINING_TEMP.HEI_MIGRATION.INTERMEDIATE_VALPROATE_ORDERS_ALL (
    -- Identifiers
    PERSON_ID VARCHAR, -- Unique identifier for the person
    MEDICATION_ORDER_ID VARCHAR, -- Identifier for the specific medication order
    MEDICATION_STATEMENT_ID VARCHAR, -- Identifier for the linked medication statement
    -- Order Details
    ORDER_CLINICAL_EFFECTIVE_DATE TIMESTAMP_NTZ(9), -- Clinical effective date of the medication order
    ORDER_MEDICATION_NAME VARCHAR, -- Name of the medication as recorded on the order
    ORDER_DOSE VARCHAR, -- Dose information from the medication order
    ORDER_QUANTITY_VALUE FLOAT, -- Numeric value of the quantity ordered
    ORDER_QUANTITY_UNIT VARCHAR, -- Unit for the quantity ordered
    ORDER_DURATION_DAYS NUMBER(38,0), -- Duration of the prescription in days, from the order
    -- Statement Details (for context)
    STATEMENT_MEDICATION_NAME VARCHAR, -- Name of the medication from the linked statement
    -- Mapped Concept Info (from Statement Concept ID)
    MAPPED_CONCEPT_CODE VARCHAR, -- Mapped concept code (e.g., SNOMED) derived from the statement's core concept ID
    MAPPED_CONCEPT_DISPLAY VARCHAR, -- Display term for the mapped concept code
    MAPPED_CONCEPT_ID VARCHAR, -- Original Concept ID from MAPPED_CONCEPTS table
    VALPROATE_PRODUCT_TERM VARCHAR, -- Specific Valproate product term if matched via VALPROATE_PROG_CODES; NULL otherwise
    -- Match Flags indicating how the Valproate record was identified
    MATCHED_ON_NAME BOOLEAN, -- Flag: TRUE if the order/statement medication name contained 'VALPROATE' or 'VALPROIC ACID' (case-insensitive)
    MATCHED_ON_CONCEPT_ID BOOLEAN -- Flag: TRUE if the statement's core concept ID matched an entry in the VALPROATE_PROG_CODES table with CODE_CATEGORY = 'DRUG'
)
COMMENT = 'Intermediate table containing all Medication Orders identified as being for Valproate. Identification occurs via either matching medication names (Order or Statement) containing \'VALPROATE\'/\'VALPROIC ACID\' OR by matching the Statement\'s core concept ID to a list of known Valproate concept IDs in VALPROATE_PROG_CODES (CODE_CATEGORY = \'DRUG\'). Includes details from both the Order and linked Statement.'
TARGET_LAG = '4 hours'
REFRESH_MODE = AUTO
INITIALIZE = ON_CREATE
WAREHOUSE = NCL_ANALYTICS_XS
AS
-- Selects distinct medication orders identified as Valproate based on name or concept ID matching.
SELECT DISTINCT -- Using DISTINCT as a safeguard against potential upstream duplicates.
    pp."person_id" AS PERSON_ID,
    mo."id" AS MEDICATION_ORDER_ID,
    ms."id" AS MEDICATION_STATEMENT_ID,
    mo."clinical_effective_date" AS ORDER_CLINICAL_EFFECTIVE_DATE,
    mo."medication_name" AS ORDER_MEDICATION_NAME,
    mo."dose" AS ORDER_DOSE,
    mo."quantity_value" AS ORDER_QUANTITY_VALUE,
    mo."quantity_unit" AS ORDER_QUANTITY_UNIT,
    mo."duration_days" AS ORDER_DURATION_DAYS,
    ms."medication_name" AS STATEMENT_MEDICATION_NAME,
    mc.CONCEPT_CODE AS MAPPED_CONCEPT_CODE,
    mc.CONCEPT_DISPLAY AS MAPPED_CONCEPT_DISPLAY,
    mc.CONCEPT_ID AS MAPPED_CONCEPT_ID,
    vp.VALPROATE_PRODUCT_TERM,
    -- Flag indicating if match occurred via medication name in either the Order or Statement (case-insensitive).
    (
        mo."medication_name" ILIKE ANY ('%VALPROATE%', '%VALPROIC ACID%') OR
        ms."medication_name" ILIKE ANY ('%VALPROATE%', '%VALPROIC ACID%')
    ) AS MATCHED_ON_NAME,
    -- Flag indicating if match occurred via a successful join between the statement's concept ID and the VALPROATE_PROG_CODES list.
    (vp.CODE IS NOT NULL) AS MATCHED_ON_CONCEPT_ID
FROM
    "Data_Store_OLIDS_Dummy".OLIDS_MASKED.MEDICATION_ORDER mo -- Base table is Medication Order.
INNER JOIN
    "Data_Store_OLIDS_Dummy".OLIDS_MASKED.MEDICATION_STATEMENT ms ON mo."medication_statement_id" = ms."id" -- Each Order must link to a Statement.
INNER JOIN
     "Data_Store_OLIDS_Dummy".OLIDS_MASKED.PATIENT_PERSON pp ON mo."patient_id" = pp."patient_id" -- Link to Person via the Order's patient_id.
LEFT JOIN
    DATA_LAB_NCL_TRAINING_TEMP.CODESETS.MAPPED_CONCEPTS mc ON ms."medication_statement_core_concept_id" = mc.SOURCE_CODE_ID -- Attempt to map the Statement's concept ID.
LEFT JOIN
    DATA_LAB_NCL_TRAINING_TEMP.CODESETS.VALPROATE_PROG_CODES vp
    -- Join the mapped concept ID to the known list of Valproate codes, filtering for drug codes only.
    ON mc.CONCEPT_ID = vp.CODE
    AND vp.CODE_CATEGORY = 'DRUG'
WHERE
    -- The core logic: include the row if EITHER the name contains Valproate OR the concept ID matches the Valproate list.
    -- Criterion A: Check if medication name (from Order OR Statement) contains 'VALPROATE' or 'VALPROIC ACID' (case-insensitive).
    (
        mo."medication_name" ILIKE ANY ('%VALPROATE%', '%VALPROIC ACID%') OR
        ms."medication_name" ILIKE ANY ('%VALPROATE%', '%VALPROIC ACID%')
    )
    OR
    -- Criterion B: Check if the LEFT JOIN to VALPROATE_PROG_CODES was successful (meaning vp.CODE is not NULL).
    (vp.CODE IS NOT NULL);
