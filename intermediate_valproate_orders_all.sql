CREATE OR REPLACE DYNAMIC TABLE DATA_LAB_NCL_TRAINING_TEMP.HEI_MIGRATION.INTERMEDIATE_VALPROATE_ORDERS_ALL (
    -- Identifiers
    PERSON_ID VARCHAR,
    MEDICATION_ORDER_ID VARCHAR,
    MEDICATION_STATEMENT_ID VARCHAR,
    -- Order Details
    ORDER_CLINICAL_EFFECTIVE_DATE TIMESTAMP_NTZ(9),
    ORDER_MEDICATION_NAME VARCHAR,
    ORDER_DOSE VARCHAR,
    ORDER_QUANTITY_VALUE FLOAT,
    ORDER_QUANTITY_UNIT VARCHAR,
    ORDER_DURATION_DAYS NUMBER(38,0),
    -- Statement Details (for context)
    STATEMENT_MEDICATION_NAME VARCHAR,
    -- Mapped Concept Info (from Statement)
    MAPPED_CONCEPT_CODE VARCHAR,
    MAPPED_CONCEPT_DISPLAY VARCHAR,
    MAPPED_CONCEPT_ID VARCHAR,
    VALPROATE_PRODUCT_TERM VARCHAR,
    -- Match Flags
    MATCHED_ON_NAME BOOLEAN,
    MATCHED_ON_CONCEPT_ID BOOLEAN
)
TARGET_LAG = '4 hours'
REFRESH_MODE = AUTO
INITIALIZE = ON_CREATE
WAREHOUSE = NCL_ANALYTICS_XS
AS
SELECT DISTINCT -- Use DISTINCT just in case of any upstream duplication; possibly not needed but safe
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
    -- Select the original CONCEPT_ID from MAPPED_CONCEPTS
    mc.CONCEPT_ID AS MAPPED_CONCEPT_ID,
    vp.VALPROATE_PRODUCT_TERM, -- Will be NULL if match was only on name
    -- Flag indicating if match occurred via name
    (
        mo."medication_name" ILIKE ANY ('%VALPROATE%', '%VALPROIC ACID%') OR
        ms."medication_name" ILIKE ANY ('%VALPROATE%', '%VALPROIC ACID%')
    ) AS MATCHED_ON_NAME,
    -- Flag indicating if match occurred via ConceptID
    (vp.CONCEPTID IS NOT NULL) AS MATCHED_ON_CONCEPT_ID
FROM
    "Data_Store_OLIDS_Dummy".OLIDS_MASKED.MEDICATION_ORDER mo -- Start with Medication Order table
INNER JOIN
    "Data_Store_OLIDS_Dummy".OLIDS_MASKED.MEDICATION_STATEMENT ms ON mo."medication_statement_id" = ms."id" -- Link Order to its Statement
INNER JOIN
     "Data_Store_OLIDS_Dummy".OLIDS_MASKED.PATIENT_PERSON pp ON mo."patient_id" = pp."patient_id" -- Get person_id from Order's patient_id
LEFT JOIN
    DATA_LAB_NCL_TRAINING_TEMP.CODESETS.MAPPED_CONCEPTS mc ON ms."medication_statement_core_concept_id" = mc.SOURCE_CODE_ID -- Map statement concept ID
LEFT JOIN
    DATA_LAB_NCL_TRAINING_TEMP.CODESETS.VALPROATE_CODES vp
    -- CORRECTED JOIN CONDITION: Cast the NUMBER side to VARCHAR for comparison
    ON mc.CONCEPT_ID = CAST(vp.CONCEPTID AS VARCHAR)
WHERE
    -- Include the row if either the name matches OR the concept ID matches
    -- Criterion A: Name match (check both order and statement name, case-insensitive)
    (
        mo."medication_name" ILIKE ANY ('%VALPROATE%', '%VALPROIC ACID%') OR
        ms."medication_name" ILIKE ANY ('%VALPROATE%', '%VALPROIC ACID%')
    )
    OR
    -- Criterion B: Concept ID match (successful join to VALPROATE_CODES via statement's concept)
    (vp.CONCEPTID IS NOT NULL); -- This check still works as vp.CONCEPTID is only non-null if the join succeeded


