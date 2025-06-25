CREATE OR REPLACE VIEW DATA_LAB_NCL_TRAINING_TEMP.TESTS.TEST_CORE_TRANSFORMATION_PATTERNS AS
WITH VALIDATION_RESULTS AS (
    -- Observation Validations
    -- Test 1: Validate Observation ID
    SELECT
        'Data Integrity: Observation ID' AS VALIDATION_TYPE,
        'All observations should have a unique ID' AS VALIDATION_DESCRIPTION,
        COUNT(*) AS TOTAL_RECORDS,
        COUNT(DISTINCT O."patient_id") AS DISTINCT_PATIENTS,
        COUNT(CASE WHEN O."id" IS NULL THEN 1 END) AS RECORDS_WITH_ISSUE,
        COUNT(CASE WHEN O."id" IS NULL THEN 1 END) = 0 AS VALIDATION_PASSED,
        'Observations with missing ID' AS ISSUE_DESCRIPTION
    FROM "Data_Store_OLIDS_Dummy"."OLIDS_MASKED"."OBSERVATION" AS O

    UNION ALL

    -- Test 2: Validate Observation Core Concept ID
    SELECT
        'Data Integrity: Observation Core Concept ID' AS VALIDATION_TYPE,
        'All observations should have a core concept ID'
            AS VALIDATION_DESCRIPTION,
        COUNT(*) AS TOTAL_RECORDS,
        COUNT(DISTINCT O."patient_id") AS DISTINCT_PATIENTS,
        COUNT(CASE WHEN O."observation_core_concept_id" IS NULL THEN 1 END)
            AS RECORDS_WITH_ISSUE,
        COUNT(CASE WHEN O."observation_core_concept_id" IS NULL THEN 1 END)
        = 0 AS VALIDATION_PASSED,
        'Observations with missing core concept ID' AS ISSUE_DESCRIPTION
    FROM "Data_Store_OLIDS_Dummy"."OLIDS_MASKED"."OBSERVATION" AS O

    UNION ALL

    -- Test 3: Validate Observation Clinical Effective Date
    SELECT
        'Data Integrity: Observation Clinical Effective Date'
            AS VALIDATION_TYPE,
        'All observations should have a valid clinical effective date'
            AS VALIDATION_DESCRIPTION,
        COUNT(*) AS TOTAL_RECORDS,
        COUNT(DISTINCT O."patient_id") AS DISTINCT_PATIENTS,
        COUNT(
            CASE
                WHEN
                    O."clinical_effective_date" IS NULL
                    OR O."clinical_effective_date" > CURRENT_DATE()
                    THEN 1
            END
        ) AS RECORDS_WITH_ISSUE,
        COUNT(
            CASE
                WHEN
                    O."clinical_effective_date" IS NULL
                    OR O."clinical_effective_date" > CURRENT_DATE()
                    THEN 1
            END
        )
        = 0 AS VALIDATION_PASSED,
        'Observations with missing or future clinical effective date'
            AS ISSUE_DESCRIPTION
    FROM "Data_Store_OLIDS_Dummy"."OLIDS_MASKED"."OBSERVATION" AS O

    UNION ALL

    -- Test 4: Validate Observation Patient ID
    SELECT
        'Data Integrity: Observation Patient ID' AS VALIDATION_TYPE,
        'All observations should have a patient ID' AS VALIDATION_DESCRIPTION,
        COUNT(*) AS TOTAL_RECORDS,
        COUNT(DISTINCT O."patient_id") AS DISTINCT_PATIENTS,
        COUNT(CASE WHEN O."patient_id" IS NULL THEN 1 END)
            AS RECORDS_WITH_ISSUE,
        COUNT(CASE WHEN O."patient_id" IS NULL THEN 1 END)
        = 0 AS VALIDATION_PASSED,
        'Observations with missing patient ID' AS ISSUE_DESCRIPTION
    FROM "Data_Store_OLIDS_Dummy"."OLIDS_MASKED"."OBSERVATION" AS O

    UNION ALL

    -- Test 5: Validate Observation to Person mapping integrity
    SELECT
        'Relationship Integrity: Observation to Person Mapping'
            AS VALIDATION_TYPE,
        'All observations should have a valid person ID mapping'
            AS VALIDATION_DESCRIPTION,
        COUNT(*) AS TOTAL_RECORDS,
        COUNT(DISTINCT O."patient_id") AS DISTINCT_PATIENTS,
        COUNT(CASE WHEN PP."person_id" IS NULL THEN 1 END)
            AS RECORDS_WITH_ISSUE,
        COUNT(CASE WHEN PP."person_id" IS NULL THEN 1 END)
        = 0 AS VALIDATION_PASSED,
        'Observations with no valid person ID mapping' AS ISSUE_DESCRIPTION
    FROM "Data_Store_OLIDS_Dummy"."OLIDS_MASKED"."OBSERVATION" AS O
    LEFT JOIN "Data_Store_OLIDS_Dummy"."OLIDS_MASKED"."PATIENT_PERSON" AS PP
        ON O."patient_id" = PP."patient_id"

    UNION ALL

    -- Test 6: Validate Observation to Concept mapping integrity
    SELECT
        'Relationship Integrity: Observation to Concept Mapping'
            AS VALIDATION_TYPE,
        'All observations should have a valid concept mapping'
            AS VALIDATION_DESCRIPTION,
        COUNT(*) AS TOTAL_RECORDS,
        COUNT(DISTINCT O."patient_id") AS DISTINCT_PATIENTS,
        COUNT(CASE WHEN MC.SOURCE_CODE_ID IS NULL THEN 1 END)
            AS RECORDS_WITH_ISSUE,
        COUNT(CASE WHEN MC.SOURCE_CODE_ID IS NULL THEN 1 END)
        = 0 AS VALIDATION_PASSED,
        'Observations with no valid concept mapping' AS ISSUE_DESCRIPTION
    FROM "Data_Store_OLIDS_Dummy"."OLIDS_MASKED"."OBSERVATION" AS O
    LEFT JOIN DATA_LAB_NCL_TRAINING_TEMP.CODESETS.MAPPED_CONCEPTS AS MC
        ON O."observation_core_concept_id" = MC.SOURCE_CODE_ID

    UNION ALL

    -- Medication Order Validations
    -- Test 7: Validate Medication Order ID
    SELECT
        'Data Integrity: Medication Order ID' AS VALIDATION_TYPE,
        'All medication orders should have a unique ID'
            AS VALIDATION_DESCRIPTION,
        COUNT(*) AS TOTAL_RECORDS,
        COUNT(DISTINCT MO."patient_id") AS DISTINCT_PATIENTS,
        COUNT(CASE WHEN MO."id" IS NULL THEN 1 END) AS RECORDS_WITH_ISSUE,
        COUNT(CASE WHEN MO."id" IS NULL THEN 1 END) = 0 AS VALIDATION_PASSED,
        'Medication orders with missing ID' AS ISSUE_DESCRIPTION
    FROM "Data_Store_OLIDS_Dummy"."OLIDS_MASKED"."MEDICATION_ORDER" AS MO

    UNION ALL

    -- Test 8: Validate Medication Order Core Concept ID
    SELECT
        'Data Integrity: Medication Order Core Concept ID' AS VALIDATION_TYPE,
        'All medication orders should have a core concept ID'
            AS VALIDATION_DESCRIPTION,
        COUNT(*) AS TOTAL_RECORDS,
        COUNT(DISTINCT MO."patient_id") AS DISTINCT_PATIENTS,
        COUNT(
            CASE WHEN MO."medication_order_core_concept_id" IS NULL THEN 1 END
        ) AS RECORDS_WITH_ISSUE,
        COUNT(
            CASE WHEN MO."medication_order_core_concept_id" IS NULL THEN 1 END
        )
        = 0 AS VALIDATION_PASSED,
        'Medication orders with missing core concept ID' AS ISSUE_DESCRIPTION
    FROM "Data_Store_OLIDS_Dummy"."OLIDS_MASKED"."MEDICATION_ORDER" AS MO

    UNION ALL

    -- Test 9: Validate Medication Order Clinical Effective Date
    SELECT
        'Data Integrity: Medication Order Clinical Effective Date'
            AS VALIDATION_TYPE,
        'All medication orders should have a valid clinical effective date'
            AS VALIDATION_DESCRIPTION,
        COUNT(*) AS TOTAL_RECORDS,
        COUNT(DISTINCT MO."patient_id") AS DISTINCT_PATIENTS,
        COUNT(
            CASE
                WHEN
                    MO."clinical_effective_date" IS NULL
                    OR MO."clinical_effective_date" > CURRENT_DATE()
                    THEN 1
            END
        ) AS RECORDS_WITH_ISSUE,
        COUNT(
            CASE
                WHEN
                    MO."clinical_effective_date" IS NULL
                    OR MO."clinical_effective_date" > CURRENT_DATE()
                    THEN 1
            END
        )
        = 0 AS VALIDATION_PASSED,
        'Medication orders with missing or future clinical effective date'
            AS ISSUE_DESCRIPTION
    FROM "Data_Store_OLIDS_Dummy"."OLIDS_MASKED"."MEDICATION_ORDER" AS MO

    UNION ALL

    -- Test 10: Validate Medication Order Patient ID
    SELECT
        'Data Integrity: Medication Order Patient ID' AS VALIDATION_TYPE,
        'All medication orders should have a patient ID'
            AS VALIDATION_DESCRIPTION,
        COUNT(*) AS TOTAL_RECORDS,
        COUNT(DISTINCT MO."patient_id") AS DISTINCT_PATIENTS,
        COUNT(CASE WHEN MO."patient_id" IS NULL THEN 1 END)
            AS RECORDS_WITH_ISSUE,
        COUNT(CASE WHEN MO."patient_id" IS NULL THEN 1 END)
        = 0 AS VALIDATION_PASSED,
        'Medication orders with missing patient ID' AS ISSUE_DESCRIPTION
    FROM "Data_Store_OLIDS_Dummy"."OLIDS_MASKED"."MEDICATION_ORDER" AS MO

    UNION ALL

    -- Test 11: Validate Medication Order Statement ID
    SELECT
        'Data Integrity: Medication Order Statement ID' AS VALIDATION_TYPE,
        'All medication orders should have a statement ID'
            AS VALIDATION_DESCRIPTION,
        COUNT(*) AS TOTAL_RECORDS,
        COUNT(DISTINCT MO."patient_id") AS DISTINCT_PATIENTS,
        COUNT(CASE WHEN MO."medication_statement_id" IS NULL THEN 1 END)
            AS RECORDS_WITH_ISSUE,
        COUNT(CASE WHEN MO."medication_statement_id" IS NULL THEN 1 END)
        = 0 AS VALIDATION_PASSED,
        'Medication orders with missing statement ID' AS ISSUE_DESCRIPTION
    FROM "Data_Store_OLIDS_Dummy"."OLIDS_MASKED"."MEDICATION_ORDER" AS MO

    UNION ALL

    -- Test 12: Validate Medication Order to Statement mapping integrity
    SELECT
        'Relationship Integrity: Medication Order to Statement Mapping'
            AS VALIDATION_TYPE,
        'All medication orders should have a valid statement mapping'
            AS VALIDATION_DESCRIPTION,
        COUNT(*) AS TOTAL_RECORDS,
        COUNT(DISTINCT MO."patient_id") AS DISTINCT_PATIENTS,
        COUNT(CASE WHEN MS."id" IS NULL THEN 1 END) AS RECORDS_WITH_ISSUE,
        COUNT(CASE WHEN MS."id" IS NULL THEN 1 END) = 0 AS VALIDATION_PASSED,
        'Medication orders with no valid statement mapping' AS ISSUE_DESCRIPTION
    FROM "Data_Store_OLIDS_Dummy"."OLIDS_MASKED"."MEDICATION_ORDER" AS MO
    LEFT JOIN
        "Data_Store_OLIDS_Dummy"."OLIDS_MASKED"."MEDICATION_STATEMENT" AS MS
        ON MO."medication_statement_id" = MS."id"

    UNION ALL

    -- Test 13: Validate Medication Order to Concept mapping integrity
    SELECT
        'Relationship Integrity: Medication Order to Concept Mapping'
            AS VALIDATION_TYPE,
        'All medication orders should have a valid concept mapping'
            AS VALIDATION_DESCRIPTION,
        COUNT(*) AS TOTAL_RECORDS,
        COUNT(DISTINCT MO."patient_id") AS DISTINCT_PATIENTS,
        COUNT(CASE WHEN MC.SOURCE_CODE_ID IS NULL THEN 1 END)
            AS RECORDS_WITH_ISSUE,
        COUNT(CASE WHEN MC.SOURCE_CODE_ID IS NULL THEN 1 END)
        = 0 AS VALIDATION_PASSED,
        'Medication orders with no valid concept mapping' AS ISSUE_DESCRIPTION
    FROM "Data_Store_OLIDS_Dummy"."OLIDS_MASKED"."MEDICATION_ORDER" AS MO
    LEFT JOIN DATA_LAB_NCL_TRAINING_TEMP.CODESETS.MAPPED_CONCEPTS AS MC
        ON MO."medication_order_core_concept_id" = MC.SOURCE_CODE_ID

    UNION ALL

    -- Test 14: Validate Medication Order to Person mapping integrity
    SELECT
        'Relationship Integrity: Medication Order to Person Mapping'
            AS VALIDATION_TYPE,
        'All medication orders should have a valid person ID mapping'
            AS VALIDATION_DESCRIPTION,
        COUNT(*) AS TOTAL_RECORDS,
        COUNT(DISTINCT MO."patient_id") AS DISTINCT_PATIENTS,
        COUNT(CASE WHEN PP."person_id" IS NULL THEN 1 END)
            AS RECORDS_WITH_ISSUE,
        COUNT(CASE WHEN PP."person_id" IS NULL THEN 1 END)
        = 0 AS VALIDATION_PASSED,
        'Medication orders with no valid person ID mapping' AS ISSUE_DESCRIPTION
    FROM "Data_Store_OLIDS_Dummy"."OLIDS_MASKED"."MEDICATION_ORDER" AS MO
    LEFT JOIN "Data_Store_OLIDS_Dummy"."OLIDS_MASKED"."PATIENT_PERSON" AS PP
        ON MO."patient_id" = PP."patient_id"

    UNION ALL

    -- Medication Statement Validations
    -- Test 15: Validate Medication Statement ID
    SELECT
        'Data Integrity: Medication Statement ID' AS VALIDATION_TYPE,
        'All medication statements should have a unique ID'
            AS VALIDATION_DESCRIPTION,
        COUNT(*) AS TOTAL_RECORDS,
        COUNT(DISTINCT MS."patient_id") AS DISTINCT_PATIENTS,
        COUNT(CASE WHEN MS."id" IS NULL THEN 1 END) AS RECORDS_WITH_ISSUE,
        COUNT(CASE WHEN MS."id" IS NULL THEN 1 END) = 0 AS VALIDATION_PASSED,
        'Medication statements with missing ID' AS ISSUE_DESCRIPTION
    FROM "Data_Store_OLIDS_Dummy"."OLIDS_MASKED"."MEDICATION_STATEMENT" AS MS

    UNION ALL

    -- Test 16: Validate Medication Statement Core Concept ID
    SELECT
        'Data Integrity: Medication Statement Core Concept ID'
            AS VALIDATION_TYPE,
        'All medication statements should have a core concept ID'
            AS VALIDATION_DESCRIPTION,
        COUNT(*) AS TOTAL_RECORDS,
        COUNT(DISTINCT MS."patient_id") AS DISTINCT_PATIENTS,
        COUNT(
            CASE
                WHEN MS."medication_statement_core_concept_id" IS NULL THEN 1
            END
        ) AS RECORDS_WITH_ISSUE,
        COUNT(
            CASE
                WHEN MS."medication_statement_core_concept_id" IS NULL THEN 1
            END
        )
        = 0 AS VALIDATION_PASSED,
        'Medication statements with missing core concept ID'
            AS ISSUE_DESCRIPTION
    FROM "Data_Store_OLIDS_Dummy"."OLIDS_MASKED"."MEDICATION_STATEMENT" AS MS

    UNION ALL

    -- Test 17: Validate Medication Statement Clinical Effective Date
    SELECT
        'Data Integrity: Medication Statement Clinical Effective Date'
            AS VALIDATION_TYPE,
        'All medication statements should have a valid clinical effective date'
            AS VALIDATION_DESCRIPTION,
        COUNT(*) AS TOTAL_RECORDS,
        COUNT(DISTINCT MS."patient_id") AS DISTINCT_PATIENTS,
        COUNT(
            CASE
                WHEN
                    MS."clinical_effective_date" IS NULL
                    OR MS."clinical_effective_date" > CURRENT_DATE()
                    THEN 1
            END
        ) AS RECORDS_WITH_ISSUE,
        COUNT(
            CASE
                WHEN
                    MS."clinical_effective_date" IS NULL
                    OR MS."clinical_effective_date" > CURRENT_DATE()
                    THEN 1
            END
        )
        = 0 AS VALIDATION_PASSED,
        'Medication statements with missing or future clinical effective date'
            AS ISSUE_DESCRIPTION
    FROM "Data_Store_OLIDS_Dummy"."OLIDS_MASKED"."MEDICATION_STATEMENT" AS MS

    UNION ALL

    -- Test 18: Validate Medication Statement Patient ID
    SELECT
        'Data Integrity: Medication Statement Patient ID' AS VALIDATION_TYPE,
        'All medication statements should have a patient ID'
            AS VALIDATION_DESCRIPTION,
        COUNT(*) AS TOTAL_RECORDS,
        COUNT(DISTINCT MS."patient_id") AS DISTINCT_PATIENTS,
        COUNT(CASE WHEN MS."patient_id" IS NULL THEN 1 END)
            AS RECORDS_WITH_ISSUE,
        COUNT(CASE WHEN MS."patient_id" IS NULL THEN 1 END)
        = 0 AS VALIDATION_PASSED,
        'Medication statements with missing patient ID' AS ISSUE_DESCRIPTION
    FROM "Data_Store_OLIDS_Dummy"."OLIDS_MASKED"."MEDICATION_STATEMENT" AS MS

    UNION ALL

    -- Test 19: Validate Medication Statement Active Status
    SELECT
        'Data Integrity: Medication Statement Active Status' AS VALIDATION_TYPE,
        'All medication statements should have an active status'
            AS VALIDATION_DESCRIPTION,
        COUNT(*) AS TOTAL_RECORDS,
        COUNT(DISTINCT MS."patient_id") AS DISTINCT_PATIENTS,
        COUNT(CASE WHEN MS."is_active" IS NULL THEN 1 END)
            AS RECORDS_WITH_ISSUE,
        COUNT(CASE WHEN MS."is_active" IS NULL THEN 1 END)
        = 0 AS VALIDATION_PASSED,
        'Medication statements with missing active status' AS ISSUE_DESCRIPTION
    FROM "Data_Store_OLIDS_Dummy"."OLIDS_MASKED"."MEDICATION_STATEMENT" AS MS

    UNION ALL

    -- Test 20: Validate Medication Statement to Concept mapping integrity
    SELECT
        'Relationship Integrity: Medication Statement to Concept Mapping'
            AS VALIDATION_TYPE,
        'All medication statements should have a valid concept mapping'
            AS VALIDATION_DESCRIPTION,
        COUNT(*) AS TOTAL_RECORDS,
        COUNT(DISTINCT MS."patient_id") AS DISTINCT_PATIENTS,
        COUNT(CASE WHEN MC.SOURCE_CODE_ID IS NULL THEN 1 END)
            AS RECORDS_WITH_ISSUE,
        COUNT(CASE WHEN MC.SOURCE_CODE_ID IS NULL THEN 1 END)
        = 0 AS VALIDATION_PASSED,
        'Medication statements with no valid concept mapping'
            AS ISSUE_DESCRIPTION
    FROM "Data_Store_OLIDS_Dummy"."OLIDS_MASKED"."MEDICATION_STATEMENT" AS MS
    LEFT JOIN DATA_LAB_NCL_TRAINING_TEMP.CODESETS.MAPPED_CONCEPTS AS MC
        ON MS."medication_statement_core_concept_id" = MC.SOURCE_CODE_ID

    UNION ALL

    -- Test 21: Validate Medication Statement to Person mapping integrity
    SELECT
        'Relationship Integrity: Medication Statement to Person Mapping'
            AS VALIDATION_TYPE,
        'All medication statements should have a valid person ID mapping'
            AS VALIDATION_DESCRIPTION,
        COUNT(*) AS TOTAL_RECORDS,
        COUNT(DISTINCT MS."patient_id") AS DISTINCT_PATIENTS,
        COUNT(CASE WHEN PP."person_id" IS NULL THEN 1 END)
            AS RECORDS_WITH_ISSUE,
        COUNT(CASE WHEN PP."person_id" IS NULL THEN 1 END)
        = 0 AS VALIDATION_PASSED,
        'Medication statements with no valid person ID mapping'
            AS ISSUE_DESCRIPTION
    FROM "Data_Store_OLIDS_Dummy"."OLIDS_MASKED"."MEDICATION_STATEMENT" AS MS
    LEFT JOIN "Data_Store_OLIDS_Dummy"."OLIDS_MASKED"."PATIENT_PERSON" AS PP
        ON MS."patient_id" = PP."patient_id"
)

SELECT
    VALIDATION_TYPE AS "Validation Type",
    VALIDATION_DESCRIPTION AS "Validation Description",
    TOTAL_RECORDS AS "Total Records",
    DISTINCT_PATIENTS AS "Distinct Patients",
    RECORDS_WITH_ISSUE AS "Records with Issue",
    ISSUE_DESCRIPTION AS "Issue Description",
    VALIDATION_PASSED AS "Validation Passed",
    ROUND(RECORDS_WITH_ISSUE::FLOAT / TOTAL_RECORDS * 100, 2)
        AS "Percentage with Issue"
FROM VALIDATION_RESULTS
ORDER BY VALIDATION_PASSED, VALIDATION_TYPE;
