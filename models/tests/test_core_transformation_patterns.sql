CREATE OR REPLACE VIEW DATA_LAB_NCL_TRAINING_TEMP.TESTS.TEST_CORE_TRANSFORMATION_PATTERNS AS
WITH validation_results AS (
    -- Observation Validations
    -- Test 1: Validate Observation ID
    SELECT 
        'Data Integrity: Observation ID' as validation_type,
        'All observations should have a unique ID' as validation_description,
        COUNT(*) as total_records,
        COUNT(DISTINCT O."patient_id") as distinct_patients,
        COUNT(CASE WHEN O."id" IS NULL THEN 1 END) as records_with_issue,
        COUNT(CASE WHEN O."id" IS NULL THEN 1 END) = 0 as validation_passed,
        'Observations with missing ID' as issue_description
    FROM "Data_Store_OLIDS_Dummy"."OLIDS_MASKED"."OBSERVATION" O

    UNION ALL

    -- Test 2: Validate Observation Core Concept ID
    SELECT 
        'Data Integrity: Observation Core Concept ID' as validation_type,
        'All observations should have a core concept ID' as validation_description,
        COUNT(*) as total_records,
        COUNT(DISTINCT O."patient_id") as distinct_patients,
        COUNT(CASE WHEN O."observation_core_concept_id" IS NULL THEN 1 END) as records_with_issue,
        COUNT(CASE WHEN O."observation_core_concept_id" IS NULL THEN 1 END) = 0 as validation_passed,
        'Observations with missing core concept ID' as issue_description
    FROM "Data_Store_OLIDS_Dummy"."OLIDS_MASKED"."OBSERVATION" O

    UNION ALL

    -- Test 3: Validate Observation Clinical Effective Date
    SELECT 
        'Data Integrity: Observation Clinical Effective Date' as validation_type,
        'All observations should have a valid clinical effective date' as validation_description,
        COUNT(*) as total_records,
        COUNT(DISTINCT O."patient_id") as distinct_patients,
        COUNT(CASE WHEN O."clinical_effective_date" IS NULL OR O."clinical_effective_date" > CURRENT_DATE() THEN 1 END) as records_with_issue,
        COUNT(CASE WHEN O."clinical_effective_date" IS NULL OR O."clinical_effective_date" > CURRENT_DATE() THEN 1 END) = 0 as validation_passed,
        'Observations with missing or future clinical effective date' as issue_description
    FROM "Data_Store_OLIDS_Dummy"."OLIDS_MASKED"."OBSERVATION" O

    UNION ALL

    -- Test 4: Validate Observation Patient ID
    SELECT 
        'Data Integrity: Observation Patient ID' as validation_type,
        'All observations should have a patient ID' as validation_description,
        COUNT(*) as total_records,
        COUNT(DISTINCT O."patient_id") as distinct_patients,
        COUNT(CASE WHEN O."patient_id" IS NULL THEN 1 END) as records_with_issue,
        COUNT(CASE WHEN O."patient_id" IS NULL THEN 1 END) = 0 as validation_passed,
        'Observations with missing patient ID' as issue_description
    FROM "Data_Store_OLIDS_Dummy"."OLIDS_MASKED"."OBSERVATION" O

    UNION ALL

    -- Test 5: Validate Observation to Person mapping integrity
    SELECT 
        'Relationship Integrity: Observation to Person Mapping' as validation_type,
        'All observations should have a valid person ID mapping' as validation_description,
        COUNT(*) as total_records,
        COUNT(DISTINCT O."patient_id") as distinct_patients,
        COUNT(CASE WHEN PP."person_id" IS NULL THEN 1 END) as records_with_issue,
        COUNT(CASE WHEN PP."person_id" IS NULL THEN 1 END) = 0 as validation_passed,
        'Observations with no valid person ID mapping' as issue_description
    FROM "Data_Store_OLIDS_Dummy"."OLIDS_MASKED"."OBSERVATION" O
    LEFT JOIN "Data_Store_OLIDS_Dummy"."OLIDS_MASKED"."PATIENT_PERSON" PP 
        ON O."patient_id" = PP."patient_id"

    UNION ALL

    -- Test 6: Validate Observation to Concept mapping integrity
    SELECT 
        'Relationship Integrity: Observation to Concept Mapping' as validation_type,
        'All observations should have a valid concept mapping' as validation_description,
        COUNT(*) as total_records,
        COUNT(DISTINCT O."patient_id") as distinct_patients,
        COUNT(CASE WHEN MC.SOURCE_CODE_ID IS NULL THEN 1 END) as records_with_issue,
        COUNT(CASE WHEN MC.SOURCE_CODE_ID IS NULL THEN 1 END) = 0 as validation_passed,
        'Observations with no valid concept mapping' as issue_description
    FROM "Data_Store_OLIDS_Dummy"."OLIDS_MASKED"."OBSERVATION" O
    LEFT JOIN DATA_LAB_NCL_TRAINING_TEMP.CODESETS.MAPPED_CONCEPTS MC 
        ON O."observation_core_concept_id" = MC.SOURCE_CODE_ID

    UNION ALL

    -- Medication Order Validations
    -- Test 7: Validate Medication Order ID
    SELECT 
        'Data Integrity: Medication Order ID' as validation_type,
        'All medication orders should have a unique ID' as validation_description,
        COUNT(*) as total_records,
        COUNT(DISTINCT MO."patient_id") as distinct_patients,
        COUNT(CASE WHEN MO."id" IS NULL THEN 1 END) as records_with_issue,
        COUNT(CASE WHEN MO."id" IS NULL THEN 1 END) = 0 as validation_passed,
        'Medication orders with missing ID' as issue_description
    FROM "Data_Store_OLIDS_Dummy"."OLIDS_MASKED"."MEDICATION_ORDER" MO

    UNION ALL

    -- Test 8: Validate Medication Order Core Concept ID
    SELECT 
        'Data Integrity: Medication Order Core Concept ID' as validation_type,
        'All medication orders should have a core concept ID' as validation_description,
        COUNT(*) as total_records,
        COUNT(DISTINCT MO."patient_id") as distinct_patients,
        COUNT(CASE WHEN MO."medication_order_core_concept_id" IS NULL THEN 1 END) as records_with_issue,
        COUNT(CASE WHEN MO."medication_order_core_concept_id" IS NULL THEN 1 END) = 0 as validation_passed,
        'Medication orders with missing core concept ID' as issue_description
    FROM "Data_Store_OLIDS_Dummy"."OLIDS_MASKED"."MEDICATION_ORDER" MO

    UNION ALL

    -- Test 9: Validate Medication Order Clinical Effective Date
    SELECT 
        'Data Integrity: Medication Order Clinical Effective Date' as validation_type,
        'All medication orders should have a valid clinical effective date' as validation_description,
        COUNT(*) as total_records,
        COUNT(DISTINCT MO."patient_id") as distinct_patients,
        COUNT(CASE WHEN MO."clinical_effective_date" IS NULL OR MO."clinical_effective_date" > CURRENT_DATE() THEN 1 END) as records_with_issue,
        COUNT(CASE WHEN MO."clinical_effective_date" IS NULL OR MO."clinical_effective_date" > CURRENT_DATE() THEN 1 END) = 0 as validation_passed,
        'Medication orders with missing or future clinical effective date' as issue_description
    FROM "Data_Store_OLIDS_Dummy"."OLIDS_MASKED"."MEDICATION_ORDER" MO

    UNION ALL

    -- Test 10: Validate Medication Order Patient ID
    SELECT 
        'Data Integrity: Medication Order Patient ID' as validation_type,
        'All medication orders should have a patient ID' as validation_description,
        COUNT(*) as total_records,
        COUNT(DISTINCT MO."patient_id") as distinct_patients,
        COUNT(CASE WHEN MO."patient_id" IS NULL THEN 1 END) as records_with_issue,
        COUNT(CASE WHEN MO."patient_id" IS NULL THEN 1 END) = 0 as validation_passed,
        'Medication orders with missing patient ID' as issue_description
    FROM "Data_Store_OLIDS_Dummy"."OLIDS_MASKED"."MEDICATION_ORDER" MO

    UNION ALL

    -- Test 11: Validate Medication Order Statement ID
    SELECT 
        'Data Integrity: Medication Order Statement ID' as validation_type,
        'All medication orders should have a statement ID' as validation_description,
        COUNT(*) as total_records,
        COUNT(DISTINCT MO."patient_id") as distinct_patients,
        COUNT(CASE WHEN MO."medication_statement_id" IS NULL THEN 1 END) as records_with_issue,
        COUNT(CASE WHEN MO."medication_statement_id" IS NULL THEN 1 END) = 0 as validation_passed,
        'Medication orders with missing statement ID' as issue_description
    FROM "Data_Store_OLIDS_Dummy"."OLIDS_MASKED"."MEDICATION_ORDER" MO

    UNION ALL

    -- Test 12: Validate Medication Order to Statement mapping integrity
    SELECT 
        'Relationship Integrity: Medication Order to Statement Mapping' as validation_type,
        'All medication orders should have a valid statement mapping' as validation_description,
        COUNT(*) as total_records,
        COUNT(DISTINCT MO."patient_id") as distinct_patients,
        COUNT(CASE WHEN MS."id" IS NULL THEN 1 END) as records_with_issue,
        COUNT(CASE WHEN MS."id" IS NULL THEN 1 END) = 0 as validation_passed,
        'Medication orders with no valid statement mapping' as issue_description
    FROM "Data_Store_OLIDS_Dummy"."OLIDS_MASKED"."MEDICATION_ORDER" MO
    LEFT JOIN "Data_Store_OLIDS_Dummy"."OLIDS_MASKED"."MEDICATION_STATEMENT" MS 
        ON MO."medication_statement_id" = MS."id"

    UNION ALL

    -- Test 13: Validate Medication Order to Concept mapping integrity
    SELECT 
        'Relationship Integrity: Medication Order to Concept Mapping' as validation_type,
        'All medication orders should have a valid concept mapping' as validation_description,
        COUNT(*) as total_records,
        COUNT(DISTINCT MO."patient_id") as distinct_patients,
        COUNT(CASE WHEN MC.SOURCE_CODE_ID IS NULL THEN 1 END) as records_with_issue,
        COUNT(CASE WHEN MC.SOURCE_CODE_ID IS NULL THEN 1 END) = 0 as validation_passed,
        'Medication orders with no valid concept mapping' as issue_description
    FROM "Data_Store_OLIDS_Dummy"."OLIDS_MASKED"."MEDICATION_ORDER" MO
    LEFT JOIN DATA_LAB_NCL_TRAINING_TEMP.CODESETS.MAPPED_CONCEPTS MC 
        ON MO."medication_order_core_concept_id" = MC.SOURCE_CODE_ID

    UNION ALL

    -- Test 14: Validate Medication Order to Person mapping integrity
    SELECT 
        'Relationship Integrity: Medication Order to Person Mapping' as validation_type,
        'All medication orders should have a valid person ID mapping' as validation_description,
        COUNT(*) as total_records,
        COUNT(DISTINCT MO."patient_id") as distinct_patients,
        COUNT(CASE WHEN PP."person_id" IS NULL THEN 1 END) as records_with_issue,
        COUNT(CASE WHEN PP."person_id" IS NULL THEN 1 END) = 0 as validation_passed,
        'Medication orders with no valid person ID mapping' as issue_description
    FROM "Data_Store_OLIDS_Dummy"."OLIDS_MASKED"."MEDICATION_ORDER" MO
    LEFT JOIN "Data_Store_OLIDS_Dummy"."OLIDS_MASKED"."PATIENT_PERSON" PP 
        ON MO."patient_id" = PP."patient_id"

    UNION ALL

    -- Medication Statement Validations
    -- Test 15: Validate Medication Statement ID
    SELECT 
        'Data Integrity: Medication Statement ID' as validation_type,
        'All medication statements should have a unique ID' as validation_description,
        COUNT(*) as total_records,
        COUNT(DISTINCT MS."patient_id") as distinct_patients,
        COUNT(CASE WHEN MS."id" IS NULL THEN 1 END) as records_with_issue,
        COUNT(CASE WHEN MS."id" IS NULL THEN 1 END) = 0 as validation_passed,
        'Medication statements with missing ID' as issue_description
    FROM "Data_Store_OLIDS_Dummy"."OLIDS_MASKED"."MEDICATION_STATEMENT" MS

    UNION ALL

    -- Test 16: Validate Medication Statement Core Concept ID
    SELECT 
        'Data Integrity: Medication Statement Core Concept ID' as validation_type,
        'All medication statements should have a core concept ID' as validation_description,
        COUNT(*) as total_records,
        COUNT(DISTINCT MS."patient_id") as distinct_patients,
        COUNT(CASE WHEN MS."medication_statement_core_concept_id" IS NULL THEN 1 END) as records_with_issue,
        COUNT(CASE WHEN MS."medication_statement_core_concept_id" IS NULL THEN 1 END) = 0 as validation_passed,
        'Medication statements with missing core concept ID' as issue_description
    FROM "Data_Store_OLIDS_Dummy"."OLIDS_MASKED"."MEDICATION_STATEMENT" MS

    UNION ALL

    -- Test 17: Validate Medication Statement Clinical Effective Date
    SELECT 
        'Data Integrity: Medication Statement Clinical Effective Date' as validation_type,
        'All medication statements should have a valid clinical effective date' as validation_description,
        COUNT(*) as total_records,
        COUNT(DISTINCT MS."patient_id") as distinct_patients,
        COUNT(CASE WHEN MS."clinical_effective_date" IS NULL OR MS."clinical_effective_date" > CURRENT_DATE() THEN 1 END) as records_with_issue,
        COUNT(CASE WHEN MS."clinical_effective_date" IS NULL OR MS."clinical_effective_date" > CURRENT_DATE() THEN 1 END) = 0 as validation_passed,
        'Medication statements with missing or future clinical effective date' as issue_description
    FROM "Data_Store_OLIDS_Dummy"."OLIDS_MASKED"."MEDICATION_STATEMENT" MS

    UNION ALL

    -- Test 18: Validate Medication Statement Patient ID
    SELECT 
        'Data Integrity: Medication Statement Patient ID' as validation_type,
        'All medication statements should have a patient ID' as validation_description,
        COUNT(*) as total_records,
        COUNT(DISTINCT MS."patient_id") as distinct_patients,
        COUNT(CASE WHEN MS."patient_id" IS NULL THEN 1 END) as records_with_issue,
        COUNT(CASE WHEN MS."patient_id" IS NULL THEN 1 END) = 0 as validation_passed,
        'Medication statements with missing patient ID' as issue_description
    FROM "Data_Store_OLIDS_Dummy"."OLIDS_MASKED"."MEDICATION_STATEMENT" MS

    UNION ALL

    -- Test 19: Validate Medication Statement Active Status
    SELECT 
        'Data Integrity: Medication Statement Active Status' as validation_type,
        'All medication statements should have an active status' as validation_description,
        COUNT(*) as total_records,
        COUNT(DISTINCT MS."patient_id") as distinct_patients,
        COUNT(CASE WHEN MS."is_active" IS NULL THEN 1 END) as records_with_issue,
        COUNT(CASE WHEN MS."is_active" IS NULL THEN 1 END) = 0 as validation_passed,
        'Medication statements with missing active status' as issue_description
    FROM "Data_Store_OLIDS_Dummy"."OLIDS_MASKED"."MEDICATION_STATEMENT" MS

    UNION ALL

    -- Test 20: Validate Medication Statement to Concept mapping integrity
    SELECT 
        'Relationship Integrity: Medication Statement to Concept Mapping' as validation_type,
        'All medication statements should have a valid concept mapping' as validation_description,
        COUNT(*) as total_records,
        COUNT(DISTINCT MS."patient_id") as distinct_patients,
        COUNT(CASE WHEN MC.SOURCE_CODE_ID IS NULL THEN 1 END) as records_with_issue,
        COUNT(CASE WHEN MC.SOURCE_CODE_ID IS NULL THEN 1 END) = 0 as validation_passed,
        'Medication statements with no valid concept mapping' as issue_description
    FROM "Data_Store_OLIDS_Dummy"."OLIDS_MASKED"."MEDICATION_STATEMENT" MS
    LEFT JOIN DATA_LAB_NCL_TRAINING_TEMP.CODESETS.MAPPED_CONCEPTS MC 
        ON MS."medication_statement_core_concept_id" = MC.SOURCE_CODE_ID

    UNION ALL

    -- Test 21: Validate Medication Statement to Person mapping integrity
    SELECT 
        'Relationship Integrity: Medication Statement to Person Mapping' as validation_type,
        'All medication statements should have a valid person ID mapping' as validation_description,
        COUNT(*) as total_records,
        COUNT(DISTINCT MS."patient_id") as distinct_patients,
        COUNT(CASE WHEN PP."person_id" IS NULL THEN 1 END) as records_with_issue,
        COUNT(CASE WHEN PP."person_id" IS NULL THEN 1 END) = 0 as validation_passed,
        'Medication statements with no valid person ID mapping' as issue_description
    FROM "Data_Store_OLIDS_Dummy"."OLIDS_MASKED"."MEDICATION_STATEMENT" MS
    LEFT JOIN "Data_Store_OLIDS_Dummy"."OLIDS_MASKED"."PATIENT_PERSON" PP 
        ON MS."patient_id" = PP."patient_id"
)
SELECT 
    validation_type as "Validation Type",
    validation_description as "Validation Description",
    total_records as "Total Records",
    distinct_patients as "Distinct Patients",
    records_with_issue as "Records with Issue",
    ROUND(records_with_issue::FLOAT / total_records * 100, 2) as "Percentage with Issue",
    issue_description as "Issue Description",
    validation_passed as "Validation Passed"
FROM validation_results
ORDER BY validation_passed, validation_type; 