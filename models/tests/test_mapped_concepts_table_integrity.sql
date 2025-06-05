CREATE OR REPLACE VIEW DATA_LAB_NCL_TRAINING_TEMP.TESTS.TEST_MAPPED_CONCEPTS_TABLE_INTEGRITY AS
WITH table_validation AS (
    -- Check for NULL SOURCE_CODE_ID
    SELECT 
        'Data Integrity: NULL SOURCE_CODE_ID' as validation_type,
        'SOURCE_CODE_ID is a required field in MAPPED_CONCEPTS' as validation_description,
        COUNT(*) as total_mapped_concepts,
        COUNT(DISTINCT SOURCE_CODE_ID) as distinct_source_codes,
        COUNT(CASE WHEN SOURCE_CODE_ID IS NULL THEN 1 END) as records_with_issue,
        COUNT(CASE WHEN SOURCE_CODE_ID IS NULL THEN 1 END) = 0 as validation_passed,
        'Records with NULL SOURCE_CODE_ID in MAPPED_CONCEPTS' as issue_description
    FROM DATA_LAB_NCL_TRAINING_TEMP.CODESETS.MAPPED_CONCEPTS
    
    UNION ALL
    
    -- Check for NULL CONCEPT_CODE
    SELECT 
        'Data Integrity: NULL CONCEPT_CODE' as validation_type,
        'CONCEPT_CODE is a required field in MAPPED_CONCEPTS' as validation_description,
        COUNT(*) as total_mapped_concepts,
        COUNT(DISTINCT SOURCE_CODE_ID) as distinct_source_codes,
        COUNT(CASE WHEN CONCEPT_CODE IS NULL THEN 1 END) as records_with_issue,
        COUNT(CASE WHEN CONCEPT_CODE IS NULL THEN 1 END) = 0 as validation_passed,
        'Records with NULL CONCEPT_CODE in MAPPED_CONCEPTS' as issue_description
    FROM DATA_LAB_NCL_TRAINING_TEMP.CODESETS.MAPPED_CONCEPTS
    
    UNION ALL
    
    -- Check for NULL CONCEPT_DISPLAY
    SELECT 
        'Data Integrity: NULL CONCEPT_DISPLAY' as validation_type,
        'CONCEPT_DISPLAY is a required field in MAPPED_CONCEPTS' as validation_description,
        COUNT(*) as total_mapped_concepts,
        COUNT(DISTINCT SOURCE_CODE_ID) as distinct_source_codes,
        COUNT(CASE WHEN CONCEPT_DISPLAY IS NULL THEN 1 END) as records_with_issue,
        COUNT(CASE WHEN CONCEPT_DISPLAY IS NULL THEN 1 END) = 0 as validation_passed,
        'Records with NULL CONCEPT_DISPLAY in MAPPED_CONCEPTS' as issue_description
    FROM DATA_LAB_NCL_TRAINING_TEMP.CODESETS.MAPPED_CONCEPTS

    UNION ALL

    -- Check that source codes exist in concept map
    SELECT 
        'Referential Integrity: SOURCE_CODE_ID in CONCEPT_MAP' as validation_type,
        'All SOURCE_CODE_IDs in MAPPED_CONCEPTS should exist in CONCEPT_MAP' as validation_description,
        COUNT(*) as total_mapped_concepts,
        COUNT(DISTINCT SOURCE_CODE_ID) as distinct_source_codes,
        COUNT(CASE WHEN NOT EXISTS (
            SELECT 1 
            FROM "Data_Store_OLIDS_Dummy".OLIDS_TERMINOLOGY.CONCEPT_MAP 
            WHERE "source_code_id" = MAPPED_CONCEPTS.SOURCE_CODE_ID
        ) THEN 1 END) as records_with_issue,
        COUNT(CASE WHEN NOT EXISTS (
            SELECT 1 
            FROM "Data_Store_OLIDS_Dummy".OLIDS_TERMINOLOGY.CONCEPT_MAP 
            WHERE "source_code_id" = MAPPED_CONCEPTS.SOURCE_CODE_ID
        ) THEN 1 END) = 0 as validation_passed,
        'Records in MAPPED_CONCEPTS with SOURCE_CODE_ID not found in CONCEPT_MAP' as issue_description
    FROM DATA_LAB_NCL_TRAINING_TEMP.CODESETS.MAPPED_CONCEPTS

    UNION ALL

    -- Check that mapped concepts exist in concept table
    SELECT 
        'Referential Integrity: CONCEPT_CODE in CONCEPT' as validation_type,
        'All CONCEPT_CODEs in MAPPED_CONCEPTS should exist in CONCEPT' as validation_description,
        COUNT(*) as total_mapped_concepts,
        COUNT(DISTINCT SOURCE_CODE_ID) as distinct_source_codes,
        COUNT(CASE WHEN NOT EXISTS (
            SELECT 1 
            FROM "Data_Store_OLIDS_Dummy".OLIDS_TERMINOLOGY.CONCEPT 
            WHERE "code" = MAPPED_CONCEPTS.CONCEPT_CODE
        ) THEN 1 END) as records_with_issue,
        COUNT(CASE WHEN NOT EXISTS (
            SELECT 1 
            FROM "Data_Store_OLIDS_Dummy".OLIDS_TERMINOLOGY.CONCEPT 
            WHERE "code" = MAPPED_CONCEPTS.CONCEPT_CODE
        ) THEN 1 END) = 0 as validation_passed,
        'Records in MAPPED_CONCEPTS with CONCEPT_CODE not found in CONCEPT' as issue_description
    FROM DATA_LAB_NCL_TRAINING_TEMP.CODESETS.MAPPED_CONCEPTS
)
SELECT 
    validation_type as "Validation Type",
    validation_description as "Validation Description",
    total_mapped_concepts as "Total Mapped Concepts",
    distinct_source_codes as "Distinct Source Codes",
    records_with_issue as "Records with Issue",
    ROUND(records_with_issue::FLOAT / total_mapped_concepts * 100, 2) as "Percentage with Issue",
    issue_description as "Issue Description",
    validation_passed as "Validation Passed"
FROM table_validation
ORDER BY validation_passed, validation_type; 