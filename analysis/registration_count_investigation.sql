/*
Investigation: Why Current Registration Counts Are Double Expected Values
Purpose: Identify why current_registered_patients counts are ~2x higher than gender counts
*/

-- ================================
-- ANALYSIS 1: Compare Total vs Gender Counts by Practice
-- ================================
-- This will show us the discrepancy pattern across practices

WITH practice_counts AS (
    SELECT 
        org."organisation_code" as practice_code,
        org."name" as practice_name,
        
        -- Total count (the problematic one)
        COUNT(DISTINCT reg."patient_id") as total_registered_patients,
        
        -- Gender-based counts
        COUNT(DISTINCT CASE WHEN UPPER(gender_concept."display") LIKE '%MALE%' 
                           AND UPPER(gender_concept."display") NOT LIKE '%FEMALE%' 
                           THEN reg."patient_id" END) as male_patients,
        COUNT(DISTINCT CASE WHEN UPPER(gender_concept."display") LIKE '%FEMALE%' 
                           THEN reg."patient_id" END) as female_patients,
        COUNT(DISTINCT CASE WHEN gender_concept."display" IS NULL 
                           OR (UPPER(gender_concept."display") NOT LIKE '%MALE%' 
                               AND UPPER(gender_concept."display") NOT LIKE '%FEMALE%')
                           THEN reg."patient_id" END) as unknown_gender_patients,
        
        -- Check for duplicate registrations per patient
        COUNT(*) as total_registration_records,
        COUNT(DISTINCT reg."patient_id") as unique_patients,
        
        -- Check gender mapping coverage
        COUNT(DISTINCT CASE WHEN gender_concept."display" IS NOT NULL 
                           THEN reg."patient_id" END) as patients_with_gender_mapped

    FROM "Data_Store_OLIDS_UAT"."OLIDS_MASKED"."PATIENT_REGISTERED_PRACTITIONER_IN_ROLE" reg
    
    INNER JOIN "Data_Store_OLIDS_UAT"."OLIDS_MASKED"."ORGANISATION" org
        ON reg."organisation_id" = org."id"
    
    INNER JOIN "Data_Store_OLIDS_UAT"."OLIDS_MASKED"."PATIENT" pat
        ON reg."patient_id" = pat."id"
    
    -- Map gender concept
    LEFT JOIN "Data_Store_OLIDS_UAT"."OLIDS_TERMINOLOGY"."CONCEPT_MAP" gender_cm
        ON pat."gender_concept_id" = gender_cm."source_code_id"
    LEFT JOIN "Data_Store_OLIDS_UAT"."OLIDS_TERMINOLOGY"."CONCEPT" gender_concept
        ON gender_cm."target_code_id" = gender_concept."id"
    
    WHERE -- Current registrations only
        (reg."end_date" IS NULL OR reg."end_date" > CURRENT_DATE)
        AND reg."start_date" IS NOT NULL
        AND pat."birth_year" IS NOT NULL
        AND COALESCE(pat."is_dummy_patient", FALSE) = FALSE
    
    GROUP BY 
        org."organisation_code",
        org."name"
)

SELECT 
    practice_code,
    practice_name,
    total_registered_patients,
    male_patients,
    female_patients,
    unknown_gender_patients,
    -- Calculate the gender sum
    (male_patients + female_patients + unknown_gender_patients) as gender_sum,
    -- Show the discrepancy
    total_registered_patients - (male_patients + female_patients + unknown_gender_patients) as count_discrepancy,
    -- Check for multiple registration records per patient
    total_registration_records,
    unique_patients,
    CASE WHEN total_registration_records = unique_patients THEN 'No duplicates' ELSE 'Has duplicates' END as duplicate_status,
    -- Gender mapping coverage
    patients_with_gender_mapped,
    ROUND(100.0 * patients_with_gender_mapped / NULLIF(total_registered_patients, 0), 1) as pct_with_gender_mapped

FROM practice_counts

WHERE total_registered_patients >= 1000 -- Focus on larger practices

ORDER BY count_discrepancy DESC, total_registered_patients DESC;


-- ================================
-- ANALYSIS 2: Investigate Duplicate Registration Records
-- ================================
-- Check if patients have multiple current registrations at the same practice

WITH duplicate_registrations AS (
    SELECT 
        org."organisation_code" as practice_code,
        org."name" as practice_name,
        reg."patient_id" as patient_id,
        COUNT(*) as registration_count,
        MIN(reg."start_date") as earliest_start_date,
        MAX(reg."start_date") as latest_start_date,
        LISTAGG(DISTINCT reg."id", ', ') as registration_record_ids

    FROM "Data_Store_OLIDS_UAT"."OLIDS_MASKED"."PATIENT_REGISTERED_PRACTITIONER_IN_ROLE" reg
    
    INNER JOIN "Data_Store_OLIDS_UAT"."OLIDS_MASKED"."ORGANISATION" org
        ON reg."organisation_id" = org."id"
    
    INNER JOIN "Data_Store_OLIDS_UAT"."OLIDS_MASKED"."PATIENT" pat
        ON reg."patient_id" = pat."id"
    
    WHERE -- Current registrations only
        (reg."end_date" IS NULL OR reg."end_date" > CURRENT_DATE)
        AND reg."start_date" IS NOT NULL
        AND pat."birth_year" IS NOT NULL
        AND COALESCE(pat."is_dummy_patient", FALSE) = FALSE
    
    GROUP BY 
        org."organisation_code",
        org."name",
        reg."patient_id"
        
    HAVING COUNT(*) > 1 -- Only patients with multiple current registrations
)

SELECT 
    'Multiple Current Registrations per Patient' as analysis_type,
    practice_code,
    practice_name,
    COUNT(DISTINCT patient_id) as patients_with_multiple_registrations,
    AVG(registration_count) as avg_registrations_per_patient,
    MAX(registration_count) as max_registrations_per_patient

FROM duplicate_registrations

GROUP BY practice_code, practice_name

ORDER BY patients_with_multiple_registrations DESC;


-- ================================
-- ANALYSIS 3: Investigate Gender Concept Mapping Issues
-- ================================
-- Check if the gender concept mapping is causing the issue

WITH gender_mapping_analysis AS (
    SELECT 
        org."organisation_code" as practice_code,
        org."name" as practice_name,
        reg."patient_id" as patient_id,
        pat."gender_concept_id",
        gender_cm."source_code_id" as gender_source_id,
        gender_cm."target_code_id" as gender_target_id,
        gender_concept."code" as gender_snomed_code,
        gender_concept."display" as gender_display,
        
        -- Check if patient appears multiple times due to mapping issues
        COUNT(*) OVER (PARTITION BY org."organisation_code", reg."patient_id") as patient_record_count

    FROM "Data_Store_OLIDS_UAT"."OLIDS_MASKED"."PATIENT_REGISTERED_PRACTITIONER_IN_ROLE" reg
    
    INNER JOIN "Data_Store_OLIDS_UAT"."OLIDS_MASKED"."ORGANISATION" org
        ON reg."organisation_id" = org."id"
    
    INNER JOIN "Data_Store_OLIDS_UAT"."OLIDS_MASKED"."PATIENT" pat
        ON reg."patient_id" = pat."id"
    
    -- Map gender concept (using LEFT JOIN to see unmapped records)
    LEFT JOIN "Data_Store_OLIDS_UAT"."OLIDS_TERMINOLOGY"."CONCEPT_MAP" gender_cm
        ON pat."gender_concept_id" = gender_cm."source_code_id"
    LEFT JOIN "Data_Store_OLIDS_UAT"."OLIDS_TERMINOLOGY"."CONCEPT" gender_concept
        ON gender_cm."target_code_id" = gender_concept."id"
    
    WHERE -- Current registrations only
        (reg."end_date" IS NULL OR reg."end_date" > CURRENT_DATE)
        AND reg."start_date" IS NOT NULL
        AND pat."birth_year" IS NOT NULL
        AND COALESCE(pat."is_dummy_patient", FALSE) = FALSE
        AND org."organisation_code" IN ('F85002') -- Focus on the example practice
)

SELECT 
    'Gender Mapping Analysis for Practice F85002' as analysis_type,
    gender_concept_id,
    gender_source_id,
    gender_target_id,
    gender_snomed_code,
    gender_display,
    COUNT(DISTINCT patient_id) as unique_patients,
    COUNT(*) as total_records,
    AVG(patient_record_count) as avg_records_per_patient

FROM gender_mapping_analysis

GROUP BY 
    gender_concept_id,
    gender_source_id,
    gender_target_id,
    gender_snomed_code,
    gender_display

ORDER BY total_records DESC;


-- ================================
-- ANALYSIS 4: Check for Cross-Join Effects in Original Query
-- ================================
-- Replicate the issue to see if it's a JOIN problem

WITH problematic_query_debug AS (
    SELECT 
        org."organisation_code" as practice_code,
        reg."patient_id",
        pat."gender_concept_id",
        gender_cm."source_code_id",
        gender_cm."target_code_id",
        COUNT(*) as record_count

    FROM "Data_Store_OLIDS_UAT"."OLIDS_MASKED"."PATIENT_REGISTERED_PRACTITIONER_IN_ROLE" reg
    
    INNER JOIN "Data_Store_OLIDS_UAT"."OLIDS_MASKED"."ORGANISATION" org
        ON reg."organisation_id" = org."id"
    
    INNER JOIN "Data_Store_OLIDS_UAT"."OLIDS_MASKED"."PATIENT" pat
        ON reg."patient_id" = pat."id"
    
    -- Map gender concept
    LEFT JOIN "Data_Store_OLIDS_UAT"."OLIDS_TERMINOLOGY"."CONCEPT_MAP" gender_cm
        ON pat."gender_concept_id" = gender_cm."source_code_id"
    
    WHERE -- Current registrations only
        (reg."end_date" IS NULL OR reg."end_date" > CURRENT_DATE)
        AND reg."start_date" IS NOT NULL
        AND pat."birth_year" IS NOT NULL
        AND COALESCE(pat."is_dummy_patient", FALSE) = FALSE
        AND org."organisation_code" IN ('F85002') -- Focus on problem practice
    
    GROUP BY 
        org."organisation_code",
        reg."patient_id",
        pat."gender_concept_id",
        gender_cm."source_code_id",
        gender_cm."target_code_id"
    
    HAVING COUNT(*) > 1 -- Find patients appearing multiple times
)

SELECT 
    'Patients with Multiple Records in Join' as analysis_type,
    practice_code,
    COUNT(DISTINCT patient_id) as patients_with_multiple_records,
    AVG(record_count) as avg_records_per_patient,
    MAX(record_count) as max_records_per_patient

FROM problematic_query_debug

GROUP BY practice_code;


-- ================================
-- ANALYSIS 5: Patient vs Person Count Comparison
-- ================================
-- Check if we're double-counting due to multiple patient_ids per person

WITH patient_vs_person_analysis AS (
    SELECT 
        org."organisation_code" as practice_code,
        org."name" as practice_name,
        
        -- Patient-based counting (what Query 3 currently does)
        COUNT(DISTINCT reg."patient_id") as unique_patient_ids,
        
        -- Person-based counting (what we probably should do)
        COUNT(DISTINCT pp."person_id") as unique_person_ids,
        
        -- Total registration records
        COUNT(*) as total_registration_records,
        
        -- Gender counts by person_id (correct approach)
        COUNT(DISTINCT CASE WHEN UPPER(gender_concept."display") LIKE '%MALE%' 
                           AND UPPER(gender_concept."display") NOT LIKE '%FEMALE%' 
                           THEN pp."person_id" END) as male_persons,
        COUNT(DISTINCT CASE WHEN UPPER(gender_concept."display") LIKE '%FEMALE%' 
                           THEN pp."person_id" END) as female_persons,
        COUNT(DISTINCT CASE WHEN gender_concept."display" IS NULL 
                           OR (UPPER(gender_concept."display") NOT LIKE '%MALE%' 
                               AND UPPER(gender_concept."display") NOT LIKE '%FEMALE%')
                           THEN pp."person_id" END) as unknown_gender_persons,
                           
        -- Check for multiple patient_ids per person
        COUNT(DISTINCT pp."person_id") as persons_with_registrations,
        COUNT(DISTINCT reg."patient_id") as patients_with_registrations,
        ROUND(COUNT(DISTINCT reg."patient_id") / NULLIF(COUNT(DISTINCT pp."person_id"), 0), 2) as avg_patient_ids_per_person

    FROM "Data_Store_OLIDS_UAT"."OLIDS_MASKED"."PATIENT_REGISTERED_PRACTITIONER_IN_ROLE" reg
    
    INNER JOIN "Data_Store_OLIDS_UAT"."OLIDS_MASKED"."ORGANISATION" org
        ON reg."organisation_id" = org."id"
    
    INNER JOIN "Data_Store_OLIDS_UAT"."OLIDS_MASKED"."PATIENT" pat
        ON reg."patient_id" = pat."id"
        
    -- Join to patient_person to get person_id
    INNER JOIN "Data_Store_OLIDS_UAT"."OLIDS_MASKED"."PATIENT_PERSON" pp
        ON reg."patient_id" = pp."patient_id"
    
    -- Map gender concept
    LEFT JOIN "Data_Store_OLIDS_UAT"."OLIDS_TERMINOLOGY"."CONCEPT_MAP" gender_cm
        ON pat."gender_concept_id" = gender_cm."source_code_id"
    LEFT JOIN "Data_Store_OLIDS_UAT"."OLIDS_TERMINOLOGY"."CONCEPT" gender_concept
        ON gender_cm."target_code_id" = gender_concept."id"
    
    WHERE -- Current registrations only
        (reg."end_date" IS NULL OR reg."end_date" > CURRENT_DATE)
        AND reg."start_date" IS NOT NULL
        AND pat."birth_year" IS NOT NULL
        AND COALESCE(pat."is_dummy_patient", FALSE) = FALSE
    
    GROUP BY 
        org."organisation_code",
        org."name"
)

SELECT 
    practice_code,
    practice_name,
    unique_patient_ids,
    unique_person_ids,
    -- Show the difference
    unique_patient_ids - unique_person_ids as patient_person_difference,
    ROUND(100.0 * (unique_patient_ids - unique_person_ids) / NULLIF(unique_person_ids, 0), 1) as pct_difference,
    avg_patient_ids_per_person,
    
    -- Gender counts by person (correct)
    male_persons,
    female_persons,
    unknown_gender_persons,
    (male_persons + female_persons + unknown_gender_persons) as total_gender_sum,
    
    -- Compare with person count
    unique_person_ids - (male_persons + female_persons + unknown_gender_persons) as person_gender_discrepancy

FROM patient_vs_person_analysis

WHERE unique_person_ids >= 1000 -- Focus on larger practices

ORDER BY patient_person_difference DESC;


-- ================================  
-- ANALYSIS 6: Examples of Multiple Patient IDs per Person
-- ================================
-- Show specific examples of persons with multiple patient_ids at same practice

WITH multiple_patients_per_person AS (
    SELECT 
        org."organisation_code" as practice_code,
        pp."person_id" as person_id,
        COUNT(DISTINCT reg."patient_id") as patient_count,
        LISTAGG(DISTINCT reg."patient_id", ', ') as patient_ids,
        MIN(reg."start_date") as earliest_registration,
        MAX(reg."start_date") as latest_registration

    FROM "Data_Store_OLIDS_UAT"."OLIDS_MASKED"."PATIENT_REGISTERED_PRACTITIONER_IN_ROLE" reg
    
    INNER JOIN "Data_Store_OLIDS_UAT"."OLIDS_MASKED"."ORGANISATION" org
        ON reg."organisation_id" = org."id"
    
    INNER JOIN "Data_Store_OLIDS_UAT"."OLIDS_MASKED"."PATIENT" pat
        ON reg."patient_id" = pat."id"
        
    INNER JOIN "Data_Store_OLIDS_UAT"."OLIDS_MASKED"."PATIENT_PERSON" pp
        ON reg."patient_id" = pp."patient_id"
    
    WHERE -- Current registrations only
        (reg."end_date" IS NULL OR reg."end_date" > CURRENT_DATE)
        AND reg."start_date" IS NOT NULL
        AND pat."birth_year" IS NOT NULL
        AND COALESCE(pat."is_dummy_patient", FALSE) = FALSE
        AND org."organisation_code" IN ('F83059') -- Focus on Brondesbury with the major issue
    
    GROUP BY 
        org."organisation_code",
        pp."person_id"
        
    HAVING COUNT(DISTINCT reg."patient_id") > 1
)

SELECT 
    'Examples: Multiple Patient IDs per Person at F83059' as analysis_type,
    practice_code,
    person_id,
    patient_count,
    patient_ids,
    earliest_registration,
    latest_registration,
    DATEDIFF('day', earliest_registration, latest_registration) as days_between_registrations

FROM multiple_patients_per_person

ORDER BY patient_count DESC, days_between_registrations DESC

LIMIT 20; -- Show top 20 examples


-- ================================  
-- ANALYSIS 7: Reverse Check - Multiple Persons per Patient ID
-- ================================
-- Check if multiple person_ids map to the same patient_id (the reverse problem)

WITH multiple_persons_per_patient AS (
    SELECT 
        org."organisation_code" as practice_code,
        reg."patient_id" as patient_id,
        COUNT(DISTINCT pp."person_id") as person_count,
        LISTAGG(DISTINCT pp."person_id", ', ') as person_ids,
        MIN(reg."start_date") as earliest_registration,
        MAX(reg."start_date") as latest_registration

    FROM "Data_Store_OLIDS_UAT"."OLIDS_MASKED"."PATIENT_REGISTERED_PRACTITIONER_IN_ROLE" reg
    
    INNER JOIN "Data_Store_OLIDS_UAT"."OLIDS_MASKED"."ORGANISATION" org
        ON reg."organisation_id" = org."id"
    
    INNER JOIN "Data_Store_OLIDS_UAT"."OLIDS_MASKED"."PATIENT" pat
        ON reg."patient_id" = pat."id"
        
    INNER JOIN "Data_Store_OLIDS_UAT"."OLIDS_MASKED"."PATIENT_PERSON" pp
        ON reg."patient_id" = pp."patient_id"
    
    WHERE -- Current registrations only
        (reg."end_date" IS NULL OR reg."end_date" > CURRENT_DATE)
        AND reg."start_date" IS NOT NULL
        AND pat."birth_year" IS NOT NULL
        AND COALESCE(pat."is_dummy_patient", FALSE) = FALSE
        AND org."organisation_code" IN ('F83059') -- Focus on Brondesbury
    
    GROUP BY 
        org."organisation_code",
        reg."patient_id"
        
    HAVING COUNT(DISTINCT pp."person_id") > 1
)

SELECT 
    'Examples: Multiple Person IDs per Patient at F83059' as analysis_type,
    practice_code,
    patient_id,
    person_count,
    person_ids,
    earliest_registration,
    latest_registration,
    DATEDIFF('day', earliest_registration, latest_registration) as days_between_registrations

FROM multiple_persons_per_patient

ORDER BY person_count DESC, days_between_registrations DESC

LIMIT 20;


-- ================================  
-- ANALYSIS 8: Patient-Person Mapping Quality Check
-- ================================
-- Check for orphaned records and mapping issues at Brondesbury

WITH mapping_quality_check AS (
    SELECT 
        'Total Registration Records' as record_type,
        COUNT(*) as count_value
    FROM "Data_Store_OLIDS_UAT"."OLIDS_MASKED"."PATIENT_REGISTERED_PRACTITIONER_IN_ROLE" reg
    INNER JOIN "Data_Store_OLIDS_UAT"."OLIDS_MASKED"."ORGANISATION" org ON reg."organisation_id" = org."id"
    INNER JOIN "Data_Store_OLIDS_UAT"."OLIDS_MASKED"."PATIENT" pat ON reg."patient_id" = pat."id"
    WHERE org."organisation_code" = 'F83059'
        AND (reg."end_date" IS NULL OR reg."end_date" > CURRENT_DATE)
        AND reg."start_date" IS NOT NULL
        AND pat."birth_year" IS NOT NULL
        AND COALESCE(pat."is_dummy_patient", FALSE) = FALSE
        
    UNION ALL
    
    SELECT 
        'Records with Patient-Person Mapping' as record_type,
        COUNT(*) as count_value
    FROM "Data_Store_OLIDS_UAT"."OLIDS_MASKED"."PATIENT_REGISTERED_PRACTITIONER_IN_ROLE" reg
    INNER JOIN "Data_Store_OLIDS_UAT"."OLIDS_MASKED"."ORGANISATION" org ON reg."organisation_id" = org."id"
    INNER JOIN "Data_Store_OLIDS_UAT"."OLIDS_MASKED"."PATIENT" pat ON reg."patient_id" = pat."id"
    INNER JOIN "Data_Store_OLIDS_UAT"."OLIDS_MASKED"."PATIENT_PERSON" pp ON reg."patient_id" = pp."patient_id"
    WHERE org."organisation_code" = 'F83059'
        AND (reg."end_date" IS NULL OR reg."end_date" > CURRENT_DATE)
        AND reg."start_date" IS NOT NULL
        AND pat."birth_year" IS NOT NULL
        AND COALESCE(pat."is_dummy_patient", FALSE) = FALSE
        
    UNION ALL
    
    SELECT 
        'Unique Patient IDs' as record_type,
        COUNT(DISTINCT reg."patient_id") as count_value
    FROM "Data_Store_OLIDS_UAT"."OLIDS_MASKED"."PATIENT_REGISTERED_PRACTITIONER_IN_ROLE" reg
    INNER JOIN "Data_Store_OLIDS_UAT"."OLIDS_MASKED"."ORGANISATION" org ON reg."organisation_id" = org."id"
    INNER JOIN "Data_Store_OLIDS_UAT"."OLIDS_MASKED"."PATIENT" pat ON reg."patient_id" = pat."id"
    WHERE org."organisation_code" = 'F83059'
        AND (reg."end_date" IS NULL OR reg."end_date" > CURRENT_DATE)
        AND reg."start_date" IS NOT NULL
        AND pat."birth_year" IS NOT NULL
        AND COALESCE(pat."is_dummy_patient", FALSE) = FALSE
        
    UNION ALL
    
    SELECT 
        'Unique Person IDs (with mapping)' as record_type,
        COUNT(DISTINCT pp."person_id") as count_value
    FROM "Data_Store_OLIDS_UAT"."OLIDS_MASKED"."PATIENT_REGISTERED_PRACTITIONER_IN_ROLE" reg
    INNER JOIN "Data_Store_OLIDS_UAT"."OLIDS_MASKED"."ORGANISATION" org ON reg."organisation_id" = org."id"
    INNER JOIN "Data_Store_OLIDS_UAT"."OLIDS_MASKED"."PATIENT" pat ON reg."patient_id" = pat."id"
    INNER JOIN "Data_Store_OLIDS_UAT"."OLIDS_MASKED"."PATIENT_PERSON" pp ON reg."patient_id" = pp."patient_id"
    WHERE org."organisation_code" = 'F83059'
        AND (reg."end_date" IS NULL OR reg."end_date" > CURRENT_DATE)
        AND reg."start_date" IS NOT NULL
        AND pat."birth_year" IS NOT NULL
        AND COALESCE(pat."is_dummy_patient", FALSE) = FALSE
)

SELECT * FROM mapping_quality_check ORDER BY record_type;