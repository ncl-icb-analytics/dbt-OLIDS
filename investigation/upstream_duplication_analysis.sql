-- Investigation: Upstream Duplication in Registration Records
-- Trace where identical registration records are being created

-- 1. Check if duplicates exist in int_patient_registrations
WITH sample_person AS (
    SELECT '001bbbd5-9ccc-4b52-8a3e-cc5d3227e4f7' AS person_id
)
SELECT 
    'int_patient_registrations' as source_table,
    ipr.*
FROM int_patient_registrations ipr
JOIN sample_person sp ON ipr.person_id = sp.person_id
WHERE ipr.is_current_registration = TRUE
ORDER BY ipr.registration_start_date, ipr.registration_record_id;

-- 2. Check the source staging table
WITH sample_person AS (
    SELECT '001bbbd5-9ccc-4b52-8a3e-cc5d3227e4f7' AS person_id
)
SELECT 
    'stg_olids_patient_registered_practitioner_in_role' as source_table,
    prpr.id,
    prpr.patient_id,
    prpr.organisation_id,
    prpr.start_date,
    prpr.end_date,
    prpr.practitioner_id,
    prpr.episode_of_care_id,
    COUNT(*) OVER (PARTITION BY prpr.patient_id, prpr.organisation_id, prpr.start_date) as duplicate_count
FROM stg_olids_patient_registered_practitioner_in_role prpr
JOIN int_patient_person_unique pp ON prpr.patient_id = pp.patient_id
JOIN sample_person sp ON pp.person_id = sp.person_id
ORDER BY prpr.start_date DESC, prpr.id;

-- 3. Check if there are truly identical records in the source
SELECT 
    patient_id,
    organisation_id,
    start_date,
    end_date,
    practitioner_id,
    episode_of_care_id,
    COUNT(*) as record_count
FROM stg_olids_patient_registered_practitioner_in_role
GROUP BY patient_id, organisation_id, start_date, end_date, practitioner_id, episode_of_care_id
HAVING COUNT(*) > 1
ORDER BY record_count DESC
LIMIT 20;

-- 4. Check patient_person mapping for duplicates
WITH sample_person AS (
    SELECT '001bbbd5-9ccc-4b52-8a3e-cc5d3227e4f7' AS person_id
)
SELECT 
    'int_patient_person_unique' as source_table,
    pp.*,
    COUNT(*) OVER (PARTITION BY pp.person_id) as person_patient_count
FROM int_patient_person_unique pp
JOIN sample_person sp ON pp.person_id = sp.person_id;

-- 5. Check if the same patient_id appears multiple times in patient_person bridge
SELECT 
    patient_id,
    COUNT(DISTINCT person_id) as person_count,
    ARRAY_AGG(DISTINCT person_id) as person_ids
FROM stg_olids_patient_person  -- Direct source before deduplication
GROUP BY patient_id
HAVING COUNT(DISTINCT person_id) > 1
ORDER BY person_count DESC
LIMIT 10;

-- 6. Deep dive: Compare all fields for suspected duplicates
WITH sample_person AS (
    SELECT '001bbbd5-9ccc-4b52-8a3e-cc5d3227e4f7' AS person_id
),
sample_registrations AS (
    SELECT 
        prpr.*,
        pp.person_id,
        ROW_NUMBER() OVER (
            PARTITION BY prpr.patient_id, prpr.organisation_id, prpr.start_date 
            ORDER BY prpr.id
        ) as dup_rank
    FROM stg_olids_patient_registered_practitioner_in_role prpr
    JOIN int_patient_person_unique pp ON prpr.patient_id = pp.patient_id
    JOIN sample_person sp ON pp.person_id = sp.person_id
)
SELECT 
    *,
    CASE 
        WHEN dup_rank > 1 THEN 'DUPLICATE'
        ELSE 'ORIGINAL'
    END as record_status
FROM sample_registrations
ORDER BY patient_id, organisation_id, start_date, id;

-- 7. Check if int_patient_registrations has deduplication
SELECT 
    person_id,
    patient_id,
    organisation_id,
    registration_start_date,
    registration_end_date,
    practitioner_id,
    episode_of_care_id,
    registration_record_id,
    COUNT(*) OVER (
        PARTITION BY person_id, organisation_id, registration_start_date, registration_end_date, practitioner_id
    ) as potential_duplicate_count
FROM int_patient_registrations
WHERE person_id = '001bbbd5-9ccc-4b52-8a3e-cc5d3227e4f7'
ORDER BY registration_start_date DESC;