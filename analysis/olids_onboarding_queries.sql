/*
OLIDS Onboarding Queries - Working with Source Tables
Purpose: Essential patterns for patient-practice relationships and clinical observations
Note: Uses raw source tables with data quality handling for London dataset

Key concepts:
- person_id: Links same patient across different systems/practices
- patient_id: System-specific identifier for a patient
- PATIENT_PERSON table: Bridge between patient_id and person_id (has duplicates)
- Concept mapping: Clinical codes mapped via CONCEPT_MAP → CONCEPT tables
*/

-- ================================
-- QUERY 1: Patient-Practice Registration History
-- ================================
-- Shows how patients move between practices over time
-- Demonstrates: Handling patient_person duplicates, concept mapping for demographics

SELECT 
    pp."person_id" as person_id,
    pp."patient_id" as patient_id,
    pat."birth_year" as birth_year,
    COALESCE(gender_concept."display", 'Unknown') as gender,
    
    -- Registration details
    reg."start_date"::DATE as registration_start,
    reg."end_date"::DATE as registration_end,
    
    -- Practice information  
    org."organisation_code" as practice_code,
    UPPER(org."name") as practice_name,
    
    -- Registration status
    CASE 
        WHEN reg."end_date" IS NULL THEN 'Current'
        WHEN reg."end_date" > CURRENT_DATE THEN 'Current (future end)'
        ELSE 'Historical'
    END as registration_status,
    
    -- Person's registration sequence (shows practice moves)
    ROW_NUMBER() OVER (
        PARTITION BY pp."person_id" 
        ORDER BY reg."start_date"
    ) as registration_sequence

FROM (
    -- Only include patient_person relationships that have valid patient AND registration records
    SELECT DISTINCT pp."patient_id", pp."person_id"
    FROM "Data_Store_OLIDS_UAT"."OLIDS_MASKED"."PATIENT_PERSON" pp
    INNER JOIN "Data_Store_OLIDS_UAT"."OLIDS_MASKED"."PATIENT" pat
        ON pp."patient_id" = pat."id"
    INNER JOIN "Data_Store_OLIDS_UAT"."OLIDS_MASKED"."PATIENT_REGISTERED_PRACTITIONER_IN_ROLE" reg
        ON pp."patient_id" = reg."patient_id"
    WHERE pp."patient_id" IS NOT NULL 
      AND pp."person_id" IS NOT NULL
      AND pat."id" IS NOT NULL
      AND reg."id" IS NOT NULL
) pp

INNER JOIN "Data_Store_OLIDS_UAT"."OLIDS_MASKED"."PATIENT" pat
    ON pp."patient_id" = pat."id"

-- Example of concept mapping: source_code_id → CONCEPT_MAP → target_code_id → CONCEPT
LEFT JOIN "Data_Store_OLIDS_UAT"."OLIDS_TERMINOLOGY"."CONCEPT_MAP" gender_cm
    ON pat."gender_concept_id" = gender_cm."source_code_id"
LEFT JOIN "Data_Store_OLIDS_UAT"."OLIDS_TERMINOLOGY"."CONCEPT" gender_concept
    ON gender_cm."target_code_id" = gender_concept."id"

INNER JOIN "Data_Store_OLIDS_UAT"."OLIDS_MASKED"."PATIENT_REGISTERED_PRACTITIONER_IN_ROLE" reg
    ON pp."patient_id" = reg."patient_id"

-- Deduplicate organisation table (multiple records per practice code)
INNER JOIN (
    SELECT DISTINCT 
        "id",
        "organisation_code",
        FIRST_VALUE("name") OVER (PARTITION BY "organisation_code" ORDER BY "id") as "name"
    FROM "Data_Store_OLIDS_UAT"."OLIDS_MASKED"."ORGANISATION"
) org
    ON reg."organisation_id" = org."id"

WHERE pp."patient_id" IS NOT NULL 
    AND pp."person_id" IS NOT NULL
    AND reg."start_date" IS NOT NULL
    AND pat."birth_year" IS NOT NULL
    -- Exclude dummy/test patients
    AND COALESCE(pat."is_dummy_patient", FALSE) = FALSE

-- Remove duplicate registration records (same person, same practice, same dates)
QUALIFY ROW_NUMBER() OVER (
    PARTITION BY 
        pp."person_id",
        pp."patient_id", 
        reg."start_date",
        reg."end_date",
        org."organisation_code"
    ORDER BY reg."id"
) = 1

ORDER BY pp."person_id", reg."start_date";


-- ================================
-- QUERY 2: Simple Count of Patients with Clinical Condition by Practice
-- ================================
-- Shows practice-level counts for any clinical condition
-- Demonstrates: SNOMED code filtering, person-level counting

WITH practice_populations AS (
    -- Current practice populations - start from current registrations, get one person_id per patient_id
    SELECT 
        org."organisation_code" as practice_code,
        COUNT(DISTINCT 
            CASE WHEN pp."person_id" IS NOT NULL THEN pp."person_id" END
        ) as current_population
    FROM "Data_Store_OLIDS_UAT"."OLIDS_MASKED"."PATIENT_REGISTERED_PRACTITIONER_IN_ROLE" reg
    INNER JOIN "Data_Store_OLIDS_UAT"."OLIDS_MASKED"."ORGANISATION" org
        ON reg."organisation_id" = org."id"
    LEFT JOIN (
        -- Get the first valid person_id per patient_id
        SELECT DISTINCT 
            pp."patient_id",
            FIRST_VALUE(pp."person_id") OVER (
                PARTITION BY pp."patient_id" 
                ORDER BY pp."person_id"
            ) as "person_id"
        FROM "Data_Store_OLIDS_UAT"."OLIDS_MASKED"."PATIENT_PERSON" pp
        INNER JOIN "Data_Store_OLIDS_UAT"."OLIDS_MASKED"."PATIENT" pat
            ON pp."patient_id" = pat."id"
        WHERE pp."patient_id" IS NOT NULL 
          AND pp."person_id" IS NOT NULL
          AND COALESCE(pat."is_dummy_patient", FALSE) = FALSE
    ) pp ON reg."patient_id" = pp."patient_id"
    WHERE reg."start_date" IS NOT NULL
      AND reg."start_date" <= CURRENT_DATE
      AND (reg."end_date" IS NULL OR reg."end_date" > CURRENT_DATE)
    GROUP BY org."organisation_code"
)

SELECT 
    -- Practice information
    owner_org."organisation_code" as practice_code,
    UPPER(owner_org."name") as practice_name,
    
    -- Practice population
    COALESCE(pop.current_population, 0) as practice_population,
    
    -- Core counts (using person_id for unique patients)
    COUNT(DISTINCT obs."id") as total_observations,
    COUNT(DISTINCT pp."person_id") as unique_patients_with_condition,
    
    -- Prevalence
    ROUND(100.0 * COUNT(DISTINCT pp."person_id") / NULLIF(pop.current_population, 0), 2) as condition_prevalence_pct,
    
    -- Clinical codes found
    COUNT(DISTINCT concept."code") as unique_snomed_codes,
    
    -- Date range for context
    MIN(obs."clinical_effective_date") as earliest_observation,
    MAX(obs."clinical_effective_date") as latest_observation

FROM "Data_Store_OLIDS_UAT"."OLIDS_MASKED"."OBSERVATION" obs

-- Link observations to person_id (for accurate patient counts)
INNER JOIN (
    -- Pick one person_id per patient_id, prioritizing those with valid patient records
    SELECT DISTINCT 
        pp."patient_id", 
        FIRST_VALUE(pp."person_id") OVER (
            PARTITION BY pp."patient_id" 
            ORDER BY CASE WHEN pat."id" IS NOT NULL THEN 1 ELSE 2 END, pp."person_id"
        ) as "person_id"
    FROM "Data_Store_OLIDS_UAT"."OLIDS_MASKED"."PATIENT_PERSON" pp
    LEFT JOIN "Data_Store_OLIDS_UAT"."OLIDS_MASKED"."PATIENT" pat
        ON pp."patient_id" = pat."id"
    WHERE pp."patient_id" IS NOT NULL AND pp."person_id" IS NOT NULL
) pp ON obs."patient_id" = pp."patient_id"

INNER JOIN "Data_Store_OLIDS_UAT"."OLIDS_MASKED"."PATIENT" pat
    ON pp."patient_id" = pat."id"

-- Map observation codes to SNOMED (observation → concept_map → concept)
INNER JOIN "Data_Store_OLIDS_UAT"."OLIDS_TERMINOLOGY"."CONCEPT_MAP" cm
    ON obs."observation_source_concept_id" = cm."source_code_id"
INNER JOIN "Data_Store_OLIDS_UAT"."OLIDS_TERMINOLOGY"."CONCEPT" concept
    ON cm."target_code_id" = concept."id"

-- Get practice information (deduplicated)
INNER JOIN (
    SELECT 
        "organisation_code",
        MAX("name") as "name"
    FROM "Data_Store_OLIDS_UAT"."OLIDS_MASKED"."ORGANISATION"
    GROUP BY "organisation_code"
) owner_org
    ON obs."record_owner_organisation_code" = owner_org."organisation_code"

-- Join practice population data
LEFT JOIN practice_populations pop
    ON owner_org."organisation_code" = pop.practice_code

-- Filter for SNOMED codes of interest (example: diabetes)
WHERE concept."code" IN (
    '73211009',   -- Diabetes mellitus
    '44054006',   -- Diabetes mellitus type 2
    '46635009'    -- Diabetes mellitus type 1
    -- Add your SNOMED codes here
)
AND obs."clinical_effective_date" IS NOT NULL
AND obs."clinical_effective_date" >= '2020-01-01' -- Adjust date range
AND COALESCE(pat."is_dummy_patient", FALSE) = FALSE

GROUP BY 
    owner_org."organisation_code",
    owner_org."name",
    pop.current_population

HAVING COUNT(DISTINCT pp."person_id") >= 5 -- Filter for meaningful counts

ORDER BY 
    unique_patients_with_condition DESC,
    practice_code;


-- ================================
-- QUERY 3: Patients with Clinical Condition by Practice Over Time (Month-End Snapshot)
-- ================================
-- Monthly prevalence tracking for any clinical condition
-- Demonstrates: Time series analysis, point-in-time populations, CTEs for efficiency

WITH analysis_months AS (
    -- Generate 60 monthly time points using Snowflake's GENERATOR
    SELECT DATE_TRUNC('month', DATEADD(month, -SEQ4(), CURRENT_DATE)) as analysis_month
    FROM TABLE(GENERATOR(ROWCOUNT => 60))
),

condition_patients AS (
    -- Find all patients ever diagnosed with condition and when
    SELECT 
        pp."person_id" as person_id,
        obs."record_owner_organisation_code" as practice_code,
        MIN(DATE_TRUNC('month', obs."clinical_effective_date")) as first_diagnosis_month
    FROM "Data_Store_OLIDS_UAT"."OLIDS_MASKED"."OBSERVATION" obs
    
    -- Link to person via deduplicated patient_person bridge
    INNER JOIN (
        SELECT DISTINCT "patient_id", "person_id"
        FROM "Data_Store_OLIDS_UAT"."OLIDS_MASKED"."PATIENT_PERSON"
        WHERE "patient_id" IS NOT NULL AND "person_id" IS NOT NULL
    ) pp ON obs."patient_id" = pp."patient_id"
    
    INNER JOIN "Data_Store_OLIDS_UAT"."OLIDS_MASKED"."PATIENT" pat
        ON pp."patient_id" = pat."id"
    
    -- Map observation codes to SNOMED concepts
    INNER JOIN "Data_Store_OLIDS_UAT"."OLIDS_TERMINOLOGY"."CONCEPT_MAP" cm
        ON obs."observation_source_concept_id" = cm."source_code_id"
    INNER JOIN "Data_Store_OLIDS_UAT"."OLIDS_TERMINOLOGY"."CONCEPT" concept
        ON cm."target_code_id" = concept."id"
    
    -- Filter for specific clinical codes (example: diabetes)
    WHERE concept."code" IN (
        '73211009',   -- Diabetes mellitus
        '44054006',   -- Diabetes mellitus type 2  
        '46635009'    -- Diabetes mellitus type 1
        -- Replace with your codes of interest
    )
    AND obs."clinical_effective_date" IS NOT NULL
    AND COALESCE(pat."is_dummy_patient", FALSE) = FALSE
    
    GROUP BY pp."person_id", obs."record_owner_organisation_code"
),

practice_monthly_populations AS (
    -- Calculate practice list size at each month-end - start from registrations, get one person_id per patient_id  
    SELECT 
        org."organisation_code" as practice_code,
        am.analysis_month,
        COUNT(DISTINCT 
            CASE WHEN pp."person_id" IS NOT NULL THEN pp."person_id" END
        ) as population_count
    FROM analysis_months am
    CROSS JOIN "Data_Store_OLIDS_UAT"."OLIDS_MASKED"."ORGANISATION" org
    LEFT JOIN "Data_Store_OLIDS_UAT"."OLIDS_MASKED"."PATIENT_REGISTERED_PRACTITIONER_IN_ROLE" reg
        ON reg."organisation_id" = org."id"
        AND reg."start_date" IS NOT NULL
        AND LAST_DAY(am.analysis_month) >= reg."start_date"
        AND (reg."end_date" IS NULL OR LAST_DAY(am.analysis_month) <= reg."end_date")
    LEFT JOIN (
        -- Get the first valid person_id per patient_id
        SELECT DISTINCT 
            pp."patient_id",
            FIRST_VALUE(pp."person_id") OVER (
                PARTITION BY pp."patient_id" 
                ORDER BY pp."person_id"
            ) as "person_id"
        FROM "Data_Store_OLIDS_UAT"."OLIDS_MASKED"."PATIENT_PERSON" pp
        INNER JOIN "Data_Store_OLIDS_UAT"."OLIDS_MASKED"."PATIENT" pat
            ON pp."patient_id" = pat."id"
        WHERE pp."patient_id" IS NOT NULL 
          AND pp."person_id" IS NOT NULL
          AND COALESCE(pat."is_dummy_patient", FALSE) = FALSE
    ) pp ON reg."patient_id" = pp."patient_id"
    GROUP BY org."organisation_code", am.analysis_month
),

condition_monthly_summary AS (
    -- Count condition patients at each time point (cumulative prevalence)
    SELECT 
        cp.practice_code,
        am.analysis_month,
        COUNT(DISTINCT cp.person_id) as patients_with_condition,
        COUNT(DISTINCT CASE WHEN cp.first_diagnosis_month = am.analysis_month 
                       THEN cp.person_id END) as newly_diagnosed_patients
    FROM analysis_months am
    CROSS JOIN (SELECT DISTINCT practice_code FROM condition_patients) practices
    LEFT JOIN condition_patients cp
        ON practices.practice_code = cp.practice_code
        AND cp.first_diagnosis_month <= am.analysis_month
    GROUP BY cp.practice_code, am.analysis_month
)

SELECT DISTINCT
    cms.practice_code,
    UPPER(COALESCE(org."name", 'UNKNOWN PRACTICE')) as practice_name,
    cms.analysis_month,
    EXTRACT(YEAR FROM cms.analysis_month) as analysis_year,
    
    -- Practice population
    COALESCE(pmp.population_count, 0) as practice_population,
    
    -- Condition patient counts
    COALESCE(cms.patients_with_condition, 0) as patients_with_condition,
    COALESCE(cms.newly_diagnosed_patients, 0) as newly_diagnosed_patients,
    
    -- Prevalence
    CASE 
        WHEN pmp.population_count > 0 
        THEN ROUND(100.0 * cms.patients_with_condition / pmp.population_count, 2)
        ELSE 0 
    END as condition_prevalence_pct

FROM condition_monthly_summary cms

LEFT JOIN practice_monthly_populations pmp
    ON cms.practice_code = pmp.practice_code
    AND cms.analysis_month = pmp.analysis_month

LEFT JOIN (
    SELECT DISTINCT 
        "organisation_code",
        FIRST_VALUE("name") OVER (PARTITION BY "organisation_code" ORDER BY "id") as "name"
    FROM "Data_Store_OLIDS_UAT"."OLIDS_MASKED"."ORGANISATION"
) org
    ON cms.practice_code = org."organisation_code"

WHERE cms.analysis_month <= DATE_TRUNC('month', CURRENT_DATE)
  AND cms.patients_with_condition > 0

ORDER BY 
    cms.practice_code,
    cms.analysis_month;