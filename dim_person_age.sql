create or replace dynamic table DATA_LAB_NCL_TRAINING_TEMP.HEI_MIGRATION.DIM_PERSON_AGE(
    PERSON_ID VARCHAR, -- Unique identifier for a person
    SK_PATIENT_ID VARCHAR, -- Surrogate key for the patient
    BIRTH_YEAR NUMBER, -- Year of birth as recorded for the patient
    BIRTH_MONTH NUMBER, -- Month of birth as recorded for the patient
    BIRTH_DATE_APPROX DATE, -- Approximate date of birth, calculated as the 1st of the birth month/year
    AGE NUMBER, -- Calculated age in full years as of the last refresh
    AGE_MONTHS NUMBER, -- Calculated age in total months as of the last refresh
    AGE_WEEKS_APPROX NUMBER, -- Calculated approximate age in total weeks as of the last refresh
    AGE_DAYS_APPROX NUMBER, -- Calculated approximate age in total days as of the last refresh
    AGE_BAND_5Y VARCHAR, -- 5-year age band (e.g., '0-4', '5-9')
    AGE_BAND_10Y VARCHAR, -- 10-year age band (e.g., '0-9', '10-19')
    AGE_BAND_NHS VARCHAR, -- Age band according to NHS Digital standards
    AGE_BAND_ONS VARCHAR, -- Age band according to ONS (Office for National Statistics) standards
    AGE_LIFE_STAGE VARCHAR, -- Categorisation of age into life stages (e.g., 'Infant', 'Adolescent', 'Adult')
    AGE_SCHOOL_STAGE VARCHAR, -- UK school year/stage (e.g., 'Reception', 'Year 1', 'Post-secondary') based on age at academic year start
    AGE_EDUCATION_LEVEL VARCHAR, -- Broader education level (e.g., 'Primary School', 'Secondary School - KS3/KS4')
    IS_PRIMARY_SCHOOL_AGE BOOLEAN, -- Flag: TRUE if age falls within UK primary school years
    IS_SECONDARY_SCHOOL_AGE BOOLEAN -- Flag: TRUE if age falls within UK secondary school years (including sixth form)
)
COMMENT = 'Dimension table providing comprehensive age-related attributes for each person. Calculates age in various units (years, months, weeks, days) and derives multiple standard age bands, life stages, and UK school stages. Assumes birth date as the 1st of the recorded birth month for calculations.'
target_lag = '4 hours'
refresh_mode = AUTO
initialize = ON_CREATE
warehouse = NCL_ANALYTICS_XS
AS
WITH patient_base AS (
    -- Selects basic patient identifiers (PERSON_ID, SK_PATIENT_ID) and recorded birth year/month.
    -- Calculates an approximate birth date assuming the 1st of the month.
    -- Filters out records where birth year or month is NULL.
    SELECT
        pp."person_id" AS PERSON_ID,
        p."sk_patient_id" AS SK_PATIENT_ID,
        p."birth_year" AS BIRTH_YEAR,    -- Explicitly alias to BIRTH_YEAR (uppercase)
        p."birth_month" AS BIRTH_MONTH,  -- Explicitly alias to BIRTH_MONTH (uppercase)
        -- Calculate approximate birth date (assuming 1st of the month)
        TO_DATE(p."birth_year" || '-' || p."birth_month" || '-01') AS BIRTH_DATE,
        CURRENT_DATE() AS CALCULATION_DATE
    FROM "Data_Store_OLIDS_Dummy".OLIDS_MASKED.PATIENT p
    JOIN "Data_Store_OLIDS_Dummy".OLIDS_MASKED.PATIENT_PERSON pp
        ON p."id" = pp."patient_id"
    WHERE p."birth_year" IS NOT NULL AND p."birth_month" IS NOT NULL
),
age_calculations AS (
    -- Calculates various age metrics based on the approximate birth date and current date.
    -- Includes age in years, total months, approximate weeks, and approximate days.
    -- Also determines the current academic year start for school stage calculations.
    SELECT
        pb.PERSON_ID,
        pb.SK_PATIENT_ID,
        pb.BIRTH_YEAR, -- Reference the explicit alias BIRTH_YEAR
        pb.BIRTH_MONTH, -- Reference the explicit alias BIRTH_MONTH
        pb.BIRTH_DATE, -- Propagate BIRTH_DATE for final selection
        pb.CALCULATION_DATE,
        FLOOR(DATEDIFF(month, pb.BIRTH_DATE, pb.CALCULATION_DATE) / 12) AS AGE,
        DATEDIFF(month, pb.BIRTH_DATE, pb.CALCULATION_DATE) AS AGE_MONTHS,
        FLOOR(DATEDIFF(day, pb.BIRTH_DATE, pb.CALCULATION_DATE) / 7) AS AGE_WEEKS_APPROX,
        DATEDIFF(day, pb.BIRTH_DATE, pb.CALCULATION_DATE) AS AGE_DAYS_APPROX,
        EXTRACT(MONTH FROM pb.CALCULATION_DATE) AS CURRENT_MONTH,
        EXTRACT(YEAR FROM pb.CALCULATION_DATE) AS CURRENT_YEAR,
        CASE
            WHEN EXTRACT(MONTH FROM pb.CALCULATION_DATE) >= 9 THEN EXTRACT(YEAR FROM pb.CALCULATION_DATE)
            ELSE EXTRACT(YEAR FROM pb.CALCULATION_DATE) - 1
        END AS ACADEMIC_YEAR_START,
        EXTRACT(YEAR FROM pb.BIRTH_DATE) AS BIRTH_YEAR_NUMERIC -- Get numeric birth year for calcs if needed later
    FROM patient_base pb
)
-- Final SELECT statement to assemble all age-related attributes for the dimension table.
-- Derives various age bands (5-year, 10-year, NHS, ONS), life stages, and detailed UK school stages/education levels.
SELECT
    ac.PERSON_ID,
    ac.SK_PATIENT_ID,
    ac.BIRTH_YEAR,   -- Select the propagated BIRTH_YEAR
    ac.BIRTH_MONTH,  -- Select the propagated BIRTH_MONTH
    ac.BIRTH_DATE AS BIRTH_DATE_APPROX, -- Select BIRTH_DATE and alias it
    ac.AGE,
    ac.AGE_MONTHS,
    ac.AGE_WEEKS_APPROX,
    ac.AGE_DAYS_APPROX,

    -- Derives 5-year age bands. Handles ages < 0 as 'Unknown' and >= 100 as '100+'.
    CASE
        WHEN ac.AGE < 0 THEN 'Unknown'
        WHEN ac.AGE >= 100 THEN '100+'
        ELSE TO_VARCHAR(FLOOR(ac.AGE / 5) * 5) || '-' || TO_VARCHAR(FLOOR(ac.AGE / 5) * 5 + 4)
    END AS AGE_BAND_5Y,

    -- Derives 10-year age bands. Handles ages < 0 as 'Unknown' and >= 100 as '100+'.
    CASE
        WHEN ac.AGE < 0 THEN 'Unknown'
        WHEN ac.AGE >= 100 THEN '100+'
        ELSE TO_VARCHAR(FLOOR(ac.AGE / 10) * 10) || '-' || TO_VARCHAR(FLOOR(ac.AGE / 10) * 10 + 9)
    END AS AGE_BAND_10Y,

    -- Derives age bands based on NHS Digital standard categories.
    CASE
        WHEN ac.AGE < 0 THEN 'Unknown'
        WHEN ac.AGE < 5 THEN '0-4'
        WHEN ac.AGE < 12 THEN '5-11'
        WHEN ac.AGE < 18 THEN '12-17'
        WHEN ac.AGE < 25 THEN '18-24'
        WHEN ac.AGE < 50 THEN '25-49'
        WHEN ac.AGE < 65 THEN '50-64'
        WHEN ac.AGE < 75 THEN '65-74'
        WHEN ac.AGE < 85 THEN '75-84'
        ELSE '85+'
    END AS AGE_BAND_NHS,

    -- Derives age bands based on ONS (Office for National Statistics) standard categories.
    CASE
        WHEN ac.AGE < 0 THEN 'Unknown'
        WHEN ac.AGE < 5 THEN '0-4'
        WHEN ac.AGE < 10 THEN '5-9'
        WHEN ac.AGE < 15 THEN '10-14'
        WHEN ac.AGE < 20 THEN '15-19'
        WHEN ac.AGE < 25 THEN '20-24'
        WHEN ac.AGE < 30 THEN '25-29'
        WHEN ac.AGE < 35 THEN '30-34'
        WHEN ac.AGE < 40 THEN '35-39'
        WHEN ac.AGE < 45 THEN '40-44'
        WHEN ac.AGE < 50 THEN '45-49'
        WHEN ac.AGE < 55 THEN '50-54'
        WHEN ac.AGE < 60 THEN '55-59'
        WHEN ac.AGE < 65 THEN '60-64'
        WHEN ac.AGE < 70 THEN '65-69'
        WHEN ac.AGE < 75 THEN '70-74'
        WHEN ac.AGE < 80 THEN '75-79'
        WHEN ac.AGE < 85 THEN '80-84'
        ELSE '85+'
    END AS AGE_BAND_ONS,

    -- Categorises individuals into general life stages based on age.
    CASE
        WHEN ac.AGE < 0 THEN 'Unknown'
        WHEN ac.AGE < 1 THEN 'Infant'
        WHEN ac.AGE < 4 THEN 'Toddler'
        WHEN ac.AGE < 13 THEN 'Child'
        WHEN ac.AGE < 20 THEN 'Adolescent'
        WHEN ac.AGE < 25 THEN 'Young Adult'
        WHEN ac.AGE < 60 THEN 'Adult'
        WHEN ac.AGE < 75 THEN 'Older Adult'
        WHEN ac.AGE < 85 THEN 'Elderly'
        ELSE 'Very Elderly'
    END AS AGE_LIFE_STAGE,

    -- Determines UK school year/stage based on the person's age at the start of the current academic year (September 1st).
    -- Logic considers the precise age in months relative to the academic year start for accuracy at transition points.
    CASE
        WHEN ac.AGE < 0 THEN 'Unknown'
        WHEN ac.AGE < 3 THEN 'Pre-school'
         -- Age check based on age at start of academic year (Sept 1st)
        WHEN ac.AGE = 3 OR (ac.AGE = 4 AND DATEDIFF(month, ac.BIRTH_DATE, DATE_FROM_PARTS(ac.ACADEMIC_YEAR_START, 9, 1)) < 48) THEN 'Nursery'
        WHEN ac.AGE = 4 OR (ac.AGE = 5 AND DATEDIFF(month, ac.BIRTH_DATE, DATE_FROM_PARTS(ac.ACADEMIC_YEAR_START, 9, 1)) < 60) THEN 'Reception'
        WHEN ac.AGE = 5 OR (ac.AGE = 6 AND DATEDIFF(month, ac.BIRTH_DATE, DATE_FROM_PARTS(ac.ACADEMIC_YEAR_START, 9, 1)) < 72) THEN 'Year 1'
        WHEN ac.AGE = 6 OR (ac.AGE = 7 AND DATEDIFF(month, ac.BIRTH_DATE, DATE_FROM_PARTS(ac.ACADEMIC_YEAR_START, 9, 1)) < 84) THEN 'Year 2'
        WHEN ac.AGE = 7 OR (ac.AGE = 8 AND DATEDIFF(month, ac.BIRTH_DATE, DATE_FROM_PARTS(ac.ACADEMIC_YEAR_START, 9, 1)) < 96) THEN 'Year 3'
        WHEN ac.AGE = 8 OR (ac.AGE = 9 AND DATEDIFF(month, ac.BIRTH_DATE, DATE_FROM_PARTS(ac.ACADEMIC_YEAR_START, 9, 1)) < 108) THEN 'Year 4'
        WHEN ac.AGE = 9 OR (ac.AGE = 10 AND DATEDIFF(month, ac.BIRTH_DATE, DATE_FROM_PARTS(ac.ACADEMIC_YEAR_START, 9, 1)) < 120) THEN 'Year 5'
        WHEN ac.AGE = 10 OR (ac.AGE = 11 AND DATEDIFF(month, ac.BIRTH_DATE, DATE_FROM_PARTS(ac.ACADEMIC_YEAR_START, 9, 1)) < 132) THEN 'Year 6'
        WHEN ac.AGE = 11 OR (ac.AGE = 12 AND DATEDIFF(month, ac.BIRTH_DATE, DATE_FROM_PARTS(ac.ACADEMIC_YEAR_START, 9, 1)) < 144) THEN 'Year 7'
        WHEN ac.AGE = 12 OR (ac.AGE = 13 AND DATEDIFF(month, ac.BIRTH_DATE, DATE_FROM_PARTS(ac.ACADEMIC_YEAR_START, 9, 1)) < 156) THEN 'Year 8'
        WHEN ac.AGE = 13 OR (ac.AGE = 14 AND DATEDIFF(month, ac.BIRTH_DATE, DATE_FROM_PARTS(ac.ACADEMIC_YEAR_START, 9, 1)) < 168) THEN 'Year 9'
        WHEN ac.AGE = 14 OR (ac.AGE = 15 AND DATEDIFF(month, ac.BIRTH_DATE, DATE_FROM_PARTS(ac.ACADEMIC_YEAR_START, 9, 1)) < 180) THEN 'Year 10'
        WHEN ac.AGE = 15 OR (ac.AGE = 16 AND DATEDIFF(month, ac.BIRTH_DATE, DATE_FROM_PARTS(ac.ACADEMIC_YEAR_START, 9, 1)) < 192) THEN 'Year 11'
        WHEN ac.AGE = 16 OR (ac.AGE = 17 AND DATEDIFF(month, ac.BIRTH_DATE, DATE_FROM_PARTS(ac.ACADEMIC_YEAR_START, 9, 1)) < 204) THEN 'Year 12'
        WHEN ac.AGE = 17 OR (ac.AGE = 18 AND DATEDIFF(month, ac.BIRTH_DATE, DATE_FROM_PARTS(ac.ACADEMIC_YEAR_START, 9, 1)) < 216) THEN 'Year 13'
        WHEN ac.AGE >= 18 THEN 'Post-secondary'
        ELSE 'Unknown'
    END AS AGE_SCHOOL_STAGE,

    -- Derives a broader education level category based on the calculated school stage.
     CASE
        WHEN ac.AGE < 0 THEN 'Unknown'
        WHEN ac.AGE < 3 THEN 'Pre-school'
        WHEN ac.AGE = 3 OR (ac.AGE = 4 AND DATEDIFF(month, ac.BIRTH_DATE, DATE_FROM_PARTS(ac.ACADEMIC_YEAR_START, 9, 1)) < 48) THEN 'Nursery'
        WHEN ac.AGE >= 4 AND (ac.AGE < 11 OR (ac.AGE = 11 AND DATEDIFF(month, ac.BIRTH_DATE, DATE_FROM_PARTS(ac.ACADEMIC_YEAR_START, 9, 1)) < 132)) THEN 'Primary School'
        WHEN ac.AGE >= 11 AND (ac.AGE < 16 OR (ac.AGE = 16 AND DATEDIFF(month, ac.BIRTH_DATE, DATE_FROM_PARTS(ac.ACADEMIC_YEAR_START, 9, 1)) < 192)) THEN 'Secondary School - KS3/KS4'
        WHEN ac.AGE >= 16 AND (ac.AGE < 18 OR (ac.AGE = 18 AND DATEDIFF(month, ac.BIRTH_DATE, DATE_FROM_PARTS(ac.ACADEMIC_YEAR_START, 9, 1)) < 216)) THEN 'Secondary School - Sixth Form'
        WHEN ac.AGE >= 18 THEN 'Post-secondary'
        ELSE 'Unknown'
    END AS AGE_EDUCATION_LEVEL,

    -- Flags if the person is of primary school age based on UK school year definitions.
    CASE
        WHEN ac.AGE >= 4 AND (ac.AGE < 11 OR (ac.AGE = 11 AND DATEDIFF(month, ac.BIRTH_DATE, DATE_FROM_PARTS(ac.ACADEMIC_YEAR_START, 9, 1)) < 132))
        THEN TRUE
        ELSE FALSE
    END AS IS_PRIMARY_SCHOOL_AGE,

    -- Flags if the person is of secondary school age (including sixth form) based on UK school year definitions.
    CASE
         WHEN ac.AGE >= 11 AND (ac.AGE < 18 OR (ac.AGE = 18 AND DATEDIFF(month, ac.BIRTH_DATE, DATE_FROM_PARTS(ac.ACADEMIC_YEAR_START, 9, 1)) < 216))
        THEN TRUE
        ELSE FALSE
    END AS IS_SECONDARY_SCHOOL_AGE

FROM age_calculations ac;