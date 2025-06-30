create or replace dynamic table DATA_LAB_OLIDS_UAT.HEI_MIGRATION.DIM_PROG_IMM_BASE_POP(
	PERSON_ID VARCHAR, -- Unique identifier for a person
	BIRTH_DATE_APPROX DATE, -- Approximate date of birth from DIM_PERSON_AGE
	AGE NUMBER, -- Age in full years from DIM_PERSON_AGE
	AGE_MONTHS NUMBER, -- Age in total months from DIM_PERSON_AGE
	AGE_DAYS_APPROX NUMBER, -- Approximate age in total days from DIM_PERSON_AGE
	FIRST_BDAY DATE, -- Calculated date of the person's 1st birthday
	SECOND_BDAY DATE, -- Calculated date of the person's 2nd birthday
	THIRD_BDAY DATE, -- Calculated date of the person's 3rd birthday
	FIFTH_BDAY DATE, -- Calculated date of the person's 5th birthday
	SIXTH_BDAY DATE, -- Calculated date of the person's 6th birthday
	IS_PRIMARY_SCHOOL_AGE BOOLEAN, -- Flag: TRUE if person is of primary school age (from DIM_PERSON_AGE)
	IS_SECONDARY_SCHOOL_AGE BOOLEAN, -- Flag: TRUE if person is of secondary school age (from DIM_PERSON_AGE)
	ETHNICITY_CATEGORY VARCHAR, -- Broad ethnicity category from DIM_PERSON_ETHNICITY
	ETHNICITY_SUBCATEGORY VARCHAR, -- More specific ethnicity subcategory from DIM_PERSON_ETHNICITY
	ETHNICITY_GRANULAR VARCHAR, -- Most granular ethnicity detail from DIM_PERSON_ETHNICITY
	LAC_FLAG VARCHAR -- Flag ('Yes'/'No') indicating if the person is a Looked After Child
)
COMMENT = 'Base population dimension for childhood immunisation programs, including individuals aged under 25. Enriches person age details with specific birthday milestones, ethnicity, and a Looked After Child (LAC) flag.'
target_lag = '4 hours'
refresh_mode = AUTO
initialize = ON_CREATE
warehouse = NCL_ANALYTICS_XS
AS
-- THIS IS THE BASE POPULATION AGED UNDER 25 FOR CHILDHOOD IMMS WITH DEMOGRAPHICS
--Find looked after Children using the MAPPEDCONCEPTS view
WITH lac as (
    SELECT DISTINCT -- Use DISTINCT to avoid duplicates if multiple observations exist for the same person
        o."patient_id" as PATIENT_ID,
        pp."person_id" as PERSON_ID
    FROM "Data_Store_OLIDS_Dummy".OLIDS_MASKED.OBSERVATION o
    -- Join OBSERVATION to MAPPEDCONCEPTS using the observation_core_concept_id
    JOIN DATA_LAB_OLIDS_UAT.REFERENCE.MAPPED_CONCEPTS mc
        ON o."observation_core_concept_id" = mc.SOURCE_CODE_ID
    -- Join to PATIENT_PERSON to get the person_id (using INNER JOIN as we need the person)
    JOIN "Data_Store_OLIDS_Dummy".OLIDS_MASKED.PATIENT_PERSON pp
        on pp."patient_id" = o."patient_id"
    -- Filter using the CONCEPT_CODE from the MAPPEDCONCEPTS view
    WHERE CAST(mc.CONCEPT_CODE AS VARCHAR) IN ('764841000000100') -- Looked after Child code
)
--DEFINE MAIN PERSON POPULATION FOR ALL IMMUNISATIONS UNDER 25s for now. Will eventually include all DEMOGRAPHICS
select
    a.PERSON_ID,
    a.BIRTH_DATE_APPROX,
    a.AGE,
    a.AGE_MONTHS,
    a.AGE_DAYS_APPROX,
    DATEADD(YEAR, 1, a.BIRTH_DATE_APPROX) AS FIRST_BDAY,
    DATEADD(YEAR, 2, a.BIRTH_DATE_APPROX) AS SECOND_BDAY,
    DATEADD(YEAR, 3, a.BIRTH_DATE_APPROX) AS THIRD_BDAY,
    DATEADD(YEAR, 5, a.BIRTH_DATE_APPROX) AS FIFTH_BDAY,
    DATEADD(YEAR, 6, a.BIRTH_DATE_APPROX) AS SIXTH_BDAY,
    a.IS_PRIMARY_SCHOOL_AGE,
    a.IS_SECONDARY_SCHOOL_AGE,
    e.ETHNICITY_CATEGORY,
    e.ETHNICITY_SUBCATEGORY,
    e.ETHNICITY_GRANULAR,
    CASE WHEN l.PATIENT_ID IS NULL THEN 'No' ELSE 'Yes' END AS LAC_FLAG
from DATA_LAB_OLIDS_UAT.HEI_MIGRATION.DIM_PERSON_AGE a
LEFT JOIN DATA_LAB_OLIDS_UAT.HEI_MIGRATION.DIM_PERSON_ETHNICITY e on a.PERSON_ID = e.PERSON_ID
LEFT JOIN lac l on l.PERSON_ID = a.PERSON_ID
WHERE a.age_days_approx < 9125;
