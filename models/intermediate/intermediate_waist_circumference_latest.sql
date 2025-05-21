CREATE OR REPLACE DYNAMIC TABLE DATA_LAB_NCL_TRAINING_TEMP.HEI_MIGRATION.INTERMEDIATE_WAIST_CIRCUMFERENCE_LATEST (
    PERSON_ID VARCHAR, -- Unique identifier for the person
    SK_PATIENT_ID VARCHAR, -- Surrogate key for the patient
    OBSERVATION_ID VARCHAR, -- Unique identifier for the observation
    OBSERVATION_DATE DATE, -- Date the measurement was taken
    OBSERVATION_VALUE NUMBER, -- The waist circumference measurement value
    OBSERVATION_UNIT VARCHAR, -- Unit of measurement (cm)
    OBSERVATION_CONCEPT_CODE VARCHAR, -- The concept code for the observation
    OBSERVATION_CONCEPT_DISPLAY VARCHAR, -- The display term for the concept code
    DAYS_SINCE_MEASUREMENT NUMBER, -- Number of days since the measurement was taken
    RECENT_MEASUREMENT_COUNT NUMBER -- Count of measurements in the last 12 months
)
COMMENT = 'Intermediate table containing the latest waist circumference measurement per person. Includes only the most recent measurement from the WAIST_COD cluster.'
TARGET_LAG = '4 hours'
REFRESH_MODE = AUTO
INITIALIZE = ON_CREATE
WAREHOUSE = NCL_ANALYTICS_XS
AS
SELECT 
    PERSON_ID,
    SK_PATIENT_ID,
    OBSERVATION_ID,
    OBSERVATION_DATE,
    OBSERVATION_VALUE,
    OBSERVATION_UNIT,
    OBSERVATION_CONCEPT_CODE,
    OBSERVATION_CONCEPT_DISPLAY,
    DATEDIFF(day, OBSERVATION_DATE, CURRENT_DATE()) AS DAYS_SINCE_MEASUREMENT,
    RECENT_MEASUREMENT_COUNT
FROM DATA_LAB_NCL_TRAINING_TEMP.HEI_MIGRATION.INTERMEDIATE_WAIST_CIRCUMFERENCE_ALL
QUALIFY ROW_NUMBER() OVER (PARTITION BY PERSON_ID ORDER BY OBSERVATION_DATE DESC) = 1; 