CREATE OR REPLACE DYNAMIC TABLE DATA_LAB_NCL_TRAINING_TEMP.HEI_MIGRATION.INTERMEDIATE_BLOOD_PRESSURE_LATEST(
	PERSON_ID VARCHAR, -- Unique identifier for the person
	CLINICAL_EFFECTIVE_DATE DATE, -- Date of the latest consolidated blood pressure event
	SYSTOLIC_VALUE NUMBER, -- Systolic value from the latest BP event
	DIASTOLIC_VALUE NUMBER, -- Diastolic value from the latest BP event
	IS_HOME_BP_EVENT BOOLEAN, -- Was the latest BP event recorded as a Home BP reading?
	IS_ABPM_BP_EVENT BOOLEAN -- Was the latest BP event recorded as an ABPM reading?
)
COMMENT = 'Intermediate table containing only the single most recent consolidated Blood Pressure event (including SBP, DBP, and context flags) for each person, derived from INTERMEDIATE_BLOOD_PRESSURE_ALL.'
TARGET_LAG = '4 hours'
REFRESH_MODE = AUTO
INITIALIZE = ON_CREATE
WAREHOUSE = NCL_ANALYTICS_XS
AS
SELECT
    PERSON_ID,
    CLINICAL_EFFECTIVE_DATE,
    SYSTOLIC_VALUE,
    DIASTOLIC_VALUE,
    IS_HOME_BP_EVENT,
    IS_ABPM_BP_EVENT
FROM DATA_LAB_NCL_TRAINING_TEMP.HEI_MIGRATION.INTERMEDIATE_BLOOD_PRESSURE_ALL
QUALIFY ROW_NUMBER() OVER (PARTITION BY PERSON_ID ORDER BY CLINICAL_EFFECTIVE_DATE DESC) = 1;
