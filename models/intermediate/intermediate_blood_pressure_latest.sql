create or replace dynamic table DATA_LAB_NCL_TRAINING_TEMP.HEI_MIGRATION.INTERMEDIATE_BLOOD_PRESSURE_LATEST(
	PERSON_ID VARCHAR, -- Unique identifier for the person
	CLINICAL_EFFECTIVE_DATE DATE, -- Date of the latest consolidated blood pressure event
	SYSTOLIC_VALUE NUMBER, -- Systolic value from the latest BP event
	DIASTOLIC_VALUE NUMBER, -- Diastolic value from the latest BP event
	IS_HOME_BP_EVENT BOOLEAN, -- Was the latest BP event recorded as a Home BP reading?
	IS_ABPM_BP_EVENT BOOLEAN -- Was the latest BP event recorded as an ABPM reading?
)
COMMENT = 'Intermediate table containing only the single most recent consolidated Blood Pressure event (including SBP, DBP, and context flags) for each person, derived from INTERMEDIATE_BLOOD_PRESSURE_ALL.'
target_lag = '4 hours'
refresh_mode = AUTO
initialize = ON_CREATE
warehouse = NCL_ANALYTICS_XS
as
WITH RankedEvents AS (
    -- Ranks all consolidated BP events for each person based on date.
    -- Selects all relevant columns from the INTERMEDIATE_BLOOD_PRESSURE_ALL table.
    -- Assigns a rank (rn) using ROW_NUMBER(), partitioning by PERSON_ID and ordering by date descending (latest first).
    SELECT
        PERSON_ID,
        CLINICAL_EFFECTIVE_DATE,
        SYSTOLIC_VALUE,
        DIASTOLIC_VALUE,
        IS_HOME_BP_EVENT,
        IS_ABPM_BP_EVENT,
        ROW_NUMBER() OVER (PARTITION BY PERSON_ID ORDER BY CLINICAL_EFFECTIVE_DATE DESC) as rn
    FROM DATA_LAB_NCL_TRAINING_TEMP.HEI_MIGRATION.INTERMEDIATE_BLOOD_PRESSURE_ALL -- Source from the table containing all consolidated BP events.
)
-- Selects only the latest BP event (where rank = 1) for each person.
SELECT
    PERSON_ID,
    CLINICAL_EFFECTIVE_DATE,
    SYSTOLIC_VALUE,
    DIASTOLIC_VALUE,
    IS_HOME_BP_EVENT,
    IS_ABPM_BP_EVENT
FROM RankedEvents
WHERE rn = 1; -- Filters to keep only the row ranked #1 (the latest event) for each person.

