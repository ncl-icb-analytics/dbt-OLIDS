create or replace dynamic table DATA_LAB_NCL_TRAINING_TEMP.HEI_MIGRATION.DIM_PROG_IMM_CHILD_ELIG(
    PERSON_ID VARCHAR, -- Unique identifier for a person
    BIRTH_DATE_APPROX DATE, -- Approximate date of birth from DIM_PROG_IMM_BASE_POP
    AGE NUMBER, -- Age in full years from DIM_PROG_IMM_BASE_POP
    AGE_DAYS_APPROX NUMBER, -- Approximate age in total days from DIM_PROG_IMM_BASE_POP
    FIRST_BDAY DATE, -- Calculated date of the person's 1st birthday
    SECOND_BDAY DATE, -- Calculated date of the person's 2nd birthday
    THIRD_BDAY DATE, -- Calculated date of the person's 3rd birthday
    FIFTH_BDAY DATE, -- Calculated date of the person's 5th birthday
    SIXTH_BDAY DATE, -- Calculated date of the person's 6th birthday
    VACCINE_ID VARCHAR, -- Identifier for the vaccine from the schedule
    VACCINE_NAME VARCHAR, -- Name of the vaccine from the schedule
    DOSE_NUMBER NUMBER, -- Dose number for the vaccine from the schedule
    ELIGIBLE_AGE_FROM_DAYS NUMBER, -- Age in days from which the person is eligible for this vaccine dose
    ELIGIBLE_AGE_TO_DAYS NUMBER, -- Age in days until which the person remains eligible for this vaccine dose
    ELIGIBLE_FROM_DATE DATE, -- Calculated date from which the person is eligible for this vaccine dose
    ELIGIBLE_TO_DATE DATE, -- Calculated date until which the person remains eligible for this vaccine dose
    CURRENTLY_ELIGIBLE VARCHAR -- Flag ('Yes'/'No') indicating if the person is currently within the eligibility window for this vaccine dose
)
COMMENT = 'Dimension table determining eligibility for each childhood immunisation dose for individuals in the immunisation base population. It cross-joins the base population with the immunisation schedule to calculate eligibility windows and current eligibility status.'
target_lag = '4 hours'
refresh_mode = AUTO
initialize = ON_CREATE
warehouse = NCL_ANALYTICS_XS

as
-- Determines eligibility for each vaccine dose in the childhood immunisation schedule
-- for each person in the DIM_PROG_IMM_BASE_POP.
select
    p.PERSON_ID,
    p.BIRTH_DATE_APPROX,
    p.AGE,
    p.AGE_DAYS_APPROX,
    p.FIRST_BDAY,
    p.SECOND_BDAY,
    p.THIRD_BDAY,
    p.FIFTH_BDAY,
    p.SIXTH_BDAY,
    sched.VACCINE_ID,
    sched.VACCINE_NAME,
    sched.DOSE_NUMBER,
    sched.eligible_age_from_days,
    sched.eligible_age_to_days,
    -- Calculates the start date of the eligibility window for the vaccine dose based on birth date and schedule rules.
    DATE((p.BIRTH_DATE_APPROX + sched.eligible_age_from_days)) AS ELIGIBLE_FROM_DATE,
    -- Calculates the end date of the eligibility window for the vaccine dose.
    DATE((p.BIRTH_DATE_APPROX + sched.eligible_age_to_days)) AS ELIGIBLE_TO_DATE,
    -- Determines if the person is currently within the eligibility window for the vaccine dose based on their current age in days.
    CASE
        WHEN p.AGE_DAYS_APPROX >= sched.eligible_age_from_days
             AND p.AGE_DAYS_APPROX <= sched.eligible_age_to_days THEN 'Yes'
        ELSE 'No'
    END AS CURRENTLY_ELIGIBLE
FROM DATA_LAB_NCL_TRAINING_TEMP.HEI_MIGRATION.DIM_PROG_IMM_BASE_POP p
CROSS JOIN -- Creates a row for each person for each vaccine dose in the schedule.
     DATA_LAB_NCL_TRAINING_TEMP.RULESETS.IMMS_SCHEDULE_LATEST sched
WHERE
    -- Filters the base population to include only individuals whose current age in days is greater than or equal to
    -- the minimum eligibility start age (in days) defined in the entire immunisation schedule.
    -- This is an optimisation to avoid processing very young individuals for vaccines they are not yet near eligibility for.
    p.AGE_DAYS_APPROX >= (select min(eligible_age_from_days) from DATA_LAB_NCL_TRAINING_TEMP.RULESETS.IMMS_SCHEDULE_LATEST)
order by p.PERSON_ID, sched.vaccine_id, sched.DOSE_NUMBER; -- Added ORDER BY for consistent output, though not strictly necessary for dynamic table definition
