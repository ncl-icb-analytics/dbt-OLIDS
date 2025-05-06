create or replace dynamic table DATA_LAB_NCL_TRAINING_TEMP.HEI_MIGRATION.DIM_PROG_IMM_CHILD_ELIG
target_lag = '4 hours' 
refresh_mode = AUTO 
initialize = ON_CREATE 
warehouse = NCL_ANALYTICS_XS

as
--IMM_BASE_POP ELIGIBLE FOR FIRST VACCINATION
select 
PERSON_ID,
BIRTH_DATE_APPROX,
AGE,
AGE_DAYS_APPROX,
FIRST_BDAY,
SECOND_BDAY,
THIRD_BDAY,
FIFTH_BDAY,
SIXTH_BDAY,
sched.VACCINE_ID,
sched.VACCINE_NAME,
sched.DOSE_NUMBER,
sched.eligible_age_from_days,
sched.eligible_age_to_days,
DATE((BIRTH_DATE_APPROX + sched.eligible_age_from_days)) AS ELIGIBLE_FROM_DATE,
DATE((BIRTH_DATE_APPROX + sched.eligible_age_to_days)) AS ELIGIBLE_TO_DATE,
CASE 
        WHEN AGE_DAYS_APPROX >= sched.eligible_age_from_days
             AND AGE_DAYS_APPROX <= sched.eligible_age_to_days THEN 'Yes' 
        ELSE 'No' 
    END AS CURRENTLY_ELIGIBLE
FROM DATA_LAB_NCL_TRAINING_TEMP.HEI_MIGRATION.DIM_PROG_IMM_BASE_POP p 
CROSS JOIN 
     DATA_LAB_NCL_TRAINING_TEMP.RULESETS.IMMS_SCHEDULE_LATEST sched
WHERE 
AGE_DAYS_APPROX >= (select min(eligible_age_from_days) from DATA_LAB_NCL_TRAINING_TEMP.RULESETS.IMMS_SCHEDULE_LATEST) 
order by vaccine_id