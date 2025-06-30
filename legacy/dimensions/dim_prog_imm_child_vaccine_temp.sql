create or replace dynamic table DATA_LAB_OLIDS_UAT.HEI_MIGRATION.DIM_PROG_IMM_CHILD_VACCINE_TEMP(
	PERSON_ID,
	AGE,
	VACCINE_ORDER,
	VACCINE_ID,
	VACCINE_NAME,
	DOSE_NUMBER,
	EVENT_TYPE,
	EVENT_DATE,
	OUT_OF_SCHEDULE
) target_lag = '4 hours' refresh_mode = AUTO initialize = ON_CREATE warehouse = NCL_ANALYTICS_XS

 as
--This query looks for vaccine events
--First ensure that the correct clusters are used for the correct doses
with codecluster as (
select * from (
select
sched.VACCINE_ORDER,
sched.vaccine_id,
clut.vaccine,
clut.SNOMEDCONCEPTID as code,
clut.PROPOSEDCLUSTER as CodeClusterId,
sched.administered_cluster_id,
sched.drug_cluster_id,
sched.declined_cluster_id,
sched.contraindicated_cluster_id ,
CASE
WHEN clut.VACCINE = 'DTaP/IPV/Hib/HepB/6-in-1' and clut.dose = '1,2,3' and sched.dose_number = '1' THEN '1'
WHEN clut.VACCINE = 'DTaP/IPV/Hib/HepB/6-in-1' and clut.dose = '1,2,3' and sched.dose_number = '2' THEN '2'
WHEN clut.VACCINE = 'DTaP/IPV/Hib/HepB/6-in-1' and clut.dose = '1,2,3' and sched.dose_number = '3'THEN '3'
WHEN clut.VACCINE = 'MenB' and clut.dose = '1,2,3' and sched.dose_number = '1' THEN '1'
WHEN clut.VACCINE = 'MenB' and clut.dose = '1,2,3' and sched.dose_number = '2' THEN '2'
WHEN clut.VACCINE = 'MenB' and clut.dose = '1,2,3' and sched.dose_number = '3' THEN '3'
WHEN clut.VACCINE = 'Hib/MenC' and clut.dose = '1,2' and sched.dose_number = '1' THEN '1'
WHEN clut.VACCINE = 'HPV' and clut.dose in ('1,2,3') and sched.dose_number = '1' THEN '1'
WHEN clut.VACCINE = 'HPV' and clut.dose in ('1,2,3') and sched.dose_number = '2' THEN '2'
WHEN clut.VACCINE = 'MMR' and clut.dose = '1,2' and sched.dose_number = '1' THEN '1'
WHEN clut.VACCINE = 'MMR' and clut.dose = '1,2' and sched.dose_number = '2' THEN '2'
WHEN clut.VACCINE = 'PCV' and clut.dose = '1,2' and sched.dose_number = '1' THEN '1'
WHEN clut.VACCINE = 'PCV' and clut.dose = '1,2' and sched.dose_number = '2' THEN '2'
WHEN clut.VACCINE = 'Rotavirus' and clut.dose = '1,2' and sched.dose_number = '1' THEN '1'
WHEN clut.VACCINE = 'Rotavirus' and clut.dose = '1,2' and sched.dose_number = '2' THEN '2'
WHEN clut.VACCINE = sched.VACCINE_NAME AND clut.DOSE = sched.DOSE_NUMBER THEN clut.dose
END as DOSE_MATCH
from DATA_LAB_OLIDS_UAT.REFERENCE.CHILDHOOD_IMMS_CODES  clut
inner join DATA_LAB_OLIDS_UAT.RULESETS.IMMS_SCHEDULE_LATEST sched ON
            (sched.administered_cluster_id = clut.proposedcluster OR
            sched.drug_cluster_id = clut.proposedcluster OR
            sched.declined_cluster_id = clut.proposedcluster OR
            sched.contraindicated_cluster_id = clut.proposedcluster)

order by vaccine, code, dose_match
) a
where a.dose_match is not null
)
--Find vaccination events from observation table using mappedconcepts and codecluster
,IMMS_CODE_OBS as (
-- Use DISTINCT to avoid duplicates if multiple observations exist for the same person
SELECT DISTINCT
        o."patient_id" as PATIENT_ID,
        pp."person_id" as PERSON_ID,
        clut.VACCINE_ORDER,
        clut.vaccine,
        clut.vaccine_id,
        clut.CODECLUSTERID,
        clut.dose_match,
        TO_DATE(o."clinical_effective_date") as EVENT_DATE,
        clut.administered_cluster_id,
        clut.drug_cluster_id,
        clut.declined_cluster_id,
        clut.contraindicated_cluster_id
    FROM "Data_Store_OLIDS_Dummy".OLIDS_MASKED.OBSERVATION o
    -- Join to PATIENT_PERSON to get the person_id (using INNER JOIN as we need the person)
    JOIN "Data_Store_OLIDS_Dummy".OLIDS_MASKED.PATIENT_PERSON pp on pp."patient_id" = o."patient_id"
    -- Join OBSERVATION to MAPPEDCONCEPTS using the observation_core_concept_id
    JOIN DATA_LAB_OLIDS_UAT.REFERENCE.MAPPED_CONCEPTS mc ON o."observation_core_concept_id" = mc.SOURCE_CODE_ID
    -- Join CONCEPT_CODE from the MAPPEDCONCEPTS to the IMMS_CODE_DOSEMATCH making sure clut.code is VARCHAR (currently is number)
    JOIN codecluster clut on mc.CONCEPT_CODE  = CAST(clut.CODE AS VARCHAR)
    WHERE TO_DATE(o."clinical_effective_date") <= CURRENT_DATE
    )

--Define Vaccination Events look for ADMIN CODES by matching IMMS_CODE_OBS to ELIGIBLE POP
,IMM_ADM as (
     SELECT distinct
        el.PERSON_ID,
	    el.AGE,
        clut.VACCINE_ORDER,
        el.VACCINE_ID,
        el.VACCINE_NAME,
        el.DOSE_NUMBER,
        el.ELIGIBLE_AGE_FROM_DAYS,
        el.ELIGIBLE_AGE_TO_DAYS,
        clut.EVENT_DATE,
            CASE
            WHEN clut.codeclusterid = clut.administered_cluster_id THEN 'Administration'
            WHEN clut.codeclusterid = clut.drug_cluster_id THEN 'Administration'
            WHEN clut.codeclusterid = clut.Contraindicated_Cluster_ID THEN 'Contraindicated'
            WHEN clut.codeclusterid = clut.Declined_Cluster_ID THEN 'Declined'
            ELSE 'Other'
        END AS EVENT_TYPE,
         -- Determine if the event was out of schedule (only for Administration)
        CASE
            WHEN clut.codeclusterid = clut.administered_cluster_id
            AND datediff(day,el.BIRTH_DATE_APPROX,clut.event_date) > el.ELIGIBLE_AGE_TO_DAYS THEN 'Yes'
            WHEN clut.codeclusterid = clut.administered_cluster_id
            AND datediff(day,el.BIRTH_DATE_APPROX,clut.event_date) < el.ELIGIBLE_AGE_FROM_DAYS THEN 'Yes'
            ELSE 'No'
        END AS OUT_OF_SCHEDULE
    FROM DATA_LAB_OLIDS_UAT.HEI_MIGRATION.DIM_PROG_IMM_CHILD_ELIG el
    INNER JOIN IMMS_CODE_OBS clut on clut.PERSON_ID = el.PERSON_ID
    AND el.DOSE_NUMBER = clut.DOSE_MATCH
    and el.VACCINE_NAME = clut.VACCINE
    and el.VACCINE_ID = clut.VACCINE_ID
       --imms date must be greater than BirthDate
    and clut.EVENT_DATE > el.BIRTH_DATE_APPROX
    AND el.age < 19
   )
,IMM_ADM_CLUSTER as (
--IDENTIFY DUPLICATE ROWS WHERE DECLINED OR CONTRAINDICATED AND ADMINSTRATION ON THE SAME DATE
SELECT
    PERSON_ID,
    AGE,
    VACCINE_ORDER,
    VACCINE_ID,
    VACCINE_NAME,
    DOSE_NUMBER,
    EVENT_DATE,
    EVENT_TYPE,
    OUT_OF_SCHEDULE,
    RANK() OVER (
		PARTITION BY PERSON_ID, VACCINE_ID ORDER BY CASE
				WHEN EVENT_TYPE = 'Declined'
					THEN 0
          WHEN EVENT_TYPE = 'Contraindicated'
					THEN 0
				WHEN EVENT_TYPE = 'Administration'
					THEN 1
				END DESC ) r
    FROM IMM_ADM
    )
--IDENTIFY DUPLICATE ROWS WHERE SAME CODE CAN BE USED FOR DIFFERENT DOSES
,IMM_ADM_RANKED as (
SELECT
	PERSON_ID,
	AGE,
    VACCINE_ORDER,
	VACCINE_ID,
	VACCINE_NAME,
	DOSE_NUMBER,
	EVENT_TYPE,
	EVENT_DATE,
	OUT_OF_SCHEDULE,
       ROW_NUMBER() OVER (PARTITION BY PERSON_ID, VACCINE_ID ORDER BY EVENT_DATE ASC) AS row_num,
       COUNT(*) OVER (PARTITION BY PERSON_ID, VACCINE_ID, EVENT_TYPE) AS TOTAL_EVENTS
    FROM imm_adm_cluster
    where r = 1
   )
--SELECT FINAL DATASET
 SELECT
	PERSON_ID,
	AGE,
    VACCINE_ORDER,
	VACCINE_ID,
	VACCINE_NAME,
	DOSE_NUMBER,
	EVENT_TYPE,
	EVENT_DATE,
	OUT_OF_SCHEDULE
	FROM IMM_ADM_RANKED
WHERE
--deduplicate where codes are non dose specific PCV, 6-in-1, MMR
(dose_number = 1 AND row_num = 1)
OR (dose_number = 2 AND row_num = 2)
OR (dose_number = 3 AND row_num = 3)
-- Include single-entry cases for dose specific MenB and Rotavirus
OR (VACCINE_ID in ('ROTA_2','MENB_2','MENB_3') AND total_events = 1);
