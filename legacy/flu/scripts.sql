Learning DisabilityInsert from Query

    Details
    Query

--5.8.2024 UKHSA RULES Date of latest learning disability code recorded before audit end date

WITH ld_diag AS 
(
select distinct empi_id from 
(
SELECT distinct
c.empi_id,
c.date as diag_date,
row_number() OVER(PARTITION BY c.empi_id ORDER BY c.date DESC) AS Row_no
FROM NCL_CODES c
INNER JOIN UKHSA_FLU cl ON c.CODE = cl.SNOMED_CODE
WHERE
--Learning Disability diagnosis codes
cl.cluster_id = 'LEARNDIS_COD' 
and c.source_description='EMIS GP'
) a 
where row_no = 1 and a.diag_date <= CURRENT_DATE
) 
--Include patients with Learning Disability diagnosis
select 
distinct empi_id,
'Learning disability (LD) codes' as clinical_cluster,
'Learning disability' as risk_group
from ld_diag 
Currently Pregnant PREGCURR_GROUPInsert from Query

    Details
    Query

--UKHSA Patients currently pregnant 
--This identifies patients who are currently pregnant (at RUN_DAT OR current date) defined by presence of a pregnancy code in the defined period without a subsequent delivery code
--IF PREGCURR_DAT = NULL Reject 
--IF PREGCURR_DAT >= PDELCURR_DAT Select
--ADD in an age restriction for currently age 12+ females only (in IV 2024 Flu searches from EMIS)

--Pregancy or delivery codes in the last 9 months
WITH LATEST_CODES AS (
    SELECT DISTINCT
        c.empi_id,
        c.CODE,
        c.date AS diag_date,
        ROW_NUMBER() OVER(PARTITION BY c.empi_id ORDER BY c.date DESC) AS row_no
    FROM NCL_CODES c
    INNER JOIN UKHSA_FLU cl ON c.CODE = cl.SNOMED_CODE
    LEFT JOIN NCL_MASTER_DEMOGRAPHICS ncl on c.empi_id = ncl.EMPI_ID
    WHERE
        cl.cluster_id = 'PREGDEL_COD'
        AND c.source_description = 'EMIS GP'
        AND c.date >= ADD_MONTHS(CURRENT_DATE(), -9)
        AND ncl.age >= 12 and ncl.GENDER = 'Female'
)

-- Identify patients currently pregnant by checking if the latest code is in PREG_COD cluster
SELECT DISTINCT
    lc.empi_id,
    'Codes indicating the patient is pregnant' AS clinical_cluster,
    'Pregnant' as risk_group
FROM LATEST_CODES lc
INNER JOIN UKHSA_FLU cl ON lc.CODE = cl.SNOMED_CODE
WHERE
    cl.cluster_id = 'PREG_COD'
    AND lc.row_no = 1
    AND lc.diag_date IS NOT NULL
Pregnant within certain dates PREG_GROUPInsert from Query

    Details
    Query

--UKHSA PREGNANT GROUP 1 DATE restricted Pregnant on 1st September 24 or becoming pregnant between 01/09/2024 and 28/02/2025 (inclusive)
--UKHSA PREGNANT Group 2 is any patient with a pregnancy, delivered, miscarriage or termination code where the latest code recorded between 01/01/2024 and 31/08/2024 is a pregnancy code
--START-DATE FOR PREGNANCY is 2024-09-01
--ADD in an age restriction for currently age 12+ females only (in IV 2024 Flu searches from EMIS)

--SELECT ALL CODES (Sort by Latest) WHERE Date of the latest pregnancy or delivery code recorded since 1st January 2024 
WITH PREG_ALL as (
SELECT DISTINCT
        c.empi_id,
        c.CODE,
        c.date AS diag_date,
        ROW_NUMBER() OVER(PARTITION BY c.empi_id ORDER BY c.date DESC) AS row_no
    FROM NCL_CODES c
    INNER JOIN UKHSA_FLU cl ON c.CODE = cl.SNOMED_CODE
    LEFT JOIN NCL_MASTER_DEMOGRAPHICS ncl on c.empi_id = ncl.EMPI_ID
    WHERE
        cl.cluster_id = 'PREGDEL_COD'
        AND c.source_description = 'EMIS GP'
and c.source_description ='EMIS GP' AND (c.date >= '2024-01-01' AND c.date <= CURRENT_DATE)
and ncl.age >= 12 and ncl.GENDER = 'Female'
)

--GROUP 1 PREG2_DAT - Pregnancy codes FROM START-DATE is 2024-09-01 (will be zero until September)
,PREG2_DAT AS (
SELECT DISTINCT
    lc.empi_id, lc.diag_date
FROM PREG_ALL lc
INNER JOIN UKHSA_FLU cl ON lc.CODE = cl.SNOMED_CODE
WHERE
    cl.cluster_id = 'PREG_COD'
    AND lc.row_no = 1
    AND lc.diag_date > '2024-08-31'
)
--GROUP 2 PREG_DAT any patient with a pregnancy, delivered, miscarriage or termination code where the latest code recorded between 01/01/2024 and 31/08/2024 is a pregnancy code
,PREG_DAT AS (
SELECT DISTINCT
     lc.empi_id, lc.diag_date
FROM PREG_ALL lc
INNER JOIN UKHSA_FLU cl ON lc.CODE = cl.SNOMED_CODE
WHERE
    cl.cluster_id = 'PREG_COD'
    AND lc.row_no = 1
    AND lc.diag_date <= '2024-08-31' 
    )

-- combine these together 
--PREG_GROUP RULES still pregnant on 31.08.2024 (will become zero on this date) and pregnant since 01.09.2024
,PREG_GROUP AS (
select empi_id 
FROM PREG_DAT WHERE diag_date IS NOT NULL

UNION

select empi_id 
FROM PREG2_DAT 
WHERE diag_date IS NOT NULL
)

select distinct empi_id,
'Codes indicating the patient is pregnant' as clinical_cluster,
 'Pregnant' as risk_group
FROM PREG_GROUP
Morbid obesityInsert from Query

    Details
    Query

--22.8.24 UKHSA RULES Morbid Obesity BMI_GROUP
--1. IF BMI_DAT >= BMI_STAGE_DAT AND IF BMI_VAL >= 40 select
--2. IF BMI_STAGE_DAT = NULL AND BMI_VAL >= 40 select
--3. IF SEV_OBESITY_DAT > BMI_DAT OR IF SEV_OBESITY_DAT <> NULL AND IF BMI_DAT = NULL select

--Using external BMI table FLU_BMI_RESULTS as base for 18 and over

--Date of latest recorded BMI value recorded before audit end date (~1,216,790)
WITH BMI_DAT as (
select empi_id, value, diag_date 
FROM FLU_BMI_RESULTS
where cluster_id = 'BMI_COD' and value is not null
)  
--Date of latest BMI stage code recorded before audit end date (~91,814)
,BMI_STAGE_DAT as (
select empi_id, diag_date 
FROM FLU_BMI_RESULTS
where cluster_id = 'BMI_STAGE_COD'  
) 
--Date of latest severe obesity code (where it matches the date of the latest BMI stage entry ~7,732)
,SEV_OBESITY_DAT as (
select empi_id, diag_date 
FROM FLU_BMI_RESULTS
where cluster_id = 'SEV_OBESITY_COD' 
) 

--BMI_GROUP RULES

, BMI_GROUP as (
--1. BMI_DAT >= BMI_STAGE_DAT AND IF BMI_VAL >= 40 select - only those with a value that's later than a stage code date and value 40+ (~9,320)
select bm.empi_id
from BMI_DAT bm
LEFT JOIN BMI_STAGE_DAT bms on bms.empi_id = bm.empi_id
WHERE bm.diag_date >= bms.diag_date and bm.value >= 40

UNION
--2. IF BMI_STAGE_DAT = NULL AND BMI_VAL >=40 select - only those without stage code and value 40+ (~10,000)
select bm.empi_id
from BMI_DAT bm
LEFT JOIN BMI_STAGE_DAT bms on bms.empi_id = bm.empi_id
WHERE bms.empi_id is null and bm.value >= 40

UNION
--3. IF SEV_OBESITY_DAT > BMI_DAT OR IF SEV_OBESITY_DAT <> NULL AND IF BMI_DAT = NULL select (~468)
select sev.empi_id
from SEV_OBESITY_DAT sev
LEFT JOIN BMI_DAT bm on bm.empi_id = sev.empi_id
WHERE (sev.diag_date > bm.diag_date OR bm.diag_date is NULL) 
)

select distinct empi_id,
'Morbid Obesity' as clinical_cluster,
'BMI 40+' as risk_group
from BMI_GROUP
Household contact of immunocompromised individualsInsert from Query

    Details
    Query

--UKHSA 9.9.24 Date of LATEST household contact of immunocompromised code recorded

WITH hhcimms_diag AS
(
select distinct empi_id from 
(
SELECT distinct
c.empi_id,
c.date as diag_date,
row_number() OVER(PARTITION BY c.empi_id ORDER BY c.date DESC) AS Row_no
FROM NCL_CODES c
INNER JOIN UKHSA_FLU cl ON c.CODE = cl.SNOMED_CODE
WHERE
-- Household contacts of immunocompromised individuals
cl.cluster_id = 'HHLD_IMDEF_COD' 
and c.source_description='EMIS GP'
) a 
where row_no = 1 and a.diag_date <= CURRENT_DATE
)

select 
distinct empi_id,
'Household contacts of immunocompromised' as clinical_cluster,
'Household contacts of immunocompromised' as risk_group
from hhcimms_diag
HomelessInsert from Query

    Details
    Query

/* 22.8.24 UKHSA RULES for people currently in precarious accomodation HOMELESS_GROUP: IF HOMELESS_DAT <> NULL and HOMELESS_DAT >= RESIDE_DAT*/
--All people residential status
WITH RESIDE_CODES AS (
    SELECT DISTINCT
        c.empi_id,
        c.CODE,
        c.date AS diag_date,
        ROW_NUMBER() OVER(PARTITION BY c.empi_id ORDER BY c.date DESC) AS row_no
    FROM NCL_CODES c
    INNER JOIN UKHSA_FLU cl ON c.CODE = cl.SNOMED_CODE
    WHERE
        cl.cluster_id = 'RESIDE_COD' AND c.source_description = 'EMIS GP' and c.date <= CURRENT_DATE
) 
-- Identify people still homeless by checking if the latest code is in HOMELESS_COD cluster
SELECT DISTINCT
    lc.empi_id,
   'Codes indicating the patient is homeless' AS clinical_cluster,
   'Homeless' AS risk_group
FROM RESIDE_CODES lc
INNER JOIN UKHSA_FLU cl ON lc.CODE = cl.SNOMED_CODE
WHERE
    cl.cluster_id = 'HOMELESS_COD'
    AND lc.row_no = 1
    AND lc.diag_date IS NOT NULL
CarerInsert from Query

    Details
    Query

/* 22.8.24 UKHSA RULES CARER_GROUP: Patients meeting Carer Criteria
IF CARER_DAT = NULL Reject
IF NOTCARER_DAT = NULL Select 
IF CARER_DAT > NOTCARER_DAT Select */
--All people currently with latest CARER OR NOT CARER CODES 
WITH CARER_CODES AS (
    SELECT DISTINCT
        c.empi_id,
        cl.CLUSTER_ID,
        c.CODE,
        c.date AS diag_date,
        ROW_NUMBER() OVER(PARTITION BY c.empi_id, cl.CLUSTER_ID ORDER BY c.date DESC) AS row_no
    FROM NCL_CODES c
    INNER JOIN UKHSA_FLU cl ON c.CODE = cl.SNOMED_CODE
    WHERE
        cl.cluster_id in ('CARER_COD','NOTCARER_COD') AND c.source_description = 'EMIS GP' and c.date <= CURRENT_DATE
        
) 
--with a current carer code (~43632)
,CARER_DAT AS (
select empi_id, diag_date 
from CARER_CODES where row_no = 1 and cluster_id = 'CARER_COD' and diag_date is not null
)
--with a not carer code (~74217)
,NOTCARER_DAT AS (
select empi_id, diag_date 
from CARER_CODES where row_no = 1 and cluster_id = 'NOTCARER_COD' and diag_date is not null
)
---- Identify carers by checking if the latest code is in CARER_COD cluster ~ 38,690
, CARER_GROUP AS (
SELECT c.empi_id
from CARER_DAT c
LEFT JOIN NOTCARER_DAT n using (empi_id)
WHERE (c.diag_date > n.diag_date OR n.empi_id is NULL)
)

select distinct empi_id,
'Current Carer' as clinical_cluster,
'Carer' as risk_group
from CARER_GROUP
Long term residential careInsert from Query

    Details
    Query

/* 22.8.24 UKHSA RULES Patients in long term residential care

LONGRES_GROUP: IF LONGRES_DAT <> NULL and LONGRES_DAT >= RESIDE_DAT*/

--All people residential status

WITH RESIDE_CODES AS (

    SELECT DISTINCT

        c.empi_id,

        c.CODE,

        c.date AS diag_date,

        ROW_NUMBER() OVER(PARTITION BY c.empi_id ORDER BY c.date DESC) AS row_no

    FROM NCL_CODES c

    INNER JOIN UKHSA_FLU cl ON c.CODE = cl.SNOMED_CODE

    WHERE

        cl.cluster_id = 'RESIDE_COD' AND c.source_description = 'EMIS GP' and c.date <= CURRENT_DATE

) 

-- Identify people in Patients in long term residential careby checking if the latest code is in HOMELESS_COD cluster

SELECT DISTINCT

    lc.empi_id,

    'Codes indicating the patient is in long term care' AS clinical_cluster,

    'Long Term Residential Care' AS risk_group

FROM RESIDE_CODES lc

INNER JOIN UKHSA_FLU cl ON lc.CODE = cl.SNOMED_CODE

WHERE

    cl.cluster_id = 'LONGRES_COD'

    AND lc.row_no = 1

    AND lc.diag_date IS NOT NULL
Health and social care workersInsert from Query

    Details
    Query

/* 22.8.24 UKHSA RULES HEALTH AND SOCIAL CARE WORKERS: Patients working in care home settings 

not sure about exclusion of some other groups

*/



--All people currently working in care home settings

WITH HCWORKER_CODES AS (

    SELECT DISTINCT

        c.empi_id,

        cl.CLUSTER_ID,

        c.CODE,

        c.date AS diag_date,

        ROW_NUMBER() OVER(PARTITION BY c.empi_id, cl.CLUSTER_ID ORDER BY c.date DESC) AS row_no

    FROM NCL_CODES c

    INNER JOIN UKHSA_FLU cl ON c.CODE = cl.SNOMED_CODE

    WHERE

        cl.cluster_id in ('CAREHOME_COD','DOMCARE_COD','NURSEHOME_COD') AND c.source_description = 'EMIS GP' and c.date <= CURRENT_DATE

  ) 

select distinct empi_id,

'Health and Social Care Workers' as clinical_cluster,

'Health and Social Care Workers' as risk_group

from HCWORKER_CODES

WHERE row_no = 1

--5.8.2024 new clinical risk froup for Asthma based on UKHSA business rules
--UKHSA RULES: Date of earliest recorded asthma diagnosis before audit end date
with asthma_diag as (
select distinct empi_id from 
(
SELECT distinct
c.empi_id,
c.date as diag_date,
row_number() OVER(PARTITION BY c.empi_id ORDER BY c.date ASC) AS Row_no
FROM NCL_CODES c
INNER JOIN UKHSA_FLU cl ON c.CODE = cl.SNOMED_CODE
WHERE
--asthma diagnosis codes
cl.cluster_id = 'AST_COD' 
and c.source_description='EMIS GP'
) a 
where row_no = 1 and a.diag_date <= CURRENT_DATE
)
--UKHSA RULES: People with inhaler admin code Or inhaler medication on or after 1st Sept 23

,asthma_inhalers as (
select distinct empi_id 
FROM
(
--UKHSA RULES: Date of latest oral or inhaled steroid ADMIN code on or after 1st Sept 23 (~7,500)
SELECT empi_id FROM
(
SELECT 
c.empi_id,
c.date as inhaler_date,
row_number() OVER(PARTITION BY c.empi_id ORDER BY c.date DESC) AS Row_no
FROM NCL_CODES c
INNER JOIN UKHSA_FLU cl ON c.CODE = cl.SNOMED_CODE
WHERE 
--inhaler admin codes
cl.cluster_id = 'ASTMED_COD' 
and c.source_description='EMIS GP'
) a
where a.row_no = 1 and (inhaler_date >= '2023-09-01' AND inhaler_date <= CURRENT_DATE)

UNION

--UKHSA RULES: Date of latest asthma inhaled steroid medication on or after 1st Sept 23 (~75,000)
SELECT empi_id FROM
(
SELECT 
c.empi_id,
DATE(c.start_dt_tm) AS inhaler_date,
row_number() OVER(PARTITION BY c.empi_id ORDER BY c.start_dt_tm DESC) AS Row_no
FROM CERNER_MEDICATION_JOINED_TABLE c
INNER JOIN UKHSA_FLU cl ON c.DRUG_CODE = cl.SNOMED_CODE
WHERE 
--inhaler drug codes
cl.cluster_id = 'ASTRX_COD' 
and c.source_description='EMIS GP' 
) a
where a.row_no = 1 and (a.inhaler_date >= '2023-09-01' AND a.inhaler_date <= CURRENT_DATE)
) b
)
--UKHSA rule: Date of latest asthma related admission before the audit end date
,asthma_adms as ( 
select distinct empi_id FROM 
(
SELECT 
c.empi_id,
c.date as adms_date,
row_number() OVER(PARTITION BY c.empi_id ORDER BY c.date DESC) AS Row_no
FROM NCL_CODES c
INNER JOIN UKHSA_FLU cl ON c.CODE = cl.SNOMED_CODE
WHERE
--asthma admission codes
cl.cluster_id = 'ASTADM_COD' 
and c.source_description='EMIS GP' 
) a 
where row_no = 1 and a.adms_date <= CURRENT_DATE
)

----UKHSA Rule include:
-- 1. Have had asthma related admission (~10,000)
-- 2. Have asthma diagnosis and an inhaler admin code or an inhaler drug code on or after 1st Sept 23 (~60,000)

,asthma_group as 
(
--1. all from admissions
select 
empi_id
FROM asthma_adms 

UNION
--2. All from diagnosis with an inhaler
select ad.empi_id
FROM asthma_diag ad
INNER JOIN asthma_inhalers at ON ad.empi_id = at.empi_id
)

select 
distinct empi_id,
'Asthma diagnosis codes' as clinical_cluster, --> creating a derived field and grouping into 1 cluster to avoid duplicates
'Asthma' as LTC
FROM asthma_group
Chronic Heart DiseaseInsert from Query

    Details
    Query

--5.8.2024 UKHSA RULES Date of earliest recorded CHD diagnosis before audit end date/current date
WITH Chronic_Heart_Disease AS 
(
select distinct empi_id from 
(
SELECT distinct
c.empi_id,
c.date as diag_date,
row_number() OVER(PARTITION BY c.empi_id ORDER BY c.date ASC) AS Row_no
FROM NCL_CODES c
INNER JOIN UKHSA_FLU cl ON c.CODE = cl.SNOMED_CODE
WHERE
--CHD diagnosis codes
cl.cluster_id = 'CHD_COD' 
and c.source_description='EMIS GP'
) a 
where row_no = 1 and a.diag_date <= CURRENT_DATE
	) 
--Include patients with Chronic Heart Disease diagnosis
SELECT empi_id
	,'Chronic heart disease codes' as clinical_cluster
	,'Chronic_heart_disease' as LTC
FROM Chronic_Heart_Disease 
Chronic Liver DiseaseInsert from Query

    Details
    Query

--5.8.2024 UKHSA RULES Date of earliest recorded CLD diagnosis before audit end date/current date

with cld_diag as (
select distinct empi_id from 
(
SELECT distinct
c.empi_id,
c.date as diag_date,
row_number() OVER(PARTITION BY c.empi_id ORDER BY c.date ASC) AS Row_no
FROM NCL_CODES c
INNER JOIN UKHSA_FLU cl ON c.CODE = cl.SNOMED_CODE
WHERE
--CLD diagnosis codes
cl.cluster_id = 'CLD_COD' 
and c.source_description ='EMIS GP'
) a 
where row_no = 1 and diag_date <= CURRENT_DATE
)



select 
distinct empi_id,
'Chronic liver disease (CLD) codes' as clinical_cluster,
'Chronic_liver_disease' as LTC
from cld_diag 
Chronic Neurological DiseaseInsert from Query

    Details
    Query

--5.8.2024 UKHSA RULES Date of earliest recorded CNS diagnosis before audit end date/current date

with cnd_diag as (

select distinct empi_id from 
(
SELECT distinct
c.empi_id,
c.date as diag_date,
row_number() OVER(PARTITION BY c.empi_id ORDER BY c.date ASC) AS Row_no
FROM NCL_CODES c
INNER JOIN UKHSA_FLU cl ON c.CODE = cl.SNOMED_CODE
WHERE
--CNS GROUP diagnosis codes
cl.cluster_id = 'CNSGROUP_COD' 
and c.source_description='EMIS GP'
) a 
where row_no = 1 and a.diag_date <= CURRENT_DATE

)



select 
distinct empi_id,
'Chronic neurological disease (CND) codes' as clinical_cluster,
'Chronic_neurological_disease' as 'LTC'
from cnd_diag 
CKD 3-5Insert from Query

    Details
    Query

--6.8.2024 UKHSA RULES 

--1. IF CKD_DAT <> NULL (diagnoses) select

--2. IF CKD15_DAT = NULL (all stages) reject

--3. IF CKD35_DAT = NULL (stages 3-5) reject

--4. IF CKD35_DAT >= CKD15_DAT select



--1. Date of earliest recorded CKD diagnosis before audit end date

--If a patient has any Chronic Kidney disease code, they are included in the CKD Risk Group.



WITH CKD_Any AS (

select distinct empi_id from 

(

SELECT distinct

c.empi_id,

c.date as diag_date,

row_number() OVER(PARTITION BY c.empi_id ORDER BY c.date ASC) AS Row_no

FROM NCL_CODES c

INNER JOIN UKHSA_FLU cl ON c.CODE = cl.SNOMED_CODE

WHERE

--CKD diagnosis codes

cl.cluster_id = 'CKD_COD' 

and c.source_description='EMIS GP'

) a 

where row_no = 1 and a.diag_date <= CURRENT_DATE

)



--2. Date of LATEST CKD stage (any) code recorded before audit end date 



,CKD15 as (

select distinct empi_id, diag_date from 

(

SELECT distinct

c.empi_id,

c.date as diag_date,

cl.cluster_id,

row_number() OVER(PARTITION BY c.empi_id ORDER BY c.date DESC) AS Row_no

FROM NCL_CODES c

INNER JOIN UKHSA_FLU cl ON c.CODE = cl.SNOMED_CODE

WHERE

--CKD15 diagnosis codes

cl.cluster_id = 'CKD15_COD' 

and c.source_description='EMIS GP'

) a 

where row_no = 1 and a.diag_date <= CURRENT_DATE

)



--3 Date of LATEST CKD stage 3, 4 or 5 code recorded before audit end date



,CKD35 AS (

select distinct empi_id, diag_date from 

(

SELECT distinct

c.empi_id,

c.date as diag_date,

row_number() OVER(PARTITION BY c.empi_id ORDER BY c.date DESC) AS Row_no

FROM NCL_CODES c

INNER JOIN UKHSA_FLU cl ON c.CODE = cl.SNOMED_CODE

WHERE

--CKD35 diagnosis codes

cl.cluster_id = 'CKD35_COD' 

and c.source_description='EMIS GP'

) a 

where row_no = 1 and a.diag_date <= CURRENT_DATE

)



--4. The patient record is then checked to see if the most recent Read code is a Stage 3 to 5 and combined with CKD any

,CKD_group as (

SELECT distinct c15.empi_id

FROM CKD15 c15

INNER JOIN CKD35 c35 ON c15.empi_id = c35.empi_id

WHERE c35.diag_date >= c15.diag_date



UNION ALL



SELECT empi_id 

FROM

CKD_Any

)

SELECT 

distinct empi_id,

'Chronic kidney disease (CKD) stage 3, 4 and 5 codes' as clinical_cluster,

'Chronic_kidney_disease' as LTC

FROM CKD_group
DiabetesInsert from Query

    Details
    Query

--6.8.2024 UKHSA RULES DIABETES

--DIABETES_GROUP Patients with Diabetes and other relevant endocrine disorders

--1. IF ADDIS_DAT <> NULL Select must have a addison disease code

--2. IF DIAB_DAT = NULL Reject - must have a diabetes code

--3. IF DMRES_DAT = NULL Select  - choose those without a diabetes resolved code

--4. IF DIAB_DAT > DMRES_DAT Select - latest code must be diabetes, not diabetes resolved



--1.Date of EARLIEST recorded Addison’s disease/pan-hypopituitary diagnosis code recorded before audit end date



WITH ADDISON AS (

select distinct empi_id from 

(

SELECT distinct

c.empi_id,

c.date as diag_date,

row_number() OVER(PARTITION BY c.empi_id ORDER BY c.date ASC) AS Row_no

FROM NCL_CODES c

INNER JOIN UKHSA_FLU cl ON c.CODE = cl.SNOMED_CODE

WHERE

--Addison's Disease diagnosis codes

cl.cluster_id = 'ADDIS_COD' 

and c.source_description='EMIS GP'

) a 

where row_no = 1 and a.diag_date <= CURRENT_DATE

)



--2. Date of LATEST recorded diabetes diagnosis before audit end date



,DIAB AS (

select distinct empi_id, diag_date from 

(

SELECT distinct

c.empi_id,

c.date as diag_date,

row_number() OVER(PARTITION BY c.empi_id ORDER BY c.date DESC) AS Row_no

FROM NCL_CODES c

INNER JOIN UKHSA_FLU cl ON c.CODE = cl.SNOMED_CODE

WHERE

--Diabetes diagnosis codes

cl.cluster_id = 'DIAB_COD' 

and c.source_description='EMIS GP'

) a 

where row_no = 1 and a.diag_date <= CURRENT_DATE

)



--3. Date of LATEST diabetes resolved code recorded before audit end date



,DMRES as (

select distinct empi_id, diag_date from 

(

SELECT distinct

c.empi_id,

c.date as diag_date,

cl.cluster_id,

row_number() OVER(PARTITION BY c.empi_id ORDER BY c.date DESC) AS Row_no

FROM NCL_CODES c

INNER JOIN UKHSA_FLU cl ON c.CODE = cl.SNOMED_CODE

WHERE

--Diabetes Resolved diagnosis codes

cl.cluster_id = 'DMRES_COD' 

and c.source_description='EMIS GP'

) a 

where row_no = 1 and a.diag_date <= CURRENT_DATE

)



--4. Note: If patients have a more recent “diabetes resolved” code than a diabetes diagnosis code they should not be selected in DIAB_GROUP unless they have a code representing Addison’s disease 

--Select Diabetes Codes where there isn't a REsolved code or the resolved code is older than the latest diagnosis code

,DIAB_GROUP as (

SELECT distinct d.empi_id

FROM DIAB d

LEFT JOIN DMRES dr ON d.empi_id = dr.empi_id

WHERE (dr.empi_id is NULL OR dr.diag_date < d.diag_date )



UNION ALL



SELECT empi_id 

FROM

ADDISON

)

SELECT 

distinct empi_id,

'Diabetes' as clinical_cluster,

'Diabetes' as LTC

FROM DIAB_GROUP
Chronic Respiratory DiseaseInsert from Query

    Details
    Query

--5.8.2024 UKHSA RULES Date of earliest recorded respiratory disease diagnosis before audit end date



WITH Chronic_Respiratory_Disease AS 

(

select distinct empi_id from 

(

SELECT distinct

c.empi_id,

c.date as diag_date,

row_number() OVER(PARTITION BY c.empi_id ORDER BY c.date ASC) AS Row_no

FROM NCL_CODES c

INNER JOIN UKHSA_FLU cl ON c.CODE = cl.SNOMED_CODE

WHERE

--Chronic Respiratory Disease Codes

cl.cluster_id = 'RESP_COD' 

and c.source_description='EMIS GP'

) a 

where row_no = 1 and a.diag_date <= CURRENT_DATE

) 

--Include patients with Chronic respiratory disease

SELECT 

	 DISTINCT

   ad.empi_id

	,'Chronic respiratory disease (CRD) codes' as clinical_cluster

	,'Chronic_Respiratory_Disease' as LTC

FROM Chronic_Respiratory_Disease ad
AspleniaInsert from Query

    Details
    Query

--5.8.2024 UKHSA RULES Date of earliest recorded Asplenia or dysfunction of the spleen code recorded before audit end date



WITH Asplenia AS 

(

select distinct empi_id from 

(

SELECT distinct

c.empi_id,

c.date as diag_date,

row_number() OVER(PARTITION BY c.empi_id ORDER BY c.date ASC) AS Row_no

FROM NCL_CODES c

INNER JOIN UKHSA_FLU cl ON c.CODE = cl.SNOMED_CODE

WHERE

--Asplenia

cl.cluster_id = 'PNSPLEEN_COD' 

and c.source_description='EMIS GP'

) a 

where row_no = 1 and a.diag_date <= CURRENT_DATE

) 

--Include patients with asplenia

SELECT DISTINCT

        ad.empi_id

	,'Asplenia codes' as clinical_cluster

	,'Asplenia' as LTC

FROM Asplenia ad
ImmunosuppressionInsert from Query

    Details
    Query

--22.08.2024 UKHSA Patients with Immunosuppression - combine into one query

/*IF IMMDX_DAT <> NULL 

IF IMMRX_DAT <> NULL

IF IMMADM_DAT <> NULL

IF DXT_CHEMO_DAT <> NULL*/



WITH IMMUNO_ALL AS (

SELECT distinct

c.empi_id,

c.date as diag_date,

cl.cluster_id,

row_number() OVER(PARTITION BY c.empi_id, cl.cluster_id ORDER BY c.date DESC) AS Row_no

FROM NCL_CODES c

INNER JOIN UKHSA_FLU cl ON c.CODE = cl.SNOMED_CODE

WHERE

--Immunosuppression diagnosis, chemo and admin

cl.cluster_id in ('IMMDX_COD', 'DXT_CHEMO_COD','IMM_ADM_COD') 

and c.source_description='EMIS GP' and c.date <= CURRENT_DATE

order by 1

) 

--Date of latest immunosuppression medication code issued on or after 1st March 24

,IMMUNO_TREAT as ( 

SELECT empi_id FROM

(

SELECT distinct

c.empi_id,

DATE(c.start_dt_tm) AS med_date,

row_number() OVER(PARTITION BY c.empi_id ORDER BY c.start_dt_tm DESC) AS Row_no

FROM CERNER_MEDICATION_JOINED_TABLE c

INNER JOIN UKHSA_FLU cl ON c.DRUG_CODE = cl.SNOMED_CODE

WHERE 

--immuno suppression medication codes

cl.cluster_id = 'IMMRX_COD' and c.source_description='EMIS GP' and (DATE(c.start_dt_tm) >= '2024-03-01' AND DATE(c.start_dt_tm) <= CURRENT_DATE)

) a

where a.row_no = 1 

)

--UKHSA Date of latest recorded Immunosuppression diagnosis before audit end date

,IMMDX_DAT AS (

select empi_id 

FROM IMMUNO_ALL im

--immuno diagnosis codes

WHERE cluster_id = 'IMMDX_COD' AND row_no = 1

) 

--UKHSA Date of latest “patient immunosuppressed” admin code since 1st March 2024 

,IMMADM_DAT AS (

select empi_id 

FROM IMMUNO_ALL im

--immuno admin codes since 1st March 24 

WHERE cluster_id = 'IMMADM_COD' AND row_no = 1 and diag_date >= '2024-03-01'

) 

--UKHSA Date of latest chemotherapy or radiotherapy code issued on or after 1st March 24 

,DXT_CHEMO_DAT AS (

select empi_id 

FROM IMMUNO_ALL im

--immuno chemo codes since 1st March 24 

WHERE cluster_id = 'DXT_CHEMO_COD' AND row_no = 1 and diag_date >= '2024-03-01'

)



--JOIN ALL 

--diagnosis

SELECT empi_id

	,'Immunosuppression diagnosis' as clinical_cluster

	,'Weakened_Immune_System' as LTC

FROM IMMDX_DAT 



UNION

--admin

SELECT empi_id

	,'Immunosuppression admin' as clinical_cluster

	,'Weakened_Immune_System' as LTC

FROM IMMADM_DAT 



UNION

--chemo

SELECT empi_id

	,'Immunosuppression chemotherapy' as clinical_cluster

	,'Weakened_Immune_System' as LTC

FROM DXT_CHEMO_DAT 



UNION

--treatment

SELECT empi_id

	,'Immunosuppression treatment' as clinical_cluster

	,'Weakened_Immune_System' as LTC

FROM IMMUNO_TREAT


Over 65Insert from Query

    Details
    Query

--All patients aged >= 65 years at REF_DAT which is the end of the booster campaig 2025-03-25
Select empi_id,
'Autumn' as campaign,
'Age 65 or older' as eligible_cohort
from 
IC_WHOLE_POP_NCL_2425
WHERE  AGE_IN_YEARS('2025-03-31'::TIMESTAMP, BIRTH_DATE::TIMESTAMP) >=65  
Children aged 2-3Insert from Query

    Details
    Query

--All Children aged 2-3 on the 31st August 2024
--Pre School Years 2-3 
--born 1st September 2020 to 31st August 2022 age 2-3 on 1st September 2024
Select empi_id,
'Autumn' as campaign,
'Age 2 to 3' as eligible_cohort
from 
IC_WHOLE_POP_NCL_2425
WHERE BIRTH_DATE >= '2020-09-01' AND BIRTH_DATE <= '2022-08-31'
 
Children aged 4-16Insert from Query

    Details
    Query

--School Years Reception to Y11 
--Reception born 1st September 2019 to 31st August 2020	age	4-5 on 1st September 2024
--Y11 born 1st September 2008 to 31st August 2009 age 15-16 on 1st September 2024

Select empi_id,
'Autumn' as campaign,
'Age 4 to 16' as eligible_cohort
from 
IC_WHOLE_POP_NCL_2425
WHERE BIRTH_DATE >= '2008-09-01' AND BIRTH_DATE <= '2020-08-31'
6 months to 64 years at clinical risk groupInsert from Query

    Details
    Query

--Patients aged >= 6 months NOW AND <65 years up to end of the booster campaign AND in at least one core clinical At Risk disease category 
Select iv.empi_id,
'Autumn' as campaign,
'Age under 65 at risk' as eligible_cohort
from 
IC_WHOLE_POP_NCL_2425 iv
WHERE
--Including at risk clinical group
iv.empi_id in (select distinct empi_id from IV_CLINICAL_RISK_FACTORS_DS_2425)
AND
--Include those aged from six months to less than 65 years of age
datediff('month',BIRTH_DATE::DATE,CURRENT_DATE::DATE) >= 6
AND AGE_IN_YEARS('2025-03-31'::TIMESTAMP, BIRTH_DATE::TIMESTAMP) <65
Long Term Residential CareInsert from Query

    Details
    Query

--Long stay residential care patient currently aged >= 6 months + LT residents based on address
Select distinct empi_id,
'Autumn' as campaign,
'Long Term Residential Care' as eligible_cohort
from 
(
Select iv.empi_id
from 
IC_WHOLE_POP_NCL_2425 iv
left join IV_OTHER_RISK_FACTORS_DS_2425 orf using (empi_id)
WHERE risk_group = 'Long Term Residential Care'
AND datediff('month',BIRTH_DATE::DATE,CURRENT_DATE::DATE) >= 6
UNION
select iv.EMPI_ID FROM
IC_WHOLE_POP_NCL_2425 iv
where carehome_flag= 'Y'
AND datediff('month',BIRTH_DATE::DATE,CURRENT_DATE::DATE) >= 6
)a
Learning DisabilityInsert from Query

    Details
    Query

--Patients with a Learning Disability aged >= 6 months
select iv.empi_id,
'Autumn' as campaign,
'Learning Disability' as eligible_cohort
FROM IC_WHOLE_POP_NCL_2425 iv
left join IV_OTHER_RISK_FACTORS_DS_2425 orf using (empi_id)
WHERE risk_group = 'Learning disability'
AND datediff('month',BIRTH_DATE::DATE,CURRENT_DATE::DATE) >= 6
AND AGE_IN_YEARS('2025-03-31'::TIMESTAMP, BIRTH_DATE::TIMESTAMP) <65
Pregnant WomenInsert from Query

    Details
    Query

--Pregnant Females added age limit for >= 12 from EMIS Flu recall searches
Select iv.empi_id,
'Autumn' as campaign,
'Pregnant women' as eligible_cohort
FROM IC_WHOLE_POP_NCL_2425 iv
left join IV_OTHER_RISK_FACTORS_DS_2425 orf using (empi_id)
WHERE risk_group = 'Pregnant' and gender = 'Female'
AND AGE_IN_YEARS(CURRENT_DATE::TIMESTAMP, BIRTH_DATE::TIMESTAMP) >=12 
AND AGE_IN_YEARS('2025-03-31'::TIMESTAMP, BIRTH_DATE::TIMESTAMP) <65
Homeless aged 16 +Insert from Query

    Details
    Query

--Homeless Patients aged >= 16 years 
--combineas both homeless status using postcode and CHIP practice registration with clinical codes
select distinct empi_id, 
'Autumn' as campaign,
'Homeless' as eligible_cohort 
FROM
(
select empi_id
from NCL_MASTER_DEMOGRAPHICS  
where HOMELESS_STATUS = 'Yes'
and AGE_IN_YEARS(CURRENT_DATE::TIMESTAMP, BIRTH_DATE::TIMESTAMP) >=16 
AND AGE_IN_YEARS('2025-03-31'::TIMESTAMP, BIRTH_DATE::TIMESTAMP) <65
UNION
select iv.empi_id
FROM IC_WHOLE_POP_NCL_2425 iv
left join IV_OTHER_RISK_FACTORS_DS_2425 orf using (empi_id)
WHERE risk_group = 'Homeless'
AND AGE_IN_YEARS(CURRENT_DATE::TIMESTAMP, BIRTH_DATE::TIMESTAMP) >=16 
AND AGE_IN_YEARS('2025-03-31'::TIMESTAMP, BIRTH_DATE::TIMESTAMP) <65
) a
Morbid Obesity Age 18+Insert from Query

    Details
    Query

select iv.empi_id,
'Autumn' as campaign,
'Morbid Obesity' as eligible_cohort
FROM IC_WHOLE_POP_NCL_2425 iv
left join IV_OTHER_RISK_FACTORS_DS_2425 orf using (empi_id)
WHERE risk_group = 'BMI 40+'
AND AGE_IN_YEARS(CURRENT_DATE::TIMESTAMP, BIRTH_DATE::TIMESTAMP)  >=18
AND AGE_IN_YEARS(CURRENT_DATE::TIMESTAMP, BIRTH_DATE::TIMESTAMP)  <65
Carers (not in any other group)Insert from Query

    Details
    Query

/*Find carers that are not already in Clinical risk group, BMI GROUP, PREGNANT or are not a carer
IF ATRISK_GROUP <> NULL Reject
IF BMI_GROUP <> NULL Reject
IF PREG_GROUP <> NULL Reject
IF CARER_DAT = NULL Reject
Next IF NOTCARER_DAT = NULL Select Next
IF CARER_DAT > NOTCARER_DAT*/

--other clinical risk factors excluded from carer group
with orf as (
select empi_id from IV_OTHER_RISK_FACTORS_DS_2425 
where risk_group in ('BMI 40+', 'Pregnant')
)

--Carers not in any other group currently aged >=5 and <65 years
select pop.empi_id,
'Autumn' as campaign,
'Carers' as eligible_cohort
from IC_WHOLE_POP_NCL_2425 pop
left join IV_CLINICAL_RISK_FACTORS_DS_2425 crf using (empi_id)
left JOIN orf using (empi_id)
where Carer_Flag ='Yes'
AND orf.empi_id IS NULL
AND crf.empi_id IS NULL
AND AGE_IN_YEARS(CURRENT_DATE::TIMESTAMP, BIRTH_DATE::TIMESTAMP) >=5
AND AGE_IN_YEARS(CURRENT_DATE::TIMESTAMP, BIRTH_DATE::TIMESTAMP) <65
Household contact of ImmunosuppressedInsert from Query

    Details
    Query

--Household contact of immunosuppressed >= 6 months NOW
Select iv.empi_id,
'Autumn' as campaign,
'Household Immunosupressed' as eligible_cohort
from 
IC_WHOLE_POP_NCL_2425 iv
left join IV_OTHER_RISK_FACTORS_DS_2425 orf using (empi_id)
WHERE risk_group = 'Household contacts of immunocompromised'
AND datediff('month',BIRTH_DATE::DATE,CURRENT_DATE::DATE) >= 6
AND AGE_IN_YEARS('2025-03-31'::TIMESTAMP, BIRTH_DATE::TIMESTAMP) <65
Health and Social Care WorkersInsert from Query

    Details
    Query

--not accurate representation of the figures. 
Select iv.empi_id,
'Autumn' as campaign,
'Health and Social Care Workers' as eligible_cohort
from 
IC_WHOLE_POP_NCL_2425 iv
left join IV_OTHER_RISK_FACTORS_DS_2425 orf using (empi_id)
WHERE risk_group = 'Health and Social Care Workers' 
AND AGE_IN_YEARS(CURRENT_DATE::TIMESTAMP, BIRTH_DATE::TIMESTAMP) >=16
AND AGE_IN_YEARS(CURRENT_DATE::TIMESTAMP, BIRTH_DATE::TIMESTAMP) <65


