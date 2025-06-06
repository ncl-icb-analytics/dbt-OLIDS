--20.8.24 new query for HTN66B using external BP results table (all sources) LTC_LATEST_BP_RESULTS - NOTE clusters are different to the other HTN indicators
-- Creating a population from base that excludes the higher priority patients in HTN_61-65 (64 does not exist) and includes AGE
-- This removes all patients with clinic BP >160/100 or home BP >150/95 
WITH pop as (
select A.*, b.age from ICB_CF_HTN_61_BASE A
LEFT JOIN LTC_LCS_BASE b ON A.EMPI_ID = B.EMPI_ID
LEFT JOIN ICB_CF_HTN_61_GP_1 D ON A.EMPI_ID = D.EMPI_ID
LEFT JOIN ICB_CF_HTN_62_GP_2a e ON A.EMPI_ID = e.EMPI_ID
LEFT JOIN ICB_CF_HTN_63_GP_2B f ON A.EMPI_ID = f.EMPI_ID
LEFT JOIN ICB_CF_HTN_65_GP_3a g ON A.EMPI_ID = g.EMPI_ID
WHERE D.EMPI_ID IS NULL
and e.EMPI_ID IS NULL
and f.EMPI_ID IS NULL
and g.EMPI_ID IS NULL
)

 --CREATING CTE FOR EMIS SOURCED 
,EMIS AS (
SELECT DISTINCT EMPI_ID, 1 as 'EMIS'
FROM
(

------- Rule 7 - limiting to people with clinic BP more than 140/90 or home BP more than 135/85

select * from ( --r7

SELECT pop.EMPI_ID from pop
--join LTC_LCS_BASE b using (EMPI_ID)
join LTC_LATEST_BP_RESULTS c using (EMPI_ID)
where C.CLUSTER_ID IN ('SYSBP_COD')
AND C.NORM_NUMERIC_VALUE >= 140 
AND C.SOURCE_DESCRIPTION = 'EMIS GP'
AND pop.EMIS = '1'

UNION

SELECT pop.EMPI_ID from pop
--join LTC_LCS_BASE b using (EMPI_ID)
join LTC_LATEST_BP_RESULTS c using (EMPI_ID)
where C.CLUSTER_ID IN ('DIASBP_COD')
AND C.NORM_NUMERIC_VALUE >= 90
AND C.SOURCE_DESCRIPTION = 'EMIS GP'
AND pop.EMIS = '1'

UNION

SELECT pop.EMPI_ID from pop
--join LTC_LCS_BASE b using (EMPI_ID)
join LTC_LATEST_BP_RESULTS c using (EMPI_ID)
where C.CLUSTER_ID IN ('BP_AMB_HOM_SYS')
AND C.NORM_NUMERIC_VALUE >=135
AND C.SOURCE_DESCRIPTION = 'EMIS GP'
AND pop.EMIS = '1'

UNION

SELECT pop.EMPI_ID from pop
--join LTC_LCS_BASE b using (EMPI_ID)
join LTC_LATEST_BP_RESULTS c using (EMPI_ID)
where C.CLUSTER_ID IN ('BP_AMB_HOM_DIA')
AND C.NORM_NUMERIC_VALUE >= 85
AND C.SOURCE_DESCRIPTION = 'EMIS GP'
AND pop.EMIS = '1'

) r7

------- Rule 5 - Excluding Over 80 with clinic BP less than 150/90
EXCEPT

select * from ( --r5

SELECT pop.EMPI_ID from pop
--join LTC_LCS_BASE b using (EMPI_ID)
join LTC_LATEST_BP_RESULTS c using (EMPI_ID)
where age >= 80
AND C.CLUSTER_ID IN ('SYSBP_COD')
AND C.NORM_NUMERIC_VALUE < 150
AND C.SOURCE_DESCRIPTION = 'EMIS GP'
AND pop.EMIS = '1'

UNION

SELECT pop.EMPI_ID from pop
--join LTC_LCS_BASE b using (EMPI_ID)
join LTC_LATEST_BP_RESULTS c using (EMPI_ID)
where age >= 80
AND C.CLUSTER_ID IN ('DIASBP_COD')
AND C.NORM_NUMERIC_VALUE < 90
AND C.SOURCE_DESCRIPTION = 'EMIS GP'
AND pop.EMIS = '1'

) r5

------- Rule 6 - Excluding Over 80 with home BP less than 145/85
EXCEPT

select * from ( --r6

SELECT pop.EMPI_ID from pop
--join LTC_LCS_BASE b using (EMPI_ID)
join LTC_LATEST_BP_RESULTS c using (EMPI_ID)
where age >= 80
AND C.CLUSTER_ID IN ('BP_AMB_HOM_SYS')
AND C.NORM_NUMERIC_VALUE < 145
AND C.SOURCE_DESCRIPTION = 'EMIS GP'
AND pop.EMIS = '1'

UNION

SELECT pop.EMPI_ID from pop
--join LTC_LCS_BASE b using (EMPI_ID)
join LTC_LATEST_BP_RESULTS c using (EMPI_ID)
where age >= 80
AND C.CLUSTER_ID IN ('BP_AMB_HOM_DIA')
AND C.NORM_NUMERIC_VALUE < 85
AND C.SOURCE_DESCRIPTION = 'EMIS GP'
AND pop.EMIS = '1'
) r6
) sub
) --emis cte

-- CREATING CTE FOR 'OTHER' SOURCED 
,OTHER AS (
SELECT DISTINCT EMPI_ID, 1 as 'OTHER'
FROM
(

------- Rule 7 - limiting to people with clinic BP more than 140/90 or home BP more than 135/85

select * from ( --r7

SELECT pop.EMPI_ID from pop
--join LTC_LCS_BASE b using (EMPI_ID)
join LTC_LATEST_BP_RESULTS c using (EMPI_ID)
where C.CLUSTER_ID IN ('SYSBP_COD')
AND C.NORM_NUMERIC_VALUE >= 140

UNION

SELECT pop.EMPI_ID from pop
--join LTC_LCS_BASE b using (EMPI_ID)
join LTC_LATEST_BP_RESULTS c using (EMPI_ID)
where C.CLUSTER_ID IN ('DIASBP_COD')
AND C.NORM_NUMERIC_VALUE >= 90

UNION

SELECT pop.EMPI_ID from pop
--join LTC_LCS_BASE b using (EMPI_ID)
join LTC_LATEST_BP_RESULTS c using (EMPI_ID)
where C.CLUSTER_ID IN ('BP_AMB_HOM_SYS')
AND C.NORM_NUMERIC_VALUE >=135

UNION

SELECT pop.EMPI_ID from pop
--join LTC_LCS_BASE b using (EMPI_ID)
join LTC_LATEST_BP_RESULTS c using (EMPI_ID)
where C.CLUSTER_ID IN ('BP_AMB_HOM_DIA')
AND C.NORM_NUMERIC_VALUE >= 85
) r7

------- Rule 5 - Excluding Over 80 with clinic BP less than 150/90
EXCEPT

select * from ( --r5

SELECT pop.EMPI_ID from pop
--join LTC_LCS_BASE b using (EMPI_ID)
join LTC_LATEST_BP_RESULTS c using (EMPI_ID)
where age >= 80
AND C.CLUSTER_ID IN ('SYSBP_COD')
AND C.NORM_NUMERIC_VALUE < 150

UNION

SELECT pop.EMPI_ID from pop
--join LTC_LCS_BASE b using (EMPI_ID)
join LTC_LATEST_BP_RESULTS c using (EMPI_ID)
where age >= 80
AND C.CLUSTER_ID IN ('DIASBP_COD')
AND C.NORM_NUMERIC_VALUE < 90
) r5

------- Rule 6 - Excluding Over 80 with home BP less than 145/85
EXCEPT

select * from ( --r6

SELECT pop.EMPI_ID from pop
--join LTC_LCS_BASE b using (EMPI_ID)
join LTC_LATEST_BP_RESULTS c using (EMPI_ID)
where age >= 80
AND C.CLUSTER_ID IN ('BP_AMB_HOM_SYS')
AND C.NORM_NUMERIC_VALUE < 145

UNION

SELECT pop.EMPI_ID from pop
--join LTC_LCS_BASE b using (EMPI_ID)
join LTC_LATEST_BP_RESULTS c using (EMPI_ID)
where age >= 80
AND C.CLUSTER_ID IN ('BP_AMB_HOM_DIA')
AND C.NORM_NUMERIC_VALUE < 85
) r6
) sub
) -- other cte
-- USING 'OTHER' TABLE TO GET WHOLE COHORT AND LEFT JOINING TO SEE WHETHER THEY ARE EMIS SOURCED OR NOT
SELECT 
COALESCE(e.empi_id, o.empi_id) as empi_id,
case when e.EMIS = 1 then 'EMIS' else 'Other' end as SOURCE
FROM EMIS e
FULL OUTER JOIN OTHER o USING (empi_id)