--ICB_CF_AF_62 Over 65 missing pulse check
-- Updated 17-06-2024 by ED to reflect R2 changes from 12m to 36m exclusions on pulse checks

WITH BASE as (

SELECT DISTINCT
b.empi_id
from LTC_LCS_BASE b
left join ICS_LTC_01 l1 on b.empi_id = l1.empi_id
left join HEALTH_CHECK_COMP_IN_24 l2 on b.empi_id = l2.empi_id
left join POPHEALTH_QOF_LTCS_LIST l3 on b.empi_id = l2.empi_id and ltc_name = 'Atrial Fibrillation'
where age >= 65

and l1.empi_id is null
and l2.empi_id is null -- query EMIS vs other?
),

--List of patients with pulse check in last year

PC_date as (

SELECT EMPI_ID, SOURCE_DESCRIPTION
FROM PH_F_CONDITION c
left JOIN JOINED_LTC_LOOKUP t
ON c.CONDITION_CODE = t.SNOMED_CODE
WHERE (t.CLUSTER_ID in ('LCS_PULSE_RHYTHM', 'LCS_PULSE_RATE')
or condition_code in ( ----------------------------------removed once clusters updated
'78564009',
'422119006',
'429525003',
'852341000000107',
'852351000000105',
'429614003',
'843941000000100',
'852331000000103'
)
)
AND c.EFFECTIVE_DT_TM >= ADD_MONTHS(CURRENT_DATE(), -36)

UNION

SELECT EMPI_ID, SOURCE_DESCRIPTION
FROM PH_F_PROCEDURE p
left JOIN JOINED_LTC_LOOKUP t
ON p.procedure_CODE = t.SNOMED_CODE
WHERE (t.CLUSTER_ID in ('LCS_PULSE_RHYTHM', 'LCS_PULSE_RATE')
or p.PROCEDURE_CODE in ( ----------------------------------removed once clusters updated
'78564009',
'422119006',
'429525003',
'852341000000107',
'852351000000105',
'429614003',
'843941000000100',
'852331000000103'
)
)
AND p.SERVICE_START_DT_TM >= ADD_MONTHS(CURRENT_DATE(), -36)

UNION

SELECT EMPI_ID, SOURCE_DESCRIPTION--, RESULT_CODE, SERVICE_DATE
FROM PH_F_RESULT r
left JOIN JOINED_LTC_LOOKUP t
ON r.RESULT_CODE = t.SNOMED_CODE
WHERE (t.CLUSTER_ID in ('LCS_PULSE_RHYTHM', 'LCS_PULSE_RATE')
or r.RESULT_CODE in ( ----------------------------------removed once clusters updated
'78564009',
'422119006',
'429525003',
'852341000000107',
'852351000000105',
'429614003',
'843941000000100',
'852331000000103'
)
)
AND r.SERVICE_DATE >= ADD_MONTHS(CURRENT_DATE(), -36)

),

EMIS as (

select empi_id,
1 as 'EMIS'

FROM (

--Excluding PC_date from base

SELECT
empi_id
from BASE

EXCEPT

SELECT EMPI_ID
FROM PC_date
where SOURCE_DESCRIPTION = 'EMIS GP'

)a
),

OTHER AS (

SELECT empi_id,
1 as 'OTHER'

FROM (

--Excluding PC_date from base

SELECT
empi_id
from BASE

EXCEPT

SELECT EMPI_ID
FROM PC_date

)b
)
-- USING 'EMIS' TABLE TO GET WHOLE COHORT AND LEFT JOINING TO SEE IF OTHER AS WELL
SELECT
COALESCE(e.empi_id, o.empi_id) as empi_id,
case when e.EMIS = 1 then 'EMIS' else 'Other' end as Source
FROM EMIS e
FULL OUTER JOIN OTHER o USING (empi_id)