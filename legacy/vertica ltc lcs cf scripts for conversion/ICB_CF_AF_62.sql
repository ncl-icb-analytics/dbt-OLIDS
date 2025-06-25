--ICB_CF_AF_62 Over 65 missing pulse check
-- Updated 17-06-2024 by ED to reflect R2 changes from 12m to 36m exclusions on pulse checks

WITH BASE AS (

    SELECT DISTINCT B.EMPI_ID
    FROM LTC_LCS_BASE AS B
    LEFT JOIN ICS_LTC_01 AS L1 ON B.EMPI_ID = L1.EMPI_ID
    LEFT JOIN HEALTH_CHECK_COMP_IN_24 AS L2 ON B.EMPI_ID = L2.EMPI_ID
    LEFT JOIN
        POPHEALTH_QOF_LTCS_LIST
        ON B.EMPI_ID = L2.EMPI_ID AND LTC_NAME = 'Atrial Fibrillation'
    WHERE
        AGE >= 65

        AND L1.EMPI_ID IS NULL
        AND L2.EMPI_ID IS NULL -- query EMIS vs other?
),

--List of patients with pulse check in last year

PC_DATE AS (

    SELECT
        EMPI_ID,
        SOURCE_DESCRIPTION
    FROM PH_F_CONDITION AS C
    LEFT JOIN JOINED_LTC_LOOKUP AS T
        ON C.CONDITION_CODE = T.SNOMED_CODE
    WHERE (
        T.CLUSTER_ID IN ('LCS_PULSE_RHYTHM', 'LCS_PULSE_RATE')
        OR CONDITION_CODE IN ( ----------------------------------removed once clusters updated
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
    AND C.EFFECTIVE_DT_TM >= ADD_MONTHS(CURRENT_DATE(), -36)

    UNION

    SELECT
        EMPI_ID,
        SOURCE_DESCRIPTION
    FROM PH_F_PROCEDURE AS P
    LEFT JOIN JOINED_LTC_LOOKUP AS T
        ON P.PROCEDURE_CODE = T.SNOMED_CODE
    WHERE (
        T.CLUSTER_ID IN ('LCS_PULSE_RHYTHM', 'LCS_PULSE_RATE')
        OR P.PROCEDURE_CODE IN ( ----------------------------------removed once clusters updated
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
    AND P.SERVICE_START_DT_TM >= ADD_MONTHS(CURRENT_DATE(), -36)

    UNION

    SELECT
        EMPI_ID,
        SOURCE_DESCRIPTION--, RESULT_CODE, SERVICE_DATE
    FROM PH_F_RESULT AS R
    LEFT JOIN JOINED_LTC_LOOKUP AS T
        ON R.RESULT_CODE = T.SNOMED_CODE
    WHERE (
        T.CLUSTER_ID IN ('LCS_PULSE_RHYTHM', 'LCS_PULSE_RATE')
        OR R.RESULT_CODE IN ( ----------------------------------removed once clusters updated
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
    AND R.SERVICE_DATE >= ADD_MONTHS(CURRENT_DATE(), -36)

),

EMIS AS (

    SELECT
        EMPI_ID,
        1 AS 'EMIS'

    FROM (

        --Excluding PC_date from base

        SELECT EMPI_ID
        FROM BASE

        EXCEPT

        SELECT EMPI_ID
        FROM PC_DATE
        WHERE SOURCE_DESCRIPTION = 'EMIS GP'

    ) AS A
),

OTHER AS (

    SELECT
        EMPI_ID,
        1 AS 'OTHER'

    FROM (

        --Excluding PC_date from base

        SELECT EMPI_ID
        FROM BASE

        EXCEPT

        SELECT EMPI_ID
        FROM PC_DATE

    ) AS B
)

-- USING 'EMIS' TABLE TO GET WHOLE COHORT AND LEFT JOINING TO SEE IF OTHER AS WELL
SELECT
    COALESCE(E.EMPI_ID, O.EMPI_ID) AS EMPI_ID,
    CASE WHEN E.EMIS = 1 THEN 'EMIS' ELSE 'Other' END AS SOURCE
FROM EMIS AS E
FULL OUTER JOIN OTHER AS O ON E.EMPI_ID = O.EMPI_ID
