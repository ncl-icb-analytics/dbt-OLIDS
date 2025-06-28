WITH RESULTS AS (
    SELECT * FROM

        (
            SELECT DISTINCT
                B.EMPI_ID,
                NORM_NUMERIC_VALUE AS VALUE,
                CLUSTER_ID,
                RESULT_CODE,
                R.SOURCE_DESCRIPTION,
                B.EMIS,
                B.OTHER,
                ROW_NUMBER()
                    OVER (
                        PARTITION BY B.EMPI_ID, R.SOURCE_DESCRIPTION, CLUSTER_ID
                        ORDER BY R.SERVICE_DATE DESC, NORM_NUMERIC_VALUE DESC
                    )
                    AS ROW_NUM

            FROM ICB_CF_HTN_61_BASE AS B
            INNER JOIN PH_F_RESULT AS R ON B.EMPI_ID = R.EMPI_ID
            INNER JOIN
                JOINED_LTC_LOOKUP AS T
                ON R.RESULT_CODE = T.SNOMED_CODE

            WHERE
                T.CLUSTER_ID IN ('HTN_SYSTOLIC_1', 'HTN_DIASTOLIC_1')
                AND R.NORM_NUMERIC_VALUE > 0
                AND DATE(R.SERVICE_DATE) <= CURRENT_DATE()
        ) AS RESULTS

    WHERE ROW_NUM = '1'

),

-- Creating CTE for EMIS sourced
EMIS AS (

    SELECT DISTINCT
        A.EMPI_ID,
        1 AS 'EMIS'

    FROM
        (

            SELECT DISTINCT R.EMPI_ID

            FROM RESULTS AS R
            INNER JOIN JOINED_LTC_LOOKUP AS L ON R.RESULT_CODE = L.SNOMED_CODE

            WHERE
                EMIS = '1' AND SOURCE_DESCRIPTION = 'EMIS GP'
                AND R.CLUSTER_ID = 'HTN_SYSTOLIC_1'
                AND L.CLUSTER_ID = 'HTN_SYSTOLIC_2'
                AND VALUE >= 180

            UNION

            SELECT DISTINCT R.EMPI_ID

            FROM RESULTS AS R
            INNER JOIN JOINED_LTC_LOOKUP AS L ON R.RESULT_CODE = L.SNOMED_CODE

            WHERE
                EMIS = '1' AND SOURCE_DESCRIPTION = 'EMIS GP'
                AND R.CLUSTER_ID = 'HTN_DIASTOLIC_1'
                AND L.CLUSTER_ID = 'HTN_DIASTOLIC_2'
                AND VALUE >= 120

            UNION

            SELECT DISTINCT R.EMPI_ID

            FROM RESULTS AS R
            INNER JOIN JOINED_LTC_LOOKUP AS L ON R.RESULT_CODE = L.SNOMED_CODE

            WHERE
                EMIS = '1' AND SOURCE_DESCRIPTION = 'EMIS GP'
                AND R.CLUSTER_ID = 'HTN_SYSTOLIC_1'
                AND L.CLUSTER_ID = 'HTN_AMBULATORY_SYSTOLIC'
                AND VALUE >= 170

            UNION

            SELECT DISTINCT R.EMPI_ID

            FROM RESULTS AS R
            INNER JOIN JOINED_LTC_LOOKUP AS L ON R.RESULT_CODE = L.SNOMED_CODE

            WHERE
                EMIS = '1' AND SOURCE_DESCRIPTION = 'EMIS GP'
                AND R.CLUSTER_ID = 'HTN_DIASTOLIC_1'
                AND L.CLUSTER_ID = 'HTN_AMBULATORY_DIASTOLIC'
                AND VALUE >= 115

        ) AS A

),

-- Creating CTE for all sources
OTHER AS (

    SELECT DISTINCT
        A.EMPI_ID,
        1 AS OTHER

    FROM
        (

            SELECT DISTINCT R.EMPI_ID

            FROM RESULTS AS R
            INNER JOIN JOINED_LTC_LOOKUP AS L ON R.RESULT_CODE = L.SNOMED_CODE

            WHERE
                R.CLUSTER_ID = 'HTN_SYSTOLIC_1'
                AND L.CLUSTER_ID = 'HTN_SYSTOLIC_2'
                AND VALUE >= 180

            UNION

            SELECT DISTINCT R.EMPI_ID

            FROM RESULTS AS R
            INNER JOIN JOINED_LTC_LOOKUP AS L ON R.RESULT_CODE = L.SNOMED_CODE

            WHERE
                R.CLUSTER_ID = 'HTN_DIASTOLIC_1'
                AND L.CLUSTER_ID = 'HTN_DIASTOLIC_2'
                AND VALUE >= 120

            UNION

            SELECT DISTINCT R.EMPI_ID

            FROM RESULTS AS R
            INNER JOIN JOINED_LTC_LOOKUP AS L ON R.RESULT_CODE = L.SNOMED_CODE

            WHERE
                R.CLUSTER_ID = 'HTN_SYSTOLIC_1'
                AND L.CLUSTER_ID = 'HTN_AMBULATORY_SYSTOLIC'
                AND VALUE >= 170

            UNION

            SELECT DISTINCT R.EMPI_ID

            FROM RESULTS AS R
            INNER JOIN JOINED_LTC_LOOKUP AS L ON R.RESULT_CODE = L.SNOMED_CODE

            WHERE
                R.CLUSTER_ID = 'HTN_DIASTOLIC_1'
                AND L.CLUSTER_ID = 'HTN_AMBULATORY_DIASTOLIC'
                AND VALUE >= 115

        ) AS A

)

-- USING 'OTHER' TABLE TO GET WHOLE COHORT AND LEFT JOINING TO SEE WHETHER THEY ARE EMIS SOURCED OR NOT
SELECT
    COALESCE(E.EMPI_ID, O.EMPI_ID) AS EMPI_ID,
    CASE WHEN E.EMIS = 1 THEN 'EMIS' ELSE 'Other' END AS SOURCE
FROM EMIS AS E
FULL OUTER JOIN OTHER AS O ON E.EMPI_ID = O.EMPI_ID
