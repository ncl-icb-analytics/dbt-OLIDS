--ICB_CF_HTN_65_GP_3A

-- Creating a population from base that excludes the highest priority patients from higher priority groups
WITH Population AS (
    SELECT DISTINCT EMPI_ID

    FROM ICB_CF_HTN_61_BASE

    EXCEPT
    SELECT EMPI_ID FROM ICB_CF_HTN_61_GP_1

    EXCEPT
    SELECT EMPI_ID FROM ICB_CF_HTN_62_GP_2A

    EXCEPT
    SELECT EMPI_ID FROM ICB_CF_HTN_63_GP_2B
),

-- Select latest reading for systolic and diastolic
RULE_4_TABLE AS (
    SELECT
        EMPI_ID,
        CLUSTER_ID,
        SOURCE_DESCRIPTION,
        RESULT_CODE,
        NORM_NUMERIC_VALUE

    FROM (
        SELECT DISTINCT
            P.EMPI_ID,
            SOURCE_DESCRIPTION,
            L.CLUSTER_ID,
            R.RESULT_CODE,
            NORM_NUMERIC_VALUE,
            SERVICE_DATE,
            ROW_NUMBER()
                OVER (
                    PARTITION BY P.EMPI_ID, R.SOURCE_DESCRIPTION, L.CLUSTER_ID
                    ORDER BY R.SERVICE_DATE DESC, NORM_NUMERIC_VALUE DESC
                )
                AS ROW_NUMBER

        FROM Population AS P
        INNER JOIN PH_F_RESULT AS R ON P.EMPI_ID = R.EMPI_ID
        INNER JOIN JOINED_LTC_LOOKUP AS L ON R.RESULT_CODE = L.SNOMED_CODE

        WHERE
            L.CLUSTER_ID IN ('HTN_SYSTOLIC_1', 'HTN_DIASTOLIC_1')
            AND R.NORM_NUMERIC_VALUE > 0
            AND DATE(R.SERVICE_DATE) <= CURRENT_DATE()

    ) AS RESULT
    WHERE ROW_NUMBER = 1
),

RULE_5_TABLE AS (

-- Myocardial, cerebral and claudication
    SELECT DISTINCT
        EMPI_ID,
        SOURCE_DESCRIPTION

    FROM PH_F_CONDITION AS C
    INNER JOIN JOINED_LTC_LOOKUP AS L ON C.CONDITION_CODE = L.SNOMED_CODE

    WHERE
        L.CLUSTER_ID IN ('HTN_MYOCARDIAL', 'HTN_CEREBRAL', 'HTN_CLAUDICATION')

    UNION

    -- CKD
    SELECT DISTINCT
        EMPI_ID,
        SOURCE_DESCRIPTION

    FROM PH_F_CONDITION AS C
    INNER JOIN JOINED_LTC_LOOKUP AS L ON C.CONDITION_CODE = L.SNOMED_CODE

    WHERE
        L.CLUSTER_ID = 'HTN_CKD' AND EFFECTIVE_DT_TM <= CURRENT_DATE()

    UNION

    -- Latest GFR less than 60
    SELECT DISTINCT
        EMPI_ID,
        SOURCE_DESCRIPTION

    FROM (
        SELECT DISTINCT
            EMPI_ID,
            SERVICE_DATE,
            SOURCE_DESCRIPTION,
            NORM_NUMERIC_VALUE,
            ROW_NUMBER()
                OVER (
                    PARTITION BY EMPI_ID, SOURCE_DESCRIPTION
                    ORDER BY SERVICE_DATE DESC
                )
                AS ROW_NUMBER

        FROM PH_F_RESULT AS R
        INNER JOIN JOINED_LTC_LOOKUP AS L ON R.RESULT_CODE = L.SNOMED_CODE

        WHERE
            L.CLUSTER_ID = 'HTN_EGFR'
    ) AS GFR

    WHERE ROW_NUMBER = '1' AND NORM_NUMERIC_VALUE < 60

    UNION

    -- Diabetes
    SELECT DISTINCT
        EMPI_ID,
        '' AS SOURCE_DESCRIPTION

    FROM PH_F_CONDITION AS C
    INNER JOIN JOINED_LTC_LOOKUP AS L ON C.CONDITION_CODE = L.SNOMED_CODE

    WHERE
        L.CLUSTER_ID = 'HTN_DIABETES' AND EFFECTIVE_DT_TM <= CURRENT_DATE()

    UNION

    -- Black and South Asian population
    SELECT DISTINCT
        Q.EMPI_ID,
        Q.SOURCE_DESCRIPTION

    FROM PH_F_QUESTIONNAIRE_QUESTION AS Q
    INNER JOIN
        PH_F_QUESTIONNAIRE_ANSWER AS A
        ON Q.EMPI_ID = A.EMPI_ID AND Q.QUESTION_ID = A.QUESTION_ID
    INNER JOIN
        JOINED_LTC_LOOKUP AS L
        ON A.ORIG_CODIFIED_VALUE_CODE = L.SNOMED_CODE

    WHERE CLUSTER_ID = 'HTN_BSA_COD'

),

-- Creating CTE for EMIS sourced
EMIS AS (

    SELECT DISTINCT
        A.EMPI_ID,
        1 AS 'EMIS'

    FROM
        (

            SELECT DISTINCT R4.EMPI_ID

            FROM RULE_4_TABLE AS R4
            INNER JOIN JOINED_LTC_LOOKUP AS L ON R4.RESULT_CODE = L.SNOMED_CODE

            WHERE
                SOURCE_DESCRIPTION = 'EMIS GP'
                AND R4.CLUSTER_ID = 'HTN_SYSTOLIC_1'
                AND L.CLUSTER_ID = 'HTN_SYSTOLIC_2'
                AND NORM_NUMERIC_VALUE >= 140

            UNION

            SELECT DISTINCT R4.EMPI_ID

            FROM RULE_4_TABLE AS R4
            INNER JOIN JOINED_LTC_LOOKUP AS L ON R4.RESULT_CODE = L.SNOMED_CODE

            WHERE
                SOURCE_DESCRIPTION = 'EMIS GP'
                AND R4.CLUSTER_ID = 'HTN_DIASTOLIC_1'
                AND L.CLUSTER_ID = 'HTN_DIASTOLIC_2'
                AND NORM_NUMERIC_VALUE >= 90

            UNION

            SELECT DISTINCT R4.EMPI_ID

            FROM RULE_4_TABLE AS R4
            INNER JOIN JOINED_LTC_LOOKUP AS L ON R4.RESULT_CODE = L.SNOMED_CODE

            WHERE
                SOURCE_DESCRIPTION = 'EMIS GP'
                AND R4.CLUSTER_ID = 'HTN_SYSTOLIC_1'
                AND L.CLUSTER_ID = 'HTN_AMBULATORY_SYSTOLIC'
                AND NORM_NUMERIC_VALUE >= 135

            UNION

            SELECT DISTINCT R4.EMPI_ID

            FROM RULE_4_TABLE AS R4
            INNER JOIN JOINED_LTC_LOOKUP AS L ON R4.RESULT_CODE = L.SNOMED_CODE

            WHERE
                SOURCE_DESCRIPTION = 'EMIS GP'
                AND R4.CLUSTER_ID = 'HTN_DIASTOLIC_1'
                AND L.CLUSTER_ID = 'HTN_AMBULATORY_DIASTOLIC'
                AND NORM_NUMERIC_VALUE >= 85

        ) AS A

    INNER JOIN
        RULE_5_TABLE AS R5
        ON A.EMPI_ID = R5.EMPI_ID AND SOURCE_DESCRIPTION = 'EMIS GP'

),

OTHER AS (

    SELECT DISTINCT
        A.EMPI_ID,
        1 AS 'OTHER'

    FROM
        (

            SELECT DISTINCT R4.EMPI_ID

            FROM RULE_4_TABLE AS R4
            INNER JOIN JOINED_LTC_LOOKUP AS L ON R4.RESULT_CODE = L.SNOMED_CODE

            WHERE
                --SOURCE_DESCRIPTION = 'EMIS GP' AND
                R4.CLUSTER_ID = 'HTN_SYSTOLIC_1'
                AND L.CLUSTER_ID = 'HTN_SYSTOLIC_2'
                AND NORM_NUMERIC_VALUE >= 140

            UNION

            SELECT DISTINCT R4.EMPI_ID

            FROM RULE_4_TABLE AS R4
            INNER JOIN JOINED_LTC_LOOKUP AS L ON R4.RESULT_CODE = L.SNOMED_CODE

            WHERE
                --SOURCE_DESCRIPTION = 'EMIS GP' AND
                R4.CLUSTER_ID = 'HTN_DIASTOLIC_1'
                AND L.CLUSTER_ID = 'HTN_DIASTOLIC_2'
                AND NORM_NUMERIC_VALUE >= 90

            UNION

            SELECT DISTINCT R4.EMPI_ID

            FROM RULE_4_TABLE AS R4
            INNER JOIN JOINED_LTC_LOOKUP AS L ON R4.RESULT_CODE = L.SNOMED_CODE

            WHERE
                --SOURCE_DESCRIPTION = 'EMIS GP' AND
                R4.CLUSTER_ID = 'HTN_SYSTOLIC_1'
                AND L.CLUSTER_ID = 'HTN_AMBULATORY_SYSTOLIC'
                AND NORM_NUMERIC_VALUE >= 135

            UNION

            SELECT DISTINCT R4.EMPI_ID

            FROM RULE_4_TABLE AS R4
            INNER JOIN JOINED_LTC_LOOKUP AS L ON R4.RESULT_CODE = L.SNOMED_CODE

            WHERE
                --SOURCE_DESCRIPTION = 'EMIS GP' AND
                R4.CLUSTER_ID = 'HTN_DIASTOLIC_1'
                AND L.CLUSTER_ID = 'HTN_AMBULATORY_DIASTOLIC'
                AND NORM_NUMERIC_VALUE >= 85

        ) AS A

    INNER JOIN RULE_5_TABLE AS R5 ON A.EMPI_ID = R5.EMPI_ID --AND SOURCE_DESCRIPTION = 'EMIS GP'

)

-- USING 'OTHER' TABLE TO GET WHOLE COHORT AND LEFT JOINING TO SEE WHETHER THEY ARE EMIS SOURCED OR NOT
SELECT
    COALESCE(E.Empi_Id, O.Empi_Id) AS Empi_Id,
    CASE WHEN E.EMIS = 1 THEN 'EMIS' ELSE 'Other' END AS SOURCE
FROM EMIS AS E
FULL OUTER JOIN OTHER AS O ON E.Empi_Id = O.Empi_Id
