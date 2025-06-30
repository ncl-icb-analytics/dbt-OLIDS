-- ICB_CF_DM_66
WITH EMIS AS (

    SELECT
        EMPI_ID,
        1 AS EMIS

    FROM
        (
            WITH BASE_POPULATION AS (

                SELECT DISTINCT B.EMPI_ID

                FROM LTC_LCS_BASE AS B
                LEFT JOIN ICS_LTC_01 AS L1 ON B.EMPI_ID = L1.EMPI_ID
                LEFT JOIN
                    HEALTH_CHECK_COMP_IN_24 AS L2
                    ON B.EMPI_ID = L2.EMPI_ID

                WHERE
                    B.AGE >= 17
                    AND L1.EMPI_ID IS NULL
                    AND L2.EMPI_ID IS NULL

                EXCEPT

                SELECT EMPI_ID
                FROM POPHEALTH_QOF_LTCS_LIST
                WHERE LTC_NAME = 'Diabetes'
            )

            -- Inclusions: Patients with a record of HbA1c more than more than or equal to 42 and less than 46

            SELECT DISTINCT INCLUSIONS.EMPI_ID

            FROM
                (
                    SELECT
                        EMPI_ID,
                        SERVICE_DATE,
                        NORM_NUMERIC_VALUE,
                        ROW_NUMBER()
                            OVER (
                                PARTITION BY EMPI_ID ORDER BY SERVICE_DATE DESC
                            )
                            AS ROW_NUM

                    FROM PH_F_RESULT

                    WHERE
                        SOURCE_DESCRIPTION = 'EMIS GP'
                        AND RESULT_CODE IN (
                            '999791000000106',
                            '1049321000000109',
                            '1049301000000100'
                        )
                        AND NORM_NUMERIC_VALUE > 0

                ) AS INCLUSIONS

            INNER JOIN
                BASE_POPULATION
                ON INCLUSIONS.EMPI_ID = BASE_POPULATION.EMPI_ID
            WHERE
                ROW_NUM = '1'
                AND NORM_NUMERIC_VALUE >= 42
                AND NORM_NUMERIC_VALUE < 46
                AND SERVICE_DATE <= ADD_MONTHS(CURRENT_DATE(), -12)

        ) AS A

),

OTHER AS (

    SELECT
        EMPI_ID,
        1 AS OTHER

    FROM
        (
            WITH BASE_POPULATION AS (

                SELECT DISTINCT B.EMPI_ID

                FROM LTC_LCS_BASE AS B
                LEFT JOIN ICS_LTC_01 AS L1 ON B.EMPI_ID = L1.EMPI_ID
                LEFT JOIN
                    HEALTH_CHECK_COMP_IN_24 AS L2
                    ON B.EMPI_ID = L2.EMPI_ID

                WHERE
                    B.AGE >= 17
                    AND L1.EMPI_ID IS NULL
                    AND L2.EMPI_ID IS NULL
            )

            -- Inclusions: Patients with a record of HbA1c more than more than or equal to 42 and less than 46

            SELECT DISTINCT INCLUSIONS.EMPI_ID

            FROM
                (
                    SELECT
                        EMPI_ID,
                        SERVICE_DATE,
                        NORM_NUMERIC_VALUE,
                        ROW_NUMBER()
                            OVER (
                                PARTITION BY EMPI_ID ORDER BY SERVICE_DATE DESC
                            )
                            AS ROW_NUM

                    FROM PH_F_RESULT

                    WHERE
                        --SOURCE_DESCRIPTION = 'EMIS GP' AND
                        RESULT_CODE IN (
                            '999791000000106',
                            '1049321000000109',
                            '1049301000000100'
                        )
                        AND NORM_NUMERIC_VALUE > 0

                ) AS INCLUSIONS

            INNER JOIN
                BASE_POPULATION
                ON INCLUSIONS.EMPI_ID = BASE_POPULATION.EMPI_ID
            WHERE
                ROW_NUM = '1'
                AND NORM_NUMERIC_VALUE >= 42
                AND NORM_NUMERIC_VALUE < 46
                AND SERVICE_DATE <= ADD_MONTHS(CURRENT_DATE(), -12)

        ) AS A

)

SELECT
    COALESCE(E.EMPI_ID, O.EMPI_ID) AS EMPI_ID,
    CASE WHEN E.EMIS = 1 THEN 'EMIS' ELSE 'Other' END AS SOURCE

FROM EMIS AS E
FULL OUTER JOIN OTHER AS O ON E.EMPI_ID = O.EMPI_ID
