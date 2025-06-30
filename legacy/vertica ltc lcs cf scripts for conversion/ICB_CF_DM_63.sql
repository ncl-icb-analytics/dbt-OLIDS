-- ICB_CF_DM_63
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

            -- Inclusions: Latest Hb1Ac value is more than or equal to 46 but less than 48 and no Hb1Ac in the last year

            SELECT DISTINCT EMPI_ID

            FROM
                (
                    SELECT
                        B.EMPI_ID,
                        R.SERVICE_DATE,
                        R.NORM_NUMERIC_VALUE,
                        ROW_NUMBER()
                            OVER (
                                PARTITION BY B.EMPI_ID
                                ORDER BY R.SERVICE_DATE DESC
                            )
                            AS ROW_NUM

                    FROM BASE_POPULATION AS B
                    LEFT OUTER JOIN PH_F_RESULT AS R ON B.EMPI_ID = R.EMPI_ID
                    INNER JOIN
                        JOINED_LTC_LOOKUP AS LOOKUP
                        ON R.RESULT_CODE = LOOKUP.SNOMED_CODE

                    WHERE
                        R.SOURCE_DESCRIPTION = 'EMIS GP'
                        AND LOOKUP.CLUSTER_ID = 'IFCCHBAM_COD'
                        AND R.NORM_NUMERIC_VALUE > 0

                ) AS INCLUSIONS

            WHERE
                ROW_NUM = '1'
                AND NORM_NUMERIC_VALUE >= 46 AND NORM_NUMERIC_VALUE < 48
                AND SERVICE_DATE <= ADD_MONTHS(CURRENT_DATE(), -12)

        ) AS A

),

-- ICB_CF_DM_63
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

            -- Inclusions: Latest Hb1Ac value is more than or equal to 46 but less than 48 and no Hb1Ac in the last year

            SELECT DISTINCT EMPI_ID

            FROM
                (
                    SELECT
                        B.EMPI_ID,
                        R.SERVICE_DATE,
                        R.NORM_NUMERIC_VALUE,
                        ROW_NUMBER()
                            OVER (
                                PARTITION BY B.EMPI_ID
                                ORDER BY R.SERVICE_DATE DESC
                            )
                            AS ROW_NUM

                    FROM BASE_POPULATION AS B
                    LEFT OUTER JOIN PH_F_RESULT AS R ON B.EMPI_ID = R.EMPI_ID
                    INNER JOIN
                        JOINED_LTC_LOOKUP AS LOOKUP
                        ON R.RESULT_CODE = LOOKUP.SNOMED_CODE

                    WHERE
                        --R.SOURCE_DESCRIPTION = 'EMIS GP' AND
                        LOOKUP.CLUSTER_ID = 'IFCCHBAM_COD'
                        AND R.NORM_NUMERIC_VALUE > 0

                ) AS INCLUSIONS

            WHERE
                ROW_NUM = '1'
                AND NORM_NUMERIC_VALUE >= 46 AND NORM_NUMERIC_VALUE < 48
                AND SERVICE_DATE <= ADD_MONTHS(CURRENT_DATE(), -12)

        ) AS A

)

SELECT
    COALESCE(E.EMPI_ID, O.EMPI_ID) AS EMPI_ID,
    CASE WHEN E.EMIS = 1 THEN 'EMIS' ELSE 'Other' END AS SOURCE

FROM EMIS AS E
FULL OUTER JOIN OTHER AS O ON E.EMPI_ID = O.EMPI_ID
