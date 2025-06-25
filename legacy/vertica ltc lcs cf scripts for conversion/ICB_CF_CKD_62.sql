--ICB_CF_CKD_62 new logic focussing on UACR
--6.9.23 repoint to ICS_LTC_01 REDO base population to match CKD_61 & add in LOINC code for OTHER UACR AND UACR >4
--13.9.23 limiting to one result per day (max) and removing same result on two adjacent days
--11.10.23 Adding LOINC '32294-1' to UACR for Other CTE

--- CREATING CTE FOR EMIS SOURCED
WITH EMIS AS (
    SELECT
        EMPI_ID,
        1 AS EMIS
    FROM
        (
            WITH BASE_POPULATION AS (
                SELECT B.EMPI_ID FROM LTC_LCS_BASE AS B
                LEFT JOIN
                    POPHEALTH_QOF_LTCS_LIST AS L
                    ON
                        B.EMPI_ID = L.EMPI_ID
                        AND L.LTC_NAME IN ('Chronic Kidney Disease', 'Diabetes')
                LEFT JOIN ICS_LTC_01 AS L2 ON B.EMPI_ID = L2.EMPI_ID
                WHERE
                    B.AGE >= 17
                    AND L.EMPI_ID IS NULL
                    AND L2.EMPI_ID IS NULL
            ),

            -- CREATING INCLUSION CRITERIA FOR PATIENTS WITH 2 OR MORE UACRs RECORDED WITH A VALUE >0, AND REMOVING MULTIPLE RESULTS ON SAME DAY
            PATIENT_LIST AS (
                SELECT DISTINCT
                    C.EMPI_ID,
                    SOURCE_DESCRIPTION,
                    SERVICE_DATE,
                    RESULT_CODE,
                    MAX(NORM_NUMERIC_VALUE) AS NORM_NUMERIC_VALUE
                FROM BASE_POPULATION AS B
                INNER JOIN PH_F_RESULT AS C ON B.EMPI_ID = C.EMPI_ID
                WHERE
                    C.RESULT_CODE IN ('1023491000000104')
                    AND SOURCE_DESCRIPTION = 'EMIS GP'
                    AND C.NORM_NUMERIC_VALUE > 0
                GROUP BY
                    C.EMPI_ID, SOURCE_DESCRIPTION, SERVICE_DATE, RESULT_CODE
            ),

            -- CHECKING FOR SAME RESULTS ONE DAY APART
            SAMERESULT AS (
                SELECT
                    *,
                    CASE
                        WHEN
                            SERVICE_DATE + 1
                            = LAG(SERVICE_DATE)
                                OVER (
                                    PARTITION BY PL.EMPI_ID
                                    ORDER BY SERVICE_DATE DESC
                                )
                            AND NORM_NUMERIC_VALUE
                            = LAG(NORM_NUMERIC_VALUE)
                                OVER (
                                    PARTITION BY PL.EMPI_ID
                                    ORDER BY SERVICE_DATE DESC
                                )
                            THEN 'EXCLUDE'
                        ELSE 'INCLUDE'
                    END AS SAMERESULTDIFFDAY
                FROM PATIENT_LIST AS PL
            ),

            -- FINDING AND INDEXING ALL UACR TESTS
            PATIENT_ROWS AS (
                SELECT
                    *,
                    ROW_NUMBER()
                        OVER (PARTITION BY EMPI_ID ORDER BY SERVICE_DATE DESC)
                        AS ROW_NUMBER
                FROM SAMERESULT
                WHERE SAMERESULTDIFFDAY = 'INCLUDE'
            --WHERE RESULT_CODE = '1023491000000104'
            ),

            -- REMOVING PATIENTS WITH ONLY ONE UACR (WE NEED 2 OR MORE)
            ROWS_OVER1 AS (
                SELECT
                    EMPI_ID,
                    COUNT(*)
                FROM PATIENT_ROWS
                GROUP BY 1
                HAVING COUNT(*) > 1
            ),

            -- FINDING THE TOP 2 MOST RECENT EGFR TESTS WITH A VALUE ABOVE 4
            RECENT2 AS (
                SELECT
                    PR.EMPI_ID,
                    SOURCE_DESCRIPTION,
                    SERVICE_DATE,
                    NORM_NUMERIC_VALUE,
                    LAG(SERVICE_DATE)
                        OVER (
                            PARTITION BY PR.EMPI_ID ORDER BY SERVICE_DATE DESC
                        )
                        AS SERVICE_DATE2,
                    LAG(NORM_NUMERIC_VALUE)
                        OVER (
                            PARTITION BY PR.EMPI_ID ORDER BY SERVICE_DATE DESC
                        )
                        AS NORM_NUMERIC_VALUE2
                FROM PATIENT_ROWS AS PR
                INNER JOIN ROWS_OVER1 ON PR.EMPI_ID = ROWS_OVER1.EMPI_ID
                WHERE
                    ROW_NUMBER IN ('1', '2')
            )

            -- FINAL TABLE
            SELECT DISTINCT
                EMPI_ID,
                SOURCE_DESCRIPTION
            FROM RECENT2
            WHERE NORM_NUMERIC_VALUE > 4 AND NORM_NUMERIC_VALUE2 > 4

        ) AS A
),

--- CREATING CTE FOR ALL SOURCED
OTHER AS (
    SELECT
        EMPI_ID,
        1 AS OTHER
    FROM
        (
            WITH BASE_POPULATION AS (
                SELECT B.EMPI_ID FROM LTC_LCS_BASE AS B
                LEFT JOIN
                    POPHEALTH_QOF_LTCS_LIST AS L
                    ON
                        B.EMPI_ID = L.EMPI_ID
                        AND L.LTC_NAME IN ('Chronic Kidney Disease', 'Diabetes')
                LEFT JOIN ICS_LTC_01 AS L2 ON B.EMPI_ID = L2.EMPI_ID
                WHERE
                    B.AGE >= 17
                    AND L.EMPI_ID IS NULL
                    AND L2.EMPI_ID IS NULL
            ),

            -- CREATING INCLUSION CRITERIA FOR PATIENTS WITH 2 OR MORE UACRs RECORDED WITH A VALUE >0, AND REMOVING MULTIPLE RESULTS ON SAME DAY
            PATIENT_LIST AS (
                SELECT DISTINCT
                    C.EMPI_ID,
                    SOURCE_DESCRIPTION,
                    SERVICE_DATE,
                    RESULT_CODE,
                    MAX(NORM_NUMERIC_VALUE) AS NORM_NUMERIC_VALUE
                FROM BASE_POPULATION AS B
                INNER JOIN PH_F_RESULT AS C ON B.EMPI_ID = C.EMPI_ID
                WHERE
                    C.RESULT_CODE IN ('1023491000000104', '32294-1')
                    AND C.NORM_NUMERIC_VALUE > 0
                GROUP BY
                    C.EMPI_ID, SOURCE_DESCRIPTION, SERVICE_DATE, RESULT_CODE
            ),

            -- CHECKING FOR SAME RESULTS ONE DAY APART
            SAMERESULT AS (
                SELECT
                    *,
                    CASE
                        WHEN
                            SERVICE_DATE + 1
                            = LAG(SERVICE_DATE)
                                OVER (
                                    PARTITION BY PL.EMPI_ID
                                    ORDER BY SERVICE_DATE DESC
                                )
                            AND NORM_NUMERIC_VALUE
                            = LAG(NORM_NUMERIC_VALUE)
                                OVER (
                                    PARTITION BY PL.EMPI_ID
                                    ORDER BY SERVICE_DATE DESC
                                )
                            THEN 'EXCLUDE'
                        ELSE 'INCLUDE'
                    END AS SAMERESULTDIFFDAY
                FROM PATIENT_LIST AS PL
            ),

            -- FINDING AND INDEXING ALL UACR TESTS
            PATIENT_ROWS AS (
                SELECT
                    *,
                    ROW_NUMBER()
                        OVER (PARTITION BY EMPI_ID ORDER BY SERVICE_DATE DESC)
                        AS ROW_NUMBER
                FROM SAMERESULT
                WHERE SAMERESULTDIFFDAY = 'INCLUDE'
            --WHERE RESULT_CODE = '1023491000000104'
            ),

            -- REMOVING PATIENTS WITH ONLY ONE UACR (WE NEED 2 OR MORE)
            ROWS_OVER1 AS (
                SELECT
                    EMPI_ID,
                    COUNT(*)
                FROM PATIENT_ROWS
                GROUP BY 1
                HAVING COUNT(*) > 1
            ),

            -- FINDING THE TOP 2 MOST RECENT UACR TESTS WITH A VALUE ABOVE 4
            RECENT2 AS (
                SELECT
                    PR.EMPI_ID,
                    SOURCE_DESCRIPTION,
                    SERVICE_DATE,
                    NORM_NUMERIC_VALUE,
                    LAG(SERVICE_DATE)
                        OVER (
                            PARTITION BY PR.EMPI_ID ORDER BY SERVICE_DATE DESC
                        )
                        AS SERVICE_DATE2,
                    LAG(NORM_NUMERIC_VALUE)
                        OVER (
                            PARTITION BY PR.EMPI_ID ORDER BY SERVICE_DATE DESC
                        )
                        AS NORM_NUMERIC_VALUE2
                FROM PATIENT_ROWS AS PR
                INNER JOIN ROWS_OVER1 ON PR.EMPI_ID = ROWS_OVER1.EMPI_ID
                WHERE
                    ROW_NUMBER IN ('1', '2')
            )

            -- FINAL TABLE
            SELECT DISTINCT
                EMPI_ID,
                SOURCE_DESCRIPTION
            FROM RECENT2
            WHERE NORM_NUMERIC_VALUE > 4 AND NORM_NUMERIC_VALUE2 > 4
        ) AS B
)

-- USING 'OTHER' TABLE TO GET WHOLE COHORT AND LEFT JOINING TO SEE WHETHER THEY ARE EMIS SOURCED OR NOT
SELECT
    COALESCE(E.EMPI_ID, O.EMPI_ID) AS EMPI_ID,
    CASE WHEN E.EMIS = 1 THEN 'EMIS' ELSE 'Other' END AS SOURCE
FROM EMIS AS E
FULL OUTER JOIN OTHER AS O ON E.EMPI_ID = O.EMPI_ID
