---ICB_CF_CKD_63: Patients with latest uACR >70
--6.9.23 - repoint to ICS_LTC_01 and add in new logic for UACR codes and LOINC code

--- CREATING CTE FOR EMIS SOURCED
WITH EMIS AS (
    SELECT
        EMPI_ID,
        1 AS EMIS
    FROM (

        -- CREATING INCLUSION CRITERIA FOR PATIENTS WITH LATEST UACR RECORDED WITH A VALUE >70
        -- Below excludes patients that are on Diabetes Register, ICS_LTC_01 and CKD Register and included in ICB_CF_CKD_62
        WITH BASE_POPULATION AS (
            SELECT B.EMPI_ID
            FROM LTC_LCS_BASE AS B
            LEFT JOIN
                POPHEALTH_QOF_LTCS_LIST AS L
                ON
                    B.EMPI_ID = L.EMPI_ID
                    AND L.LTC_NAME IN ('Chronic Kidney Disease', 'Diabetes')
            LEFT JOIN ICS_LTC_01 AS L2 ON B.EMPI_ID = L2.EMPI_ID
            LEFT JOIN ICB_CF_CKD_62 AS L3 ON B.EMPI_ID = L3.EMPI_ID

            WHERE
                B.AGE >= 17
                AND L.EMPI_ID IS NULL
                AND L2.EMPI_ID IS NULL
                AND L3.EMPI_ID IS NULL
        ),

        -- CREATING INCLUSION CRITERIA FOR PATIENTS UACRs RECORDED WITH A VALUE >0
        PATIENT_LIST AS (
            SELECT DISTINCT
                C.EMPI_ID,
                SOURCE_DESCRIPTION,
                SERVICE_DATE,
                NORM_NUMERIC_VALUE,
                RESULT_CODE
            FROM BASE_POPULATION AS B
            INNER JOIN PH_F_RESULT AS C ON B.EMPI_ID = C.EMPI_ID
            WHERE
                C.RESULT_CODE IN ('1023491000000104', '149861000000104')
                AND SOURCE_DESCRIPTION = 'EMIS GP'
                AND C.NORM_NUMERIC_VALUE > 0
        ),

        -- FINDING AND INDEXING ALL UACR TESTS
        PATIENT_ROWS AS (
            SELECT
                *,
                ROW_NUMBER()
                    OVER (PARTITION BY EMPI_ID ORDER BY SERVICE_DATE DESC)
                    AS ROW_NUMBER
            FROM PATIENT_LIST
        )

        -- FINDING THE MOST RECENT UACR TEST WITH A VALUE ABOVE 70
        SELECT
            PR.EMPI_ID,
            SOURCE_DESCRIPTION,
            SERVICE_DATE
        FROM PATIENT_ROWS AS PR
        WHERE ROW_NUMBER = '1' AND NORM_NUMERIC_VALUE > 70

    ) AS A
),

--- CREATING CTE FOR ALL SOURCED
OTHER AS (
    SELECT
        EMPI_ID,
        1 AS OTHER
    FROM (

        -- CREATING INCLUSION CRITERIA FOR PATIENTS WITH LATEST UACR RECORDED WITH A VALUE >70
        -- Below excludes patients that are on Diabetes Register, ICS_LTC_01 and CKD Register and included in ICB_CF_CKD_62
        WITH BASE_POPULATION AS (
            SELECT B.EMPI_ID
            FROM LTC_LCS_BASE AS B
            LEFT JOIN
                POPHEALTH_QOF_LTCS_LIST AS L
                ON
                    B.EMPI_ID = L.EMPI_ID
                    AND L.LTC_NAME IN ('Chronic Kidney Disease', 'Diabetes')
            LEFT JOIN ICS_LTC_01 AS L2 ON B.EMPI_ID = L2.EMPI_ID
            LEFT JOIN ICB_CF_CKD_62 AS L3 ON B.EMPI_ID = L3.EMPI_ID
            WHERE
                B.AGE >= 17
                AND L.EMPI_ID IS NULL
                AND L2.EMPI_ID IS NULL
                AND L3.EMPI_ID IS NULL
        ),

        --incluse LOINC code for OTHER source
        PATIENT_LIST AS (
            SELECT DISTINCT
                C.EMPI_ID,
                SOURCE_DESCRIPTION,
                SERVICE_DATE,
                NORM_NUMERIC_VALUE,
                RESULT_CODE
            FROM BASE_POPULATION AS B
            INNER JOIN PH_F_RESULT AS C ON B.EMPI_ID = C.EMPI_ID
            WHERE
                C.RESULT_CODE IN (
                    '1023491000000104', '149861000000104', '32294-1'
                )
                AND C.NORM_NUMERIC_VALUE > 0
        ),

        -- FINDING AND INDEXING ALL UACR TESTS
        PATIENT_ROWS AS (
            SELECT
                *,
                ROW_NUMBER()
                    OVER (PARTITION BY EMPI_ID ORDER BY SERVICE_DATE DESC)
                    AS ROW_NUMBER
            FROM PATIENT_LIST
        )

        -- FINDING THE MOST RECENT EGFR TEST WITH A VALUE ABOVE 70
        SELECT
            PR.EMPI_ID,
            SOURCE_DESCRIPTION,
            SERVICE_DATE
        FROM PATIENT_ROWS AS PR
        WHERE ROW_NUMBER = '1' AND NORM_NUMERIC_VALUE > 70

    ) AS B
)

-- USING 'OTHER' TABLE TO GET WHOLE COHORT AND LEFT JOINING TO SEE WHETHER THEY ARE EMIS SOURCED OR NOT
SELECT
    COALESCE(E.EMPI_ID, O.EMPI_ID) AS EMPI_ID,
    CASE WHEN E.EMIS = 1 THEN 'EMIS' ELSE 'Other' END AS SOURCE
FROM EMIS AS E
FULL OUTER JOIN OTHER AS O ON E.EMPI_ID = O.EMPI_ID
