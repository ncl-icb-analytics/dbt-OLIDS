--- CREATING CTE FOR EMIS SOURCED
WITH EMIS AS (
    SELECT
        EMPI_ID,
        1 AS 'EMIS'
    FROM (
        -- CREATING CTE FOR MOST RECENT QDIABETES SCORE
        WITH QDIAB AS (
            SELECT
                EMPI_ID,
                NORM_NUMERIC_VALUE,
                SERVICE_LOCAL_DATE_ID AS DATE_OF_LAST_QDIAB,
                SOURCE_DESCRIPTION
            FROM PH_F_RESULT AS C
            WHERE
                RESULT_RAW_CODE IN ('863501000000102')
                AND C.SOURCE_DESCRIPTION = 'EMIS GP'
                AND (EMPI_ID, SERVICE_LOCAL_DATE_ID) IN (
                    SELECT
                        EMPI_ID,
                        MAX(SERVICE_LOCAL_DATE_ID) AS SERVICE_LOCAL_DATE_ID
                    FROM PH_F_RESULT
                    GROUP BY EMPI_ID
                )
        ),

        -- CREATING A) INCLUSION COHORT FOR HBA1C OVER OR EQUAL TO 42 WITHIN LAST 5 YEARS
        INCLUSIONA AS (
            SELECT DISTINCT
                EMPI_ID,
                SOURCE_DESCRIPTION
            FROM PH_F_RESULT AS C
            INNER JOIN JOINED_LTC_LOOKUP AS T
                ON
                    T.SNOMED_CODE = C.RESULT_CODE
                    AND T.CLUSTER_ID IN ('HBA1C')
                    AND C.NORM_NUMERIC_VALUE >= 42
                    AND DATE(C.SERVICE_DATE) >= ADD_MONTHS(CURRENT_DATE(), -60)
                    AND C.SOURCE_DESCRIPTION = 'EMIS GP'
        ),

        -- CREATING B) INCLUSION COHORT FOR MOST RECENT Q DIABETES OVER OR EQUAL TO 5.6
        INCLUSIONB AS (
            SELECT DISTINCT
                B.EMPI_ID,
                SOURCE_DESCRIPTION
            FROM LTC_LCS_BASE AS B
            INNER JOIN QDIAB ON B.EMPI_ID = QDIAB.EMPI_ID
            WHERE
                NORM_NUMERIC_VALUE >= 5.6
                AND SOURCE_DESCRIPTION = 'EMIS GP'
        ),

        -- CREATING C) INCLUSION COHORT FOR MOST RECENT Q RISK OVER 20
        INCLUSIONC AS (
            SELECT DISTINCT
                B.EMPI_ID,
                SOURCE_DESCRIPTION
            FROM LTC_LCS_BASE AS B
            INNER JOIN LTC_QRISK ON B.EMPI_ID = LTC_QRISK.EMPI_ID
            WHERE
                NORM_NUMERIC_VALUE > 20
                AND SOURCE_DESCRIPTION = 'EMIS GP'
        ),

        -- CREATING D) INCLUSION COHORT FOR HISTORY OF GESTATIONAL DIABETES
        INCLUSIOND AS (
            SELECT DISTINCT
                B.EMPI_ID,
                C.SOURCE_DESCRIPTION
            FROM LTC_LCS_BASE AS B
            INNER JOIN PH_F_CONDITION AS C ON B.EMPI_ID = C.EMPI_ID
            INNER JOIN
                LTC_LCS_LOOKUPTABLE AS T
                ON T.SNOMED_CODE = C.CONDITION_CODE
            WHERE
                C.EMPI_ID = B.EMPI_ID
                AND T.CLUSTER_ID IN ('GESTDIAB_COD')
                AND C.SOURCE_DESCRIPTION = 'EMIS GP'
        )

        -- COMBINING ALL INCLUSION CRITERIA
        SELECT DISTINCT * FROM
            (
                SELECT
                    EMPI_ID,
                    SOURCE_DESCRIPTION
                FROM INCLUSIONA
                UNION
                SELECT
                    EMPI_ID,
                    SOURCE_DESCRIPTION
                FROM INCLUSIONB
                UNION
                SELECT
                    EMPI_ID,
                    SOURCE_DESCRIPTION
                FROM INCLUSIONC
                UNION
                SELECT
                    EMPI_ID,
                    SOURCE_DESCRIPTION
                FROM INCLUSIOND
            ) AS FINAL

        -- REMOVING PATIENTS ON REGISTERS
        WHERE NOT EXISTS (
            SELECT 1
            FROM POPHEALTH_QOF_LTCS_LIST AS L
            WHERE
                L.EMPI_ID = FINAL.EMPI_ID
                AND L.LTC_NAME IN (
                    'Atrial Fibrillation',
                    'Asthma',
                    'Chronic Kidney Disease',
                    'COPD',
                    'Coronary Heart Disease',
                    'Diabetes',
                    'Stroke and TIA',
                    'Peripheral Arterial Disease',
                    'Heart Failure',
                    'Hypertension'
                )
        )
        -- REMOVING PATIENTS WITH CONDITIONS
        AND NOT EXISTS (
            SELECT 1
            FROM PH_F_CONDITION AS C
            INNER JOIN
                LTC_LCS_LOOKUPTABLE AS T
                ON T.SNOMED_CODE = C.CONDITION_CODE
            WHERE
                C.EMPI_ID = FINAL.EMPI_ID
                AND T.CLUSTER_ID IN ('NDH_COD', 'PRD_COD')
                AND C.SOURCE_DESCRIPTION = 'EMIS GP'
        )
        -- REMOVING PATIENTS WITH NHS HEALTH CHECK COMPLETED IN THE LAST 2 YEARS
        AND NOT EXISTS (
            SELECT 1
            FROM PH_F_CONDITION AS C
            INNER JOIN
                JOINED_LTC_LOOKUP AS T
                ON T.SNOMED_CODE = C.CONDITION_CODE
            WHERE
                C.EMPI_ID = FINAL.EMPI_ID
                AND T.CLUSTER_ID = 'HEALTH_CHECK_COMP'
                AND DATE(C.EFFECTIVE_DATE_ID) >= ADD_MONTHS(CURRENT_DATE(), -24)
                AND C.SOURCE_DESCRIPTION = 'EMIS GP'
        )
    ) AS A
),


--- CREATING CTE FOR ALL SOURCED
OTHER AS (
    SELECT
        EMPI_ID,
        1 AS 'OTHER'
    FROM (
        -- CREATING CTE FOR MOST RECENT QDIABETES SCORE
        WITH QDIAB AS (
            SELECT
                EMPI_ID,
                NORM_NUMERIC_VALUE,
                SERVICE_LOCAL_DATE_ID AS DATE_OF_LAST_QDIAB
            FROM PH_F_RESULT
            WHERE
                RESULT_RAW_CODE IN ('863501000000102')
                AND (EMPI_ID, SERVICE_LOCAL_DATE_ID) IN (
                    SELECT
                        EMPI_ID,
                        MAX(SERVICE_LOCAL_DATE_ID) AS SERVICE_LOCAL_DATE_ID
                    FROM PH_F_RESULT
                    GROUP BY EMPI_ID
                )
        ),

        -- CREATING A) INCLUSION COHORT FOR HBA1C OVER OR EQUAL TO 42 WITHIN LAST 5 YEARS
        INCLUSIONA AS (
            SELECT DISTINCT EMPI_ID
            FROM PH_F_RESULT AS C
            INNER JOIN JOINED_LTC_LOOKUP AS T
                ON
                    T.SNOMED_CODE = C.RESULT_CODE
                    AND T.CLUSTER_ID IN ('HBA1C')
                    AND C.NORM_NUMERIC_VALUE >= 42
                    AND DATE(C.SERVICE_DATE) >= ADD_MONTHS(CURRENT_DATE(), -60)
        ),

        -- CREATING B) INCLUSION COHORT FOR MOST RECENT Q DIABETES OVER OR EQUAL TO 5.6
        INCLUSIONB AS (
            SELECT DISTINCT B.EMPI_ID
            FROM LTC_LCS_BASE AS B
            INNER JOIN QDIAB ON B.EMPI_ID = QDIAB.EMPI_ID
            WHERE NORM_NUMERIC_VALUE >= 5.6
        ),

        -- CREATING C) INCLUSION COHORT FOR MOST RECENT Q RISK OVER 20
        INCLUSIONC AS (
            SELECT DISTINCT B.EMPI_ID
            FROM LTC_LCS_BASE AS B
            INNER JOIN LTC_QRISK ON B.EMPI_ID = LTC_QRISK.EMPI_ID
            WHERE NORM_NUMERIC_VALUE > 20
        ),

        -- CREATING D) INCLUSION COHORT FOR HISTORY OF GESTATIONAL DIABETES
        INCLUSIOND AS (
            SELECT DISTINCT B.EMPI_ID
            FROM LTC_LCS_BASE AS B
            INNER JOIN PH_F_CONDITION AS C ON B.EMPI_ID = C.EMPI_ID
            INNER JOIN
                LTC_LCS_LOOKUPTABLE AS T
                ON T.SNOMED_CODE = C.CONDITION_CODE
            WHERE
                C.EMPI_ID = B.EMPI_ID
                AND T.CLUSTER_ID IN ('GESTDIAB_COD')
        )

        -- COMBINING ALL INCLUSION CRITERIA
        SELECT DISTINCT * FROM
            (
                SELECT EMPI_ID FROM INCLUSIONA
                UNION
                SELECT EMPI_ID FROM INCLUSIONB
                UNION
                SELECT EMPI_ID FROM INCLUSIONC
                UNION
                SELECT EMPI_ID FROM INCLUSIOND
            ) AS FINAL

        -- REMOVING PATIENTS ON REGISTERS
        WHERE NOT EXISTS (
            SELECT 1
            FROM POPHEALTH_QOF_LTCS_LIST AS L
            WHERE
                L.EMPI_ID = FINAL.EMPI_ID
                AND L.LTC_NAME IN (
                    'Atrial Fibrillation',
                    'Asthma',
                    'Chronic Kidney Disease',
                    'COPD',
                    'Coronary Heart Disease',
                    'Diabetes',
                    'Stroke and TIA',
                    'Peripheral Arterial Disease',
                    'Heart Failure',
                    'Hypertension'
                )
        )
        -- REMOVING PATIENTS WITH CONDITIONS
        AND NOT EXISTS (
            SELECT 1
            FROM PH_F_CONDITION AS C
            INNER JOIN
                LTC_LCS_LOOKUPTABLE AS T
                ON T.SNOMED_CODE = C.CONDITION_CODE
            WHERE
                C.EMPI_ID = FINAL.EMPI_ID
                AND T.CLUSTER_ID IN ('NDH_COD', 'PRD_COD')
        )
        -- REMOVING PATIENTS WITH NHS HEALTH CHECK COMPLETED IN THE LAST 2 YEARS
        AND NOT EXISTS (
            SELECT 1
            FROM PH_F_CONDITION AS C
            INNER JOIN
                JOINED_LTC_LOOKUP AS T
                ON T.SNOMED_CODE = C.CONDITION_CODE
            WHERE
                C.EMPI_ID = FINAL.EMPI_ID
                AND T.CLUSTER_ID = 'HEALTH_CHECK_COMP'
                AND DATE(C.EFFECTIVE_DATE_ID) >= ADD_MONTHS(CURRENT_DATE(), -24)
        )
    ) AS B
)

SELECT
    E.EMIS,
    O.OTHER,
    COALESCE(E.EMPI_ID, O.EMPI_ID) AS EMPI_ID
FROM EMIS AS E
FULL JOIN OTHER AS O ON E.EMPI_ID = O.EMPI_ID
