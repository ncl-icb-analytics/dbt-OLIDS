-- BAME population
WITH BAME_POPULATION_EMIS AS (
    SELECT DISTINCT EMPI_ID
    FROM NCL_CODES
    WHERE
        CODE IN (
            SELECT SNOMED_CODE
            FROM JOINED_LTC_LOOKUP
            WHERE CLUSTER_ID = 'BAME_ETHNICITY'
        )
        AND SOURCE_DESCRIPTION = 'EMIS GP'

    EXCEPT
    SELECT EMPI_ID
    FROM NCL_CODES
    WHERE
        CODE IN (
            SELECT SNOMED_CODE
            FROM JOINED_LTC_LOOKUP
            WHERE CLUSTER_ID IN ('WHITE_BRITISH', 'DM_EXCL_ETHNICITY')
        )
        AND SOURCE_DESCRIPTION = 'EMIS GP'
),

BAME_POPULATION_ALL AS (
    SELECT DISTINCT EMPI_ID
    FROM NCL_CODES
    WHERE
        CODE IN (
            SELECT SNOMED_CODE
            FROM JOINED_LTC_LOOKUP
            WHERE CLUSTER_ID = 'BAME_ETHNICITY'
        )
    EXCEPT
    SELECT EMPI_ID
    FROM NCL_CODES
    WHERE
        CODE IN (
            SELECT SNOMED_CODE
            FROM JOINED_LTC_LOOKUP
            WHERE CLUSTER_ID IN ('WHITE_BRITISH', 'DM_EXCL_ETHNICITY')
        )
),

BASE_POPULATION_EMIS AS (
    SELECT DISTINCT B.EMPI_ID
    FROM LTC_LCS_BASE AS B
    WHERE B.AGE >= 17

    EXCEPT
    SELECT EMPI_ID
    FROM ICS_LTC_01
    EXCEPT
    SELECT EMPI_ID
    FROM HEALTH_CHECK_COMP_IN_24
    EXCEPT
    SELECT EMPI_ID
    FROM POPHEALTH_QOF_LTCS_LIST
    WHERE LTC_NAME = 'Diabetes'
    EXCEPT
    SELECT EMPI_ID
    FROM ICB_CF_DM_64
    WHERE SOURCE = 'EMIS'
),

BASE_POPULATION_ALL AS (
    SELECT DISTINCT B.EMPI_ID
    FROM LTC_LCS_BASE AS B
    WHERE B.AGE >= 17

    EXCEPT
    SELECT EMPI_ID
    FROM ICS_LTC_01
    EXCEPT
    SELECT EMPI_ID
    FROM HEALTH_CHECK_COMP_IN_24
    EXCEPT
    SELECT EMPI_ID
    FROM POPHEALTH_QOF_LTCS_LIST
    WHERE LTC_NAME = 'Diabetes'
    EXCEPT
    SELECT EMPI_ID
    FROM ICB_CF_DM_64

),

EMIS AS (
    WITH LATEST_BMI AS (
        SELECT DISTINCT
            A.EMPI_ID,
            A.VALUE,
            CASE
                WHEN BAME.EMPI_ID IS NOT NULL THEN 1
                ELSE 0
            END AS IS_BAME
        FROM (
            SELECT
                BASE_POPULATION_EMIS.EMPI_ID,
                NCL_CODES.VALUE,
                ROW_NUMBER()
                    OVER (
                        PARTITION BY BASE_POPULATION_EMIS.EMPI_ID
                        ORDER BY NCL_CODES.DATETIME DESC, NCL_CODES.VALUE DESC
                    )
                    AS ROW_NUM
            FROM BASE_POPULATION_EMIS
            LEFT OUTER JOIN
                NCL_CODES
                ON BASE_POPULATION_EMIS.EMPI_ID = NCL_CODES.EMPI_ID
            WHERE
                SOURCE_DESCRIPTION = 'EMIS GP'
                AND CODE IN (
                    SELECT SNOMED_CODE
                    FROM JOINED_LTC_LOOKUP
                    WHERE CLUSTER_ID = 'BMI_CODES'
                )
                AND NCL_CODES.VALUE IS NOT NULL
        ) AS A
        LEFT JOIN BAME_POPULATION_EMIS AS BAME ON A.EMPI_ID = BAME.EMPI_ID
        WHERE A.ROW_NUM = 1
    )

    SELECT DISTINCT
        LATEST_BMI.EMPI_ID,
        1 AS EMIS
    FROM LATEST_BMI
    LEFT JOIN EXC ON LATEST_BMI.EMPI_ID = EXC.EMPI_ID
    WHERE EXC.EMPI_ID IS NULL AND (
        (
            LATEST_BMI.IS_BAME = 1
            AND CAST(LATEST_BMI.VALUE AS FLOAT) BETWEEN 27.5 AND 32.5
        )
        OR (
            LATEST_BMI.IS_BAME = 0
            AND CAST(LATEST_BMI.VALUE AS FLOAT) BETWEEN 30 AND 35
        )
    )
),

OTHER AS (
    WITH LATEST_BMI_OTHER AS (
        SELECT DISTINCT
            A.EMPI_ID,
            A.VALUE,
            CASE
                WHEN BAME.EMPI_ID IS NOT NULL THEN 1
                ELSE 0
            END AS IS_BAME
        FROM (
            SELECT
                BASE_POPULATION_ALL.EMPI_ID,
                NCL_CODES.VALUE,
                ROW_NUMBER()
                    OVER (
                        PARTITION BY BASE_POPULATION_ALL.EMPI_ID
                        ORDER BY NCL_CODES.DATETIME DESC, NCL_CODES.VALUE DESC
                    )
                    AS ROW_NUM
            FROM BASE_POPULATION_ALL
            LEFT OUTER JOIN
                NCL_CODES
                ON BASE_POPULATION_ALL.EMPI_ID = NCL_CODES.EMPI_ID
            WHERE
                CODE IN (
                    SELECT SNOMED_CODE
                    FROM JOINED_LTC_LOOKUP
                    WHERE CLUSTER_ID = 'BMI_CODES'
                )
                AND NCL_CODES.VALUE IS NOT NULL
        ) AS A
        LEFT JOIN BAME_POPULATION_ALL AS BAME ON A.EMPI_ID = BAME.EMPI_ID
        WHERE A.ROW_NUM = 1
    )

    SELECT DISTINCT
        LATEST_BMI_OTHER.EMPI_ID,
        1 AS OTHER
    FROM LATEST_BMI_OTHER
    LEFT JOIN (
        SELECT DISTINCT EMPI_ID
        FROM NCL_CODES
        WHERE
            CODE IN (
                SELECT SNOMED_CODE
                FROM JOINED_LTC_LOOKUP
                WHERE CLUSTER_ID = 'HBA1C_LEVEL'
            )
            AND DATETIME >= ADD_MONTHS(CURRENT_DATE(), -24)
    ) AS EXC ON LATEST_BMI_OTHER.EMPI_ID = EXC.EMPI_ID
    WHERE EXC.EMPI_ID IS NULL AND (
        (
            LATEST_BMI_OTHER.IS_BAME = 1
            AND CAST(LATEST_BMI_OTHER.VALUE AS FLOAT) BETWEEN 27.5 AND 32.5
        )
        OR (
            LATEST_BMI_OTHER.IS_BAME = 0
            AND CAST(LATEST_BMI_OTHER.VALUE AS FLOAT) BETWEEN 30 AND 35
        )
    )
),

EXC AS (
    SELECT DISTINCT EMPI_ID
    FROM NCL_CODES
    WHERE
        SOURCE_DESCRIPTION = 'EMIS GP'
        AND CODE IN (
            SELECT SNOMED_CODE
            FROM JOINED_LTC_LOOKUP
            WHERE CLUSTER_ID = 'HBA1C_LEVEL'
        )
        AND DATETIME >= ADD_MONTHS(CURRENT_DATE(), -24)
)

SELECT
    COALESCE(E.EMPI_ID, O.EMPI_ID) AS EMPI_ID,
    CASE
        WHEN E.EMIS = 1 THEN 'EMIS'
        ELSE 'Other'
    END AS SOURCE
FROM EMIS AS E
FULL OUTER JOIN OTHER AS O ON E.EMPI_ID = O.EMPI_ID;
