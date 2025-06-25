--14.8.23 Hypertension base population to support the priority groups

-- Find anyone with white coat hypertension or Hypertension resolved
WITH EMIS_BASE AS (
    SELECT DISTINCT
        EMPI_ID,
        1 AS 'EMIS'
    FROM (
        SELECT DISTINCT EMPI_ID FROM LTC_LCS_BASE

        EXCEPT
        SELECT DISTINCT EMPI_ID FROM ICS_LTC_01

        EXCEPT
        SELECT DISTINCT EMPI_ID FROM HEALTH_CHECK_COMP_IN_24

        EXCEPT
        SELECT DISTINCT EMPI_ID FROM POPHEALTH_QOF_LTCS_LIST
        WHERE LTC_NAME IN ('Diabetes', 'Palliative Care')

        -- Exclude any previous hypertension resolved or white coat hypertension from EMIS GP
        EXCEPT
        SELECT DISTINCT EMPI_ID
        FROM NCL_CODES
        INNER JOIN
            JOINED_LTC_LOOKUP AS LOOKUP
            ON NCL_CODES.CODE = LOOKUP.SNOMED_CODE

        WHERE
            LOOKUP.CLUSTER_ID IN ('HTN_RES', 'HTN_WHITE_COAT')
            AND SOURCE_DESCRIPTION = 'EMIS GP'

        EXCEPT
        SELECT DISTINCT EMPI_ID

        FROM (
            SELECT DISTINCT
                EMPI_ID,
                CODE,
                CLUSTER_ID,
                ROW_NUMBER()
                    OVER (PARTITION BY EMPI_ID ORDER BY DATETIME DESC)
                    AS ROW_NUM

            FROM NLHCR_ANALYST.NCL_CODES AS CODES
            INNER JOIN
                JOINED_LTC_LOOKUP AS LOOKUP
                ON CODES.CODE = LOOKUP.SNOMED_CODE

            WHERE
                LOOKUP.CLUSTER_ID IN ('HTN_RES', 'HTN_COD')
                AND SOURCE_DESCRIPTION = 'EMIS GP'
        ) AS A

        WHERE ROW_NUM = '1' AND CLUSTER_ID = 'HTN_COD'
    ) AS EMIS
),

OTHER_BASE AS (
    SELECT DISTINCT
        EMPI_ID,
        1 AS 'OTHER'
    FROM (
        SELECT DISTINCT EMPI_ID FROM LTC_LCS_BASE

        EXCEPT
        SELECT DISTINCT EMPI_ID FROM ICS_LTC_01

        EXCEPT
        SELECT DISTINCT EMPI_ID FROM HEALTH_CHECK_COMP_IN_24

        EXCEPT
        SELECT DISTINCT EMPI_ID FROM POPHEALTH_QOF_LTCS_LIST
        WHERE LTC_NAME IN ('Diabetes', 'Palliative Care')

        -- Exclude any previous hypertension resolved or white coat hypertension from all sources
        EXCEPT
        SELECT DISTINCT EMPI_ID
        FROM NCL_CODES
        INNER JOIN
            JOINED_LTC_LOOKUP AS LOOKUP
            ON NCL_CODES.CODE = LOOKUP.SNOMED_CODE

        WHERE LOOKUP.CLUSTER_ID IN ('HTN_RES', 'HTN_WHITE_COAT')

        EXCEPT
        SELECT DISTINCT EMPI_ID

        FROM (
            SELECT DISTINCT
                EMPI_ID,
                CODE,
                CLUSTER_ID,
                ROW_NUMBER()
                    OVER (PARTITION BY EMPI_ID ORDER BY DATETIME DESC)
                    AS ROW_NUM

            FROM NLHCR_ANALYST.NCL_CODES AS CODES
            INNER JOIN
                JOINED_LTC_LOOKUP AS LOOKUP
                ON CODES.CODE = LOOKUP.SNOMED_CODE

            WHERE LOOKUP.CLUSTER_ID IN ('HTN_RES', 'HTN_COD')
        ) AS A

        WHERE ROW_NUM = '1' AND CLUSTER_ID = 'HTN_COD'
    ) AS OTHER
)

SELECT
    E.EMIS,
    O.OTHER,
    COALESCE(E.EMPI_ID, O.EMPI_ID) AS EMPI_ID
FROM EMIS_BASE AS E
FULL OUTER JOIN OTHER_BASE AS O ON E.EMPI_ID = O.EMPI_ID
