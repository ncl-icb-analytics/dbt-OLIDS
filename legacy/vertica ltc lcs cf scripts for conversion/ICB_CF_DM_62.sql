-- ICB_CF_DM_62
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

                INNER JOIN
                    PH_F_CONDITION AS L3
                    ON B.EMPI_ID = L3.EMPI_ID AND SOURCE_DESCRIPTION = 'EMIS GP'
                INNER JOIN JOINED_LTC_LOOKUP AS LOOKUP
                    ON
                        L3.CONDITION_CODE = LOOKUP.SNOMED_CODE
                        AND LOOKUP.CLUSTER_ID IN ('DM_GESTDIAB_AND_PREG_RISK')


                WHERE
                    B.AGE >= 17
                    AND L1.EMPI_ID IS NULL
                    AND L2.EMPI_ID IS NULL

                EXCEPT

                SELECT EMPI_ID
                FROM POPHEALTH_QOF_LTCS_LIST
                WHERE LTC_NAME = 'Diabetes'
            ),

            -- Exclusions: Hb1Ac in the last year
            EXCLUSIONS AS (
                SELECT EMPI_ID

                FROM PH_F_CONDITION AS C
                INNER JOIN
                    JOINED_LTC_LOOKUP AS LOOKUP
                    ON C.CONDITION_CODE = LOOKUP.SNOMED_CODE

                WHERE
                    SOURCE_DESCRIPTION = 'EMIS GP'
                    AND LOOKUP.CLUSTER_ID = 'IFCCHBAM_COD'
                    AND EFFECTIVE_DT_TM > ADD_MONTHS(CURRENT_DATE(), -12)

                UNION

                SELECT EMPI_ID

                FROM PH_F_RESULT AS C
                INNER JOIN
                    JOINED_LTC_LOOKUP AS LOOKUP
                    ON C.RESULT_CODE = LOOKUP.SNOMED_CODE

                WHERE
                    SOURCE_DESCRIPTION = 'EMIS GP'
                    AND LOOKUP.CLUSTER_ID = 'IFCCHBAM_COD'
                    AND SERVICE_DATE > ADD_MONTHS(CURRENT_DATE(), -12)

            )

            SELECT B.*

            FROM BASE_POPULATION AS B
            LEFT JOIN EXCLUSIONS AS E ON B.EMPI_ID = E.EMPI_ID

            WHERE E.EMPI_ID IS NULL

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

                INNER JOIN PH_F_CONDITION AS L3 ON B.EMPI_ID = L3.EMPI_ID
                INNER JOIN JOINED_LTC_LOOKUP AS LOOKUP
                    ON
                        L3.CONDITION_CODE = LOOKUP.SNOMED_CODE
                        AND LOOKUP.CLUSTER_ID IN ('DM_GESTDIAB_AND_PREG_RISK')

                WHERE
                    B.AGE >= 17
                    AND L1.EMPI_ID IS NULL
                    AND L2.EMPI_ID IS NULL
            ),

            -- Exclusions: Hb1Ac in the last year
            EXCLUSIONS AS (
                SELECT EMPI_ID

                FROM PH_F_CONDITION AS C
                INNER JOIN
                    JOINED_LTC_LOOKUP AS LOOKUP
                    ON C.CONDITION_CODE = LOOKUP.SNOMED_CODE

                WHERE
                    --SOURCE_DESCRIPTION = 'EMIS GP' AND
                    LOOKUP.CLUSTER_ID = 'IFCCHBAM_COD'
                    AND EFFECTIVE_DT_TM > ADD_MONTHS(CURRENT_DATE(), -12)

                UNION

                SELECT EMPI_ID


                FROM PH_F_RESULT AS C
                INNER JOIN
                    JOINED_LTC_LOOKUP AS LOOKUP
                    ON C.RESULT_CODE = LOOKUP.SNOMED_CODE

                WHERE
                    --SOURCE_DESCRIPTION = 'EMIS GP' AND
                    LOOKUP.CLUSTER_ID = 'IFCCHBAM_COD'
                    AND SERVICE_DATE > ADD_MONTHS(CURRENT_DATE(), -12)

            )

            SELECT B.*

            FROM BASE_POPULATION AS B
            LEFT JOIN EXCLUSIONS AS E ON B.EMPI_ID = E.EMPI_ID

            WHERE E.EMPI_ID IS NULL

        ) AS A

)

SELECT
    COALESCE(E.EMPI_ID, O.EMPI_ID) AS EMPI_ID,
    CASE WHEN E.EMIS = 1 THEN 'EMIS' ELSE 'Other' END AS SOURCE

FROM EMIS AS E
FULL OUTER JOIN OTHER AS O ON E.EMPI_ID = O.EMPI_ID
