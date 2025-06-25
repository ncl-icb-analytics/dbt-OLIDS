--ICB_CF_AF_61 Patients on dignoxin, flecainide, propafenone or anticoag
--7.9.23 small adjustments in syntax

WITH EMIS AS (

    SELECT
        EMPI_ID,
        1 AS 'EMIS'

    FROM (

        -- Base table: Patients with current COURSES of ORAL ANTICOAGULANTS or CARDIAC GLYCOSIDES

        WITH RULE1 AS (
            SELECT DISTINCT EMPI_ID
            FROM LTC_LCS_BASE
            EXCEPT
            SELECT EMPI_ID
            FROM ICS_LTC_01
            EXCEPT
            SELECT EMPI_ID
            FROM POPHEALTH_LTCS_LIST
            WHERE LTC_NAME IN ('Atrial Fibrillation', 'Heart Failure')
            EXCEPT
            SELECT EMPI_ID
            FROM HEALTH_CHECK_COMP_IN_24
        ),

        INCLUSION AS (
            SELECT R.EMPI_ID
            FROM RULE1 AS R
            INNER JOIN
                CERNER_MEDICATION_JOINED_TABLE AS M
                ON R.EMPI_ID = M.EMPI_ID
            INNER JOIN JOINED_LTC_LOOKUP AS T ON M.DRUG_CODE = T.SNOMED_CODE
            WHERE
                M.STATUS_DISPLAY = 'Active'
                AND (M.STOP_DATE IS NULL OR M.STOP_DATE >= CURRENT_DATE())
                AND M.START_DT_TM > ADD_MONTHS(CURRENT_DATE(), -3)
                --AND t.CLUSTER_ID = 'ORANTICOAG_2.8.2' --REMOVE IN FINAL
                AND T.CLUSTER_ID IN (
                    'DRUGS_USED_IN_AF', 'DIGOXIN', 'ORANTICOAG_2.8.2'
                )
                AND SOURCE_DESCRIPTION = 'EMIS GP'

            UNION

            -- CARDIAC GLYCOSIDES SEPARATELY TO MATCH SEARCH 'AND ANTICOAG' CRITERION

            SELECT R.EMPI_ID
            FROM RULE1 AS R
            INNER JOIN
                CERNER_MEDICATION_JOINED_TABLE AS M
                ON R.EMPI_ID = M.EMPI_ID
            INNER JOIN JOINED_LTC_LOOKUP AS T ON M.DRUG_CODE = T.SNOMED_CODE
            WHERE
                M.STATUS_DISPLAY = 'Active'
                AND (M.STOP_DATE IS NULL OR M.STOP_DATE >= CURRENT_DATE())
                AND M.START_DT_TM > ADD_MONTHS(CURRENT_DATE(), -3)
                AND T.CLUSTER_ID = 'CARDIAC GLYCOSIDES'
                AND SOURCE_DESCRIPTION = 'EMIS GP'

            UNION

            -- Patients with current courses of ANTICOAGUALANTS AND PROTAMINE (excluding.....)

            SELECT R.EMPI_ID
            FROM RULE1 AS R
            INNER JOIN
                CERNER_MEDICATION_JOINED_TABLE AS M
                ON R.EMPI_ID = M.EMPI_ID
            INNER JOIN JOINED_LTC_LOOKUP AS T ON M.DRUG_CODE = T.SNOMED_CODE
            WHERE
                M.START_DT_TM > ADD_MONTHS(CURRENT_DATE(), -6)
                AND T.CLUSTER_ID IN ('ORANTICOAG_2.8.2', 'PROTAMINE_DRUGS')
                AND SOURCE_DESCRIPTION = 'EMIS GP'
        ),

        -------------------------------------------------------------------------------------------------

        EXCLUSION AS (

            SELECT --DISTINCT
                EMPI_ID
            FROM PH_F_RESULT AS R
            INNER JOIN JOINED_LTC_LOOKUP AS T ON R.RESULT_CODE = T.SNOMED_CODE
            WHERE
                (
                    --Long-haul COVID-19 + Hypoplastic left heart syndrome (disorder) + DVT
                    RESULT_CODE IN ('1119304009', '62067003', '132221000119109')
                    OR T.CLUSTER_ID IN ('DVT', 'AF_FLUTTER')
                )
                AND SOURCE_DESCRIPTION = 'EMIS GP'

            UNION

            SELECT --DISTINCT
                EMPI_ID
            FROM PH_F_CONDITION AS R
            INNER JOIN
                JOINED_LTC_LOOKUP AS T
                ON R.CONDITION_CODE = T.SNOMED_CODE
            WHERE
                (
                    --Long-haul COVID-19 + Hypoplastic left heart syndrome (disorder) + DVT
                    CONDITION_CODE IN (
                        '1119304009', '62067003', '132221000119109'
                    )
                    OR T.CLUSTER_ID IN ('DVT', 'AF_FLUTTER')
                )
                AND SOURCE_DESCRIPTION = 'EMIS GP'

            UNION

            SELECT --DISTINCT
                EMPI_ID
            FROM PH_F_PROCEDURE AS R
            INNER JOIN
                JOINED_LTC_LOOKUP AS T
                ON R.PROCEDURE_CODE = T.SNOMED_CODE
            WHERE
                (
                    --Long-haul COVID-19 + Hypoplastic left heart syndrome (disorder) + DVT
                    PROCEDURE_CODE IN (
                        '1119304009', '62067003', '132221000119109'
                    )
                    OR T.CLUSTER_ID IN ('DVT', 'AF_FLUTTER')
                )
                AND SOURCE_DESCRIPTION = 'EMIS GP'

        )

        --Exclude all patients with selected clusters (Rules 1-4)

        SELECT DISTINCT EMPI_ID FROM INCLUSION

        EXCEPT

        SELECT DISTINCT EMPI_ID FROM EXCLUSION

    ) AS A
),

OTHER AS (

    SELECT
        EMPI_ID,
        1 AS 'OTHER'

    FROM (

        -- Base table: Patients with current COURSES of ORAL ANTICOAGULANTS or CARDIAC GLYCOSIDES
        WITH RULE1 AS (
            SELECT DISTINCT EMPI_ID
            FROM LTC_LCS_BASE
            EXCEPT
            SELECT EMPI_ID
            FROM ICS_LTC_01
            EXCEPT
            SELECT EMPI_ID
            FROM POPHEALTH_LTCS_LIST
            WHERE LTC_NAME IN ('Atrial Fibrillation', 'Heart Failure')
            EXCEPT
            SELECT EMPI_ID
            FROM HEALTH_CHECK_COMP_IN_24
        ),

        INCLUSION AS (
            SELECT R.EMPI_ID
            FROM RULE1 AS R
            INNER JOIN
                CERNER_MEDICATION_JOINED_TABLE AS M
                ON R.EMPI_ID = M.EMPI_ID
            INNER JOIN JOINED_LTC_LOOKUP AS T ON M.DRUG_CODE = T.SNOMED_CODE
            WHERE
                M.STATUS_DISPLAY = 'Active'
                AND (M.STOP_DATE IS NULL OR M.STOP_DATE >= CURRENT_DATE())
                AND M.START_DT_TM > ADD_MONTHS(CURRENT_DATE(), -3)
                --and t.CLUSTER_ID = 'ORANTICOAG_2.8.2' --REMOVE IN FINAL
                AND T.CLUSTER_ID IN (
                    'DRUGS_USED_IN_AF', 'DIGOXIN', 'ORANTICOAG_2.8.2'
                )
            --AND SOURCE_DESCRIPTION = 'EMIS GP'

            UNION

            -- CARDIAC GLYCOSIDES SEPARATELY TO MATCH SEARCH 'AND ANTICOAG' CRITERION

            SELECT R.EMPI_ID
            FROM RULE1 AS R
            INNER JOIN
                CERNER_MEDICATION_JOINED_TABLE AS M
                ON R.EMPI_ID = M.EMPI_ID
            INNER JOIN JOINED_LTC_LOOKUP AS T ON M.DRUG_CODE = T.SNOMED_CODE
            WHERE
                M.STATUS_DISPLAY = 'Active'
                AND (M.STOP_DATE IS NULL OR M.STOP_DATE >= CURRENT_DATE())
                AND M.START_DT_TM > ADD_MONTHS(CURRENT_DATE(), -3)
                AND T.CLUSTER_ID = 'CARDIAC GLYCOSIDES'
            --AND SOURCE_DESCRIPTION = 'EMIS GP'

            UNION

            -- Patients with current courses of ANTICOAGUALANTS AND PROTAMINE (excluding.....)

            SELECT R.EMPI_ID
            FROM RULE1 AS R
            INNER JOIN
                CERNER_MEDICATION_JOINED_TABLE AS M
                ON R.EMPI_ID = M.EMPI_ID
            INNER JOIN JOINED_LTC_LOOKUP AS T ON M.DRUG_CODE = T.SNOMED_CODE
            WHERE
                M.START_DT_TM > ADD_MONTHS(CURRENT_DATE(), -6)
                AND T.CLUSTER_ID IN ('ORANTICOAG_2.8.2', 'PROTAMINE_DRUGS')
        --AND SOURCE_DESCRIPTION = 'EMIS GP'

        ),

        -------------------------------------------------------------------------------------------------

        EXCLUSION AS (

            SELECT --DISTINCT
                EMPI_ID
            FROM PH_F_RESULT AS R
            INNER JOIN JOINED_LTC_LOOKUP AS T ON R.RESULT_CODE = T.SNOMED_CODE
            WHERE
                (
                    --Long-haul COVID-19 + Hypoplastic left heart syndrome (disorder) + DVT
                    RESULT_CODE IN ('1119304009', '62067003', '132221000119109')
                    OR T.CLUSTER_ID IN ('DVT', 'AF_FLUTTER')
                )
            --AND SOURCE_DESCRIPTION = 'EMIS GP'

            UNION

            SELECT --DISTINCT
                EMPI_ID
            FROM PH_F_CONDITION AS R
            INNER JOIN
                JOINED_LTC_LOOKUP AS T
                ON R.CONDITION_CODE = T.SNOMED_CODE
            WHERE
                (
                    --Long-haul COVID-19 + Hypoplastic left heart syndrome (disorder) + DVT
                    CONDITION_CODE IN (
                        '1119304009', '62067003', '132221000119109'
                    )
                    OR T.CLUSTER_ID IN ('DVT', 'AF_FLUTTER')
                )
            --AND SOURCE_DESCRIPTION = 'EMIS GP'

            UNION

            SELECT --DISTINCT
                EMPI_ID
            FROM PH_F_PROCEDURE AS R
            INNER JOIN
                JOINED_LTC_LOOKUP AS T
                ON R.PROCEDURE_CODE = T.SNOMED_CODE
            WHERE
                (
                    --Long-haul COVID-19 + Hypoplastic left heart syndrome (disorder) + DVT
                    PROCEDURE_CODE IN (
                        '1119304009', '62067003', '132221000119109'
                    )
                    OR T.CLUSTER_ID IN ('DVT', 'AF_FLUTTER')
                )
        --AND SOURCE_DESCRIPTION = 'EMIS GP'

        )

        --Exclude all patients with selected clusters (Rules 1-4)

        SELECT DISTINCT EMPI_ID FROM INCLUSION

        EXCEPT

        SELECT DISTINCT EMPI_ID FROM EXCLUSION

    ) AS B
)

-- USING 'EMIS' TABLE TO GET WHOLE COHORT AND LEFT JOINING TO SEE IF OTHER AS WELL
SELECT
    COALESCE(E.EMPI_ID, O.EMPI_ID) AS EMPI_ID,
    CASE WHEN E.EMIS = 1 THEN 'EMIS' ELSE 'Other' END AS SOURCE
FROM EMIS AS E
FULL OUTER JOIN OTHER AS O ON E.EMPI_ID = O.EMPI_ID
