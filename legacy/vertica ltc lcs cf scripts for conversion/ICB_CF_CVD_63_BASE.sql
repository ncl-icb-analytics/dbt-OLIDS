-- Define the 'OTHER' CTE
WITH OTHER AS (
    SELECT DISTINCT
        CARDIAC_RISK_SCORES.EMPI_ID,
        'Other' AS SOURCE
    FROM (
        SELECT
            EMPI_ID,
            NORM_NUMERIC_VALUE,
            ROW_NUMBER()
                OVER (PARTITION BY EMPI_ID ORDER BY SERVICE_DATE DESC)
                AS RN
        FROM PH_F_RESULT
        WHERE
            RESULT_CODE IN (
                '752451000000100',
                '809311000000105',
                '718087004',
                '1085871000000105',
                '450759008',
                '763244005'
            )
            -- SELECT SNOMED_CODE
            --FROM JOINED_LTC_LOOKUP
            --WHERE CLUSTER_ID = 'CARDIAC_RISK_SCORE')
            AND SERVICE_DATE < NOW()
    ) AS CARDIAC_RISK_SCORES
    INNER JOIN
        LTC_LCS_BASE
        ON CARDIAC_RISK_SCORES.EMPI_ID = LTC_LCS_BASE.EMPI_ID
    WHERE
        RN = 1
        AND NORM_NUMERIC_VALUE >= 10
        AND AGE BETWEEN 40 AND 83
    EXCEPT
    SELECT
        EMPI_ID,
        'Other'
    FROM ICS_LTC_01
    EXCEPT
    SELECT
        EMPI_ID,
        'Other'
    FROM POPHEALTH_QOF_LTCS_LIST
    WHERE LTC_NAME IN ('Diabetes', 'Palliative Care')
    EXCEPT
    SELECT
        EMPI_ID,
        'Other'
    FROM NCL_CODES WHERE CODE IN (
        SELECT SNOMED_CODE FROM JOINED_LTC_LOOKUP
        WHERE
            CLUSTER_ID IN (
                'STATIN_ALLERGY_ADVERSE_REACTION', 'STATIN_NOT_INDICATED'
            )
    )
),

-- Define the 'EMIS' CTE
EMIS AS (
    SELECT DISTINCT
        CARDIAC_RISK_SCORES.EMPI_ID,
        'EMIS' AS SOURCE
    FROM (
        SELECT
            EMPI_ID,
            NORM_NUMERIC_VALUE,
            ROW_NUMBER()
                OVER (PARTITION BY EMPI_ID ORDER BY SERVICE_DATE DESC)
                AS RN
        FROM PH_F_RESULT
        WHERE
            RESULT_CODE IN (
                '752451000000100',
                '809311000000105',
                '718087004',
                '1085871000000105',
                '450759008',
                '763244005'
            )
            -- SELECT SNOMED_CODE
            --FROM JOINED_LTC_LOOKUP
            --WHERE CLUSTER_ID = 'CARDIAC_RISK_SCORE')
            AND SERVICE_DATE < NOW()
            AND SOURCE_DESCRIPTION = 'EMIS GP'
    ) AS CARDIAC_RISK_SCORES
    INNER JOIN
        LTC_LCS_BASE
        ON CARDIAC_RISK_SCORES.EMPI_ID = LTC_LCS_BASE.EMPI_ID
    WHERE
        RN = 1
        AND NORM_NUMERIC_VALUE >= 10
        AND AGE BETWEEN 40 AND 83
    EXCEPT
    SELECT
        EMPI_ID,
        'EMIS'
    FROM ICS_LTC_01
    EXCEPT
    SELECT
        EMPI_ID,
        'EMIS'
    FROM POPHEALTH_QOF_LTCS_LIST
    WHERE LTC_NAME IN ('Diabetes', 'Palliative Care')
    EXCEPT
    SELECT
        EMPI_ID,
        'EMIS'
    FROM NCL_CODES WHERE CODE IN (
        SELECT SNOMED_CODE FROM JOINED_LTC_LOOKUP
        WHERE
            CLUSTER_ID IN (
                'STATIN_ALLERGY_ADVERSE_REACTION', 'STATIN_NOT_INDICATED'
            )
    )
)

-- Main Query
SELECT DISTINCT
    ALL_IDS.EMPI_ID,
    COALESCE(
        CASE WHEN EMIS.EMPI_ID IS NOT NULL THEN 'EMIS' END,
        CASE WHEN OTHER.EMPI_ID IS NOT NULL THEN 'Other' END
    ) AS SOURCE
FROM (
    SELECT EMPI_ID FROM EMIS
    UNION
    SELECT EMPI_ID FROM OTHER
) AS ALL_IDS
LEFT JOIN EMIS ON ALL_IDS.EMPI_ID = EMIS.EMPI_ID
LEFT JOIN OTHER ON ALL_IDS.EMPI_ID = OTHER.EMPI_ID
